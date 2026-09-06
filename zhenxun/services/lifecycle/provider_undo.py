from __future__ import annotations

import asyncio
from collections.abc import MutableMapping, MutableSequence, MutableSet
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from typing import Any

from .models import ResourceReceipt
from .providers import capture_runtime_providers

_MISSING = object()
_CONTAINERS = (MutableMapping, MutableSequence, MutableSet)


class ProviderUndoConflict(RuntimeError):
    pass


def _shallow(container):
    return dict(container) if isinstance(container, MutableMapping) else list(container)


def _signature(container):
    if isinstance(container, MutableMapping):
        return {key: id(value) for key, value in container.items()}
    if isinstance(container, MutableSet):
        return {id(value) for value in container}
    return tuple(id(value) for value in container)


def _leaves(value, seen=None):
    seen = set() if seen is None else seen
    if id(value) in seen:
        return
    seen.add(id(value))
    if isinstance(value, _CONTAINERS):
        children = value.values() if isinstance(value, MutableMapping) else value
        for child in children:
            yield from _leaves(child, seen)
    else:
        yield value


@dataclass
class UndoEntry:
    transaction_id: str
    owner: str
    incarnation: str | None
    provider: str
    container: Any
    key: Any
    before: Any = _MISSING
    after: Any = _MISSING
    after_signature: Any = None
    registered_values: tuple[Any, ...] = ()
    successors: tuple[Any, ...] = ()
    undone: bool = False

    def undo(self):
        if self.undone:
            return
        container = self.container
        if isinstance(container, MutableMapping):
            current = container.get(self.key, _MISSING)
            if current is self.before:
                self.undone = True
                return
            if current is not self.after and current is not _MISSING:
                raise ProviderUndoConflict("provider_key_identity_conflict")
            if current is self.after and isinstance(current, _CONTAINERS):
                if _signature(current) != self.after_signature:
                    raise ProviderUndoConflict("provider_container_identity_conflict")
            if self.before is _MISSING:
                container.pop(self.key, None)
            else:
                container[self.key] = self.before
        elif isinstance(container, MutableSequence):
            if self.after is not _MISSING:
                container[:] = [item for item in container if item is not self.after]
            if self.before is not _MISSING and not any(
                item is self.before for item in container
            ):
                index = next(
                    (
                        i
                        for successor in self.successors
                        for i, item in enumerate(container)
                        if item is successor
                    ),
                    min(self.key, len(container)),
                )
                container.insert(index, self.before)
        else:
            if self.after is not _MISSING:
                for item in list(container):
                    if item is self.after:
                        container.remove(item)
            if self.before is not _MISSING:
                container.add(self.before)
        self.undone = True


class ProviderUndoLog:
    def __init__(self, transaction_id: str):
        self.transaction_id = transaction_id
        self.task = asyncio.current_task()
        self.entries: list[UndoEntry] = []
        self.states: dict[int, tuple[str, Any, Any]] = {}
        self.recording = True
        self.depth = 0
        self._observe()

    def _observe(self):
        def visit(provider, container):
            if not isinstance(container, _CONTAINERS) or id(container) in seen:
                return
            seen.add(id(container))
            self.states[id(container)] = (provider, container, _shallow(container))
            values = (
                container.values()
                if isinstance(container, MutableMapping)
                else container
            )
            for value in values:
                visit(provider, value)

        seen = set()
        for snapshot in capture_runtime_providers().containers:
            visit(snapshot.name, snapshot.container)

    def verify(self):
        for _, container, expected in self.states.values():
            if isinstance(container, MutableSet):
                matches = {id(item) for item in container} == {
                    id(item) for item in expected
                }
            else:
                matches = _signature(container) == _signature(expected)
            if not matches:
                raise ProviderUndoConflict("provider_unattributed_mutation")

    @contextmanager
    def capture(self, owner: str, incarnation: str | None):
        if not self.recording or self.depth:
            yield
            return
        self.verify()
        self.depth += 1
        try:
            yield
        finally:
            self.depth -= 1
            for provider, container, before in list(self.states.values()):
                changes = []
                if isinstance(container, MutableMapping):
                    for key in dict.fromkeys([*before, *container]):
                        old, new = (
                            before.get(key, _MISSING),
                            container.get(key, _MISSING),
                        )
                        if old is not new:
                            changes.append((key, old, new))
                else:
                    old_ids = {id(item) for item in before}
                    new_ids = {id(item) for item in container}
                    if isinstance(container, MutableSequence):
                        old_common = [
                            id(item) for item in before if id(item) in new_ids
                        ]
                        new_common = [
                            id(item) for item in container if id(item) in old_ids
                        ]
                        if old_common != new_common:
                            raise ProviderUndoConflict("provider_sequence_reordered")
                    changes.extend(
                        (i, item, _MISSING)
                        for i, item in enumerate(before)
                        if id(item) not in new_ids
                    )
                    changes.extend(
                        (i, _MISSING, item)
                        for i, item in enumerate(container)
                        if id(item) not in old_ids
                    )
                for key, old, new in changes:
                    self.entries.append(
                        UndoEntry(
                            self.transaction_id,
                            owner,
                            incarnation,
                            provider,
                            container,
                            key,
                            old,
                            new,
                            _signature(new) if isinstance(new, _CONTAINERS) else None,
                            tuple(_leaves(new)) if new is not _MISSING else (),
                            tuple(before[key + 1 :])
                            if isinstance(container, MutableSequence)
                            and old is not _MISSING
                            else (),
                        )
                    )
                self.states[id(container)] = (provider, container, _shallow(container))
            self._observe()

    def rollback(self):
        self.recording = False
        errors = []
        for entry in reversed(self.entries):
            try:
                entry.undo()
            except ProviderUndoConflict as error:
                errors.append(error)
        if errors:
            raise errors[0]

    def receipts(self, owner_id, incarnation_id=None):
        self.verify()
        receipts = {}
        for entry in self.entries:
            if entry.owner != owner_id or entry.incarnation != incarnation_id:
                continue
            if entry.after is _MISSING or entry.undone:
                continue
            for value in entry.registered_values:
                receipt_id = f"{entry.provider}:{id(value)}"
                receipts[receipt_id] = ResourceReceipt(
                    receipt_id=receipt_id,
                    provider=entry.provider,
                    resource_type="registration",
                    owner_id=owner_id,
                    incarnation_id=incarnation_id,
                    detail={"transaction_id": self.transaction_id},
                )
        return list(receipts.values())


active_undo: ContextVar[ProviderUndoLog | None] = ContextVar(
    "provider_undo", default=None
)


@contextmanager
def provider_capture(owner: str, incarnation: str | None = None):
    journal = active_undo.get()
    if journal is None:
        yield
    else:
        with journal.capture(owner, incarnation):
            yield


def provider_transaction(func):
    @wraps(func)
    async def wrapped(*args, **kwargs):
        from zhenxun.services.runtime_mutation import runtime_mutation_coordinator

        journal = ProviderUndoLog(
            runtime_mutation_coordinator.current_operation_id or "local"
        )
        token = active_undo.set(journal)
        try:
            return await func(*args, **kwargs)
        finally:
            journal.recording = False
            active_undo.reset(token)

    return wrapped
