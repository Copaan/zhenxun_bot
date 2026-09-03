from __future__ import annotations

from collections.abc import Iterable, MutableMapping, MutableSequence, MutableSet
from dataclasses import dataclass, field
from typing import Any, Literal

from .models import ResourceReceipt

ContainerKind = Literal["mapping", "sequence", "set"]


@dataclass(slots=True)
class ProviderContainerSnapshot:
    name: str
    container: Any
    kind: ContainerKind
    values: Any
    undo_added_ids: set[int] = field(default_factory=set)
    undo_removed_ids: set[int] = field(default_factory=set)
    undo_added_values: list[Any] = field(default_factory=list)
    sealed: bool = False

    def added(self) -> list[Any]:
        baseline_ids = _identity_set(self.values)
        return [
            item
            for item in _iter_values(self.container)
            if id(item) not in baseline_ids
        ]

    def restore(self) -> None:
        self.seal()
        _apply_identity_undo(
            self.container,
            self.values,
            self.undo_added_ids,
            self.undo_removed_ids,
        )

    def seal(self) -> None:
        if self.sealed:
            return
        baseline_ids = _identity_set(self.values)
        current_values = list(_iter_values(self.container))
        current_ids = {id(item) for item in current_values}
        self.undo_added_values = [
            item for item in current_values if id(item) not in baseline_ids
        ]
        self.undo_added_ids = {id(item) for item in self.undo_added_values}
        self.undo_removed_ids = baseline_ids - current_ids
        self.sealed = True


@dataclass(slots=True)
class RuntimeProviderSnapshot:
    containers: list[ProviderContainerSnapshot]
    asgi_app: Any | None = None

    def receipts(
        self, owner_id: str, incarnation_id: str | None = None
    ) -> list[ResourceReceipt]:
        receipts: list[ResourceReceipt] = []
        for snapshot in self.containers:
            snapshot.seal()
            for value in snapshot.undo_added_values:
                receipts.append(
                    ResourceReceipt(
                        receipt_id=f"{snapshot.name}:{id(value)}",
                        provider=snapshot.name,
                        resource_type="registration",
                        owner_id=owner_id,
                        incarnation_id=incarnation_id,
                    )
                )
        return receipts

    def rollback(self) -> None:
        error: BaseException | None = None
        for snapshot in reversed(self.containers):
            try:
                snapshot.restore()
            except BaseException as caught:
                error = error or caught
        if self.asgi_app is not None and hasattr(self.asgi_app, "openapi_schema"):
            self.asgi_app.openapi_schema = None
        if error is not None:
            raise error


def _append(
    snapshots: list[ProviderContainerSnapshot], name: str, container: Any
) -> None:
    if isinstance(container, MutableMapping):
        values = {key: _copy_container(value) for key, value in container.items()}
        kind: ContainerKind = "mapping"
    elif isinstance(container, MutableSet):
        values = set(container)
        kind = "set"
    elif isinstance(container, MutableSequence):
        values = list(container)
        kind = "sequence"
    else:
        return
    snapshots.append(ProviderContainerSnapshot(name, container, kind, values))


def _copy_container(value: Any) -> Any:
    if isinstance(value, MutableMapping):
        return {key: _copy_container(item) for key, item in value.items()}
    if isinstance(value, MutableSet):
        return set(value)
    if isinstance(value, MutableSequence):
        return list(value)
    return value


def _iter_values(value: Any) -> Iterable[Any]:
    if isinstance(value, MutableMapping):
        for item in value.values():
            yield from _iter_values(item)
    elif isinstance(value, MutableSet | MutableSequence):
        for item in value:
            yield from _iter_values(item)
    else:
        yield value


def _identity_set(value: Any) -> set[int]:
    return {id(item) for item in _iter_values(value)}


def _mapping_pop(mapping: MutableMapping[Any, Any], key: Any) -> None:
    try:
        mapping.pop(key)
    except KeyError:
        pass


def _apply_identity_undo(
    current: Any,
    baseline: Any,
    added_ids: set[int],
    removed_ids: set[int],
) -> None:
    if isinstance(current, MutableMapping):
        baseline_mapping = baseline if isinstance(baseline, MutableMapping) else {}
        for key, value in list(current.items()):
            if key not in baseline_mapping:
                if id(value) in added_ids:
                    _mapping_pop(current, key)
                elif isinstance(value, MutableMapping | MutableSequence | MutableSet):
                    _apply_identity_undo(value, type(value)(), added_ids, set())
                    if not value:
                        _mapping_pop(current, key)
                continue
            baseline_value = baseline_mapping[key]
            if isinstance(value, MutableMapping | MutableSequence | MutableSet):
                _apply_identity_undo(value, baseline_value, added_ids, removed_ids)
            elif id(value) in added_ids:
                current[key] = baseline_value
        for key, value in baseline_mapping.items():
            if key not in current and any(
                id(item) in removed_ids for item in _iter_values(value)
            ):
                current[key] = _copy_container(value)
    elif isinstance(current, MutableSequence):
        baseline_sequence = list(baseline)
        retained = [item for item in current if id(item) not in added_ids]
        current_ids = {id(item) for item in retained}
        for index, item in enumerate(baseline_sequence):
            if id(item) in removed_ids and id(item) not in current_ids:
                retained.insert(min(index, len(retained)), item)
                current_ids.add(id(item))
        current[:] = retained
    elif isinstance(current, MutableSet):
        current.difference_update(
            [item for item in list(current) if id(item) in added_ids]
        )
        current.update(item for item in baseline if id(item) in removed_ids)


def capture_runtime_providers(driver: Any | None = None) -> RuntimeProviderSnapshot:
    """Discover mutable registries structurally instead of by framework version."""
    snapshots: list[ProviderContainerSnapshot] = []
    try:
        import nonebot
        from nonebot.internal.adapter import Bot
        from nonebot.matcher import matchers
        import nonebot.message as message
        import nonebot.plugin as plugin
        from nonebot.rule import TrieRule

        driver = driver or nonebot.get_driver()
        for name, container in (
            ("nonebot.plugins", getattr(plugin, "_plugins", None)),
            ("nonebot.managers", getattr(plugin, "_managers", None)),
            ("nonebot.matchers", matchers),
            ("nonebot.trie", getattr(TrieRule, "prefix", None)),
            (
                "nonebot.event_preprocessors",
                getattr(message, "_event_preprocessors", None),
            ),
            (
                "nonebot.event_postprocessors",
                getattr(message, "_event_postprocessors", None),
            ),
            ("nonebot.run_preprocessors", getattr(message, "_run_preprocessors", None)),
            (
                "nonebot.run_postprocessors",
                getattr(message, "_run_postprocessors", None),
            ),
            ("nonebot.bot_calling_api", getattr(Bot, "_calling_api_hook", None)),
            ("nonebot.bot_called_api", getattr(Bot, "_called_api_hook", None)),
            ("nonebot.bot_connect", getattr(driver, "_bot_connection_hook", None)),
            (
                "nonebot.bot_disconnect",
                getattr(driver, "_bot_disconnection_hook", None),
            ),
        ):
            _append(snapshots, name, container)
        lifespan = getattr(driver, "_lifespan", None)
        for name in ("_startup_funcs", "_ready_funcs", "_shutdown_funcs"):
            _append(
                snapshots,
                f"nonebot.lifespan.{name.removeprefix('_')}",
                getattr(lifespan, name, None),
            )
        try:
            app = nonebot.get_app()
        except (AssertionError, AttributeError, ValueError):
            app = getattr(driver, "server_app", None) or getattr(
                driver, "_server_app", None
            )
        _append(snapshots, "asgi.routes", getattr(app, "routes", None))
    except (ImportError, RuntimeError):
        pass
    return RuntimeProviderSnapshot(snapshots, app if "app" in locals() else None)


def receipt_provider_names(receipts: Iterable[ResourceReceipt]) -> set[str]:
    return {receipt.provider for receipt in receipts}


__all__ = [
    "ProviderContainerSnapshot",
    "RuntimeProviderSnapshot",
    "capture_runtime_providers",
    "receipt_provider_names",
]
