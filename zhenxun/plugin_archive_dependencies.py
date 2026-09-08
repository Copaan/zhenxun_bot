"""Persistent archive dependency ownership shared by both plugin stores."""

from __future__ import annotations

from typing import Any

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from zhenxun import plugin_store_receipts as receipts


class ArchiveDependencyConflict(RuntimeError):
    def __init__(self, code: str = "archive_dependency_conflict") -> None:
        self.code = code
        super().__init__(code)


def archive_dependency_contract() -> dict[str, Any]:
    from zhenxun import plugin_store_transaction as source

    records = []
    installed = (
        receipts.StoreReceiptStore.load() if receipts._RECEIPT_FILE.exists() else {}
    )
    for key, receipt in installed.items():
        if key.startswith("local_archive:") or receipt.get("source") == "local_archive":
            records.append((key, receipt))
    pending = source.pending_transaction() if source.PENDING_FILE.exists() else None
    for operation in (pending or {}).get("operations", []):
        key = str(operation.get("store_key") or "")
        receipt = operation.get("receipt") or {}
        if key.startswith("local_archive:") or receipt.get("source") == "local_archive":
            # Keep installed roots until uninstall commits. A pending operation
            # may supply evidence, but missing evidence must not become empties.
            records.append(
                (
                    key,
                    {
                        "dependency_inputs": operation.get("dependency_inputs"),
                        "dependency_packages": operation.get("dependency_packages"),
                        **receipt,
                    },
                )
            )
    requirements: set[str] = set()
    packages: dict[str, str] = {}
    for _key, receipt in records:
        if not isinstance(receipt, dict):
            raise ArchiveDependencyConflict("archive_dependency_receipt_invalid")
        roots = receipt.get("dependency_inputs")
        pins = receipt.get("dependency_packages")
        if not isinstance(roots, list) or not isinstance(pins, dict):
            raise ArchiveDependencyConflict("archive_dependency_receipt_incomplete")
        parsed = []
        local_pins = {}
        for raw in roots:
            try:
                requirement = Requirement(raw)
            except (InvalidRequirement, TypeError) as error:
                raise ArchiveDependencyConflict(
                    "archive_dependency_receipt_invalid"
                ) from error
            if requirement.url:
                raise ArchiveDependencyConflict("archive_dependency_receipt_invalid")
            parsed.append(requirement)
            requirements.add(str(requirement))
        for raw_name, version in pins.items():
            try:
                name = canonicalize_name(raw_name, validate=True)
                Version(version)
            except (ValueError, TypeError, InvalidVersion) as error:
                raise ArchiveDependencyConflict(
                    "archive_dependency_receipt_invalid"
                ) from error
            if name in local_pins and local_pins[name] != version:
                raise ArchiveDependencyConflict()
            local_pins[name] = version
        # One receipt cannot borrow missing evidence from another archive.
        for requirement in parsed:
            if requirement.marker and not requirement.marker.evaluate():
                continue
            version = local_pins.get(canonicalize_name(requirement.name))
            if version is None or not requirement.specifier.contains(
                version, prereleases=True
            ):
                raise ArchiveDependencyConflict("archive_dependency_receipt_invalid")
        for name, version in local_pins.items():
            if name in packages and packages[name] != version:
                raise ArchiveDependencyConflict()
            packages[name] = version
    return {
        "requirements": sorted(requirements),
        "packages": dict(sorted(packages.items())),
        "store_keys": sorted({key for key, _ in records}),
    }


def preserve_archive_dependencies(
    manifest: dict[str, Any],
    *,
    core: dict[str, str] | None = None,
) -> None:
    contract = archive_dependency_contract()
    if not contract["packages"]:
        return
    if core is None:
        from zhenxun.nonebot_store.dependencies import protected_core

        core = protected_core()
    target = manifest.setdefault("packages", {})
    if not isinstance(target, dict):
        raise ArchiveDependencyConflict("dependency_plan_invalid")
    normalized = {}
    for raw_name, value in target.items():
        name = canonicalize_name(raw_name)
        if name in normalized and normalized[name] != value:
            raise ArchiveDependencyConflict()
        normalized[name] = value
    target = normalized
    for name, version in contract["packages"].items():
        if name in core:
            if core[name] != version:
                raise ArchiveDependencyConflict()
            continue
        existing = target.get(name)
        if existing is not None and (
            not isinstance(existing, dict) or existing.get("version") != version
        ):
            raise ArchiveDependencyConflict()
        target[name] = {"version": version}
    manifest["packages"] = target
