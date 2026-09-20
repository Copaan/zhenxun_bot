"""Persistent archive dependency ownership shared by both plugin stores."""

from __future__ import annotations

import hashlib
import json
import platform
import sys
from typing import Any

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from zhenxun.services.plugin_store import plugin_store_receipts as receipts


class ArchiveDependencyConflict(RuntimeError):
    def __init__(
        self,
        code: str = "archive_dependency_conflict",
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.code = code
        context = {
            "python": platform.python_version(),
            "platform": sys.platform,
        }
        try:
            from zhenxun.services.nonebot_store.storage import load_manifest

            context["active_generation"] = load_manifest().get("active_generation")
        except Exception:
            context["active_generation"] = None
        self.details = {**context, **(details or {})}
        message = code
        if self.details:
            message = (
                f"{code}: {json.dumps(self.details, ensure_ascii=True, sort_keys=True)}"
            )
        super().__init__(message)


def archive_environment_fingerprint() -> str:
    payload = {
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": sys.platform,
        "machine": platform.machine(),
        # The value is hashed; it only distinguishes a receipt created on a
        # different host and is never exposed as a diagnostic field.
        "node": platform.node(),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def archive_dependency_contract() -> dict[str, Any]:
    from zhenxun.services.plugin_store import plugin_store_transaction as source

    records = []
    installed = (
        receipts.StoreReceiptStore.load() if receipts._RECEIPT_FILE.exists() else {}
    )
    for key, receipt in installed.items():
        if key.startswith("local_archive:") or (
            isinstance(receipt, dict) and receipt.get("source") == "local_archive"
        ):
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
    wheels_only_packages: set[str] = set()
    source_build_packages: set[str] = set()
    source_revisions: set[str] = set()
    legacy_receipts: list[str] = []
    resolved_records: list[str] = []
    current_fingerprint = archive_environment_fingerprint()
    for key, receipt in records:
        if not isinstance(receipt, dict):
            raise ArchiveDependencyConflict(
                "archive_dependency_receipt_invalid",
                details={"archive_owner": key, "reason": "receipt_not_object"},
            )
        roots = receipt.get("dependency_inputs")
        pins = receipt.get("dependency_packages")
        if not isinstance(roots, list):
            legacy_receipts.append(key)
            continue
        if not isinstance(pins, dict):
            # Old receipts are evidence of requirements, not portable lock
            # files. Re-resolve them on this machine from dependency_inputs.
            legacy_receipts.append(key)
            pins = {}
        portable = receipt.get("environment_fingerprint") == current_fingerprint
        if not portable and pins:
            legacy_receipts.append(key)
            pins = {}
        else:
            resolved_records.append(key)
        parsed = []
        local_pins = {}
        for raw in roots:
            try:
                requirement = Requirement(raw)
            except (InvalidRequirement, TypeError) as error:
                raise ArchiveDependencyConflict(
                    "archive_dependency_receipt_invalid",
                    details={
                        "archive_owner": key,
                        "reason": "invalid_requirement",
                        "value": str(raw)[:200],
                    },
                ) from error
            if requirement.url:
                raise ArchiveDependencyConflict(
                    "archive_dependency_receipt_invalid",
                    details={"archive_owner": key, "reason": "url_requirement"},
                )
            parsed.append(requirement)
            requirements.add(str(requirement))
        for raw_name, version in pins.items():
            try:
                name = canonicalize_name(raw_name, validate=True)
                Version(version)
            except (ValueError, TypeError, InvalidVersion) as error:
                raise ArchiveDependencyConflict(
                    "archive_dependency_receipt_invalid",
                    details={
                        "archive_owner": key,
                        "reason": "invalid_pin",
                        "package": str(raw_name),
                        "version": str(version),
                    },
                ) from error
            if name in local_pins and local_pins[name] != version:
                raise ArchiveDependencyConflict(
                    details={
                        "archive_owner": key,
                        "package": name,
                        "versions": [local_pins[name], version],
                        "conflict_source": "one_receipt",
                    }
                )
            local_pins[name] = version
            if receipt.get("dependency_source_build") is not True:
                wheels_only_packages.add(name)
            else:
                revision = receipt.get("dependency_source_revision")
                if not isinstance(revision, str) or len(revision) != 64:
                    raise ArchiveDependencyConflict(
                        "archive_dependency_receipt_invalid",
                        details={
                            "archive_owner": key,
                            "reason": "source_revision_missing",
                        },
                    )
                source_revisions.add(revision)
                source_build_packages.add(name)
        # One receipt cannot borrow missing evidence from another archive.
        for requirement in parsed:
            if requirement.marker and not requirement.marker.evaluate():
                continue
            version = local_pins.get(canonicalize_name(requirement.name))
            if not local_pins:
                # Legacy or cross-machine receipts are re-resolved from the
                # requirements above and do not carry a portable lock.
                continue
            if version is None or not requirement.specifier.contains(
                version, prereleases=True
            ):
                raise ArchiveDependencyConflict(
                    "archive_dependency_receipt_invalid",
                    details={
                        "archive_owner": key,
                        "reason": "pin_outside_requirement",
                        "package": canonicalize_name(requirement.name),
                        "requirement": str(requirement),
                        "version": version,
                    },
                )
        for name, version in local_pins.items():
            if name in packages and packages[name] != version:
                raise ArchiveDependencyConflict(
                    details={
                        "archive_owner": key,
                        "package": name,
                        "versions": [packages[name], version],
                        "conflict_source": "archive_receipts",
                        "owners": [
                            *resolved_records,
                        ],
                    }
                )
            packages[name] = version
    return {
        "requirements": sorted(requirements),
        "packages": dict(sorted(packages.items())),
        "store_keys": sorted({key for key, _ in records}),
        "wheels_only_packages": sorted(wheels_only_packages),
        "source_build_packages": sorted(source_build_packages),
        "source_revisions": sorted(source_revisions),
        "legacy_receipts": sorted(set(legacy_receipts)),
        "resolved_records": sorted(set(resolved_records)),
        "requires_wheels": bool(wheels_only_packages),
        "environment_fingerprint": current_fingerprint,
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
        from zhenxun.services.nonebot_store.dependencies import protected_core

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
                raise ArchiveDependencyConflict(
                    details={
                        "package": name,
                        "expected": core[name],
                        "actual": version,
                        "conflict_source": "protected_core",
                        "archive_owners": contract.get("resolved_records", []),
                    }
                )
            continue
        existing = target.get(name)
        if existing is not None and (
            not isinstance(existing, dict) or existing.get("version") != version
        ):
            raise ArchiveDependencyConflict(
                details={
                    "package": name,
                    "expected": version,
                    "actual": existing.get("version")
                    if isinstance(existing, dict)
                    else existing,
                    "archive_owners": contract.get("resolved_records", []),
                }
            )
        target[name] = {"version": version}
    manifest["packages"] = target
