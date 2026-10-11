"""Durable cleanup of settled transactions, independent of the active slot."""

from copy import deepcopy
from hashlib import sha256
import shutil

from . import storage


def _path(revision: str):
    return (
        storage.STORE_ROOT
        / "settled"
        / (sha256(revision.encode()).hexdigest() + ".json")
    )


def is_settled(transaction: dict | None) -> bool:
    if not transaction or not transaction.get("revision"):
        return False
    manifest = storage.load_manifest()
    if manifest.get("pending_verification"):
        return False
    if transaction.get("rollback_confirmed"):
        return manifest.get("active_generation") == transaction.get(
            "base_manifest", {}
        ).get("active_generation")
    return bool(
        transaction.get("verification_confirmed")
        and manifest.get("verified_transaction_revision") == transaction["revision"]
        and manifest.get("active_generation") == transaction.get("generation")
    )


def active_transaction() -> dict | None:
    value = storage.pending_transaction()
    return None if is_settled(value) else value


def archive_settled() -> dict | None:
    """Archive proven outcomes before releasing their slot under the store mutex."""
    transaction = storage.pending_transaction()
    if not is_settled(transaction):
        return None
    record = deepcopy(transaction)
    existing = storage.read_json(_path(record["revision"]), {})
    record["cleanup_state"] = existing.get(
        "cleanup_state", record.get("cleanup_state", "pending")
    )
    record["settled_at"] = existing.get("settled_at", storage.utc_now())
    storage.write_json(_path(record["revision"]), record)
    storage.clear_pending_transaction()
    return record


def records() -> list[dict]:
    result = []
    for path in (storage.STORE_ROOT / "settled").glob("*.json"):
        value = storage.read_json(path, {})
        if isinstance(value, dict) and value.get("revision"):
            result.append(value)
    return sorted(result, key=lambda value: value.get("settled_at", ""), reverse=True)


def transaction_for_operation(identity: str) -> dict | None:
    return next(
        (
            record
            for record in records()
            if any(
                op.get("operation_id") == identity
                for op in record.get("operations", [])
            )
        ),
        None,
    )


def retry_cleanup(revision: str) -> dict:
    """Delete only this settled transaction's resources that nobody still uses."""
    from .dependencies import safe_process_error
    from .runtime import referenced_generations

    path = _path(revision)
    transaction = storage.read_json(path, None)
    if not isinstance(transaction, dict) or transaction.get("revision") != revision:
        raise ValueError("dependency_cleanup_not_found")
    if transaction.get("cleanup_state") == "completed":
        return transaction
    try:
        keep = referenced_generations()
        manifest = storage.load_manifest()
        for source in (manifest, storage.pending_transaction() or {}):
            for key in ("active_generation", "previous_generation", "generation"):
                if isinstance(source.get(key), int):
                    keep.add(source[key])
        candidates = {
            transaction.get("generation")
            if transaction.get("rollback_confirmed")
            else transaction.get("base_manifest", {}).get("active_generation")
        }
        deferred = []
        for generation in candidates:
            if not isinstance(generation, int):
                continue
            if generation in keep:
                deferred.append(generation)
            else:
                storage.remove_generation(generation)
        backup_root = (storage.STORE_ROOT / "orm-backups").resolve()
        backup = (backup_root / revision).resolve()
        if backup.parent != backup_root:
            raise ValueError("dependency_cleanup_revision_invalid")
        active = storage.pending_transaction() or {}
        if active.get("revision") == revision:
            raise RuntimeError("dependency_cleanup_in_use")
        if backup.is_dir():
            shutil.rmtree(backup)
        status = storage.read_json(storage.ORM_MIGRATION_STATUS_FILE, {})
        if status.get("revision") == revision:
            storage.ORM_MIGRATION_STATUS_FILE.unlink(missing_ok=True)
        transaction.update(
            cleanup_state="deferred" if deferred else "completed",
            cleanup_error=None,
            cleanup_retained_generations=sorted(deferred),
        )
    except Exception as error:
        transaction.update(
            cleanup_state="pending",
            cleanup_error={
                "code": getattr(error, "code", type(error).__name__),
                "diagnostic": safe_process_error(str(error)),
            },
        )
    storage.write_json(path, transaction)
    return transaction


def public_records() -> list[dict]:
    values = records()
    pending = storage.pending_transaction()
    if is_settled(pending) and not any(
        item["revision"] == pending["revision"] for item in values
    ):
        values.insert(
            0, {**pending, "cleanup_state": pending.get("cleanup_state", "pending")}
        )
    return [
        {
            key: record.get(key)
            for key in (
                "revision",
                "state",
                "cleanup_state",
                "cleanup_error",
                "cleanup_retained_generations",
                "verification_confirmed",
                "rollback_confirmed",
                "settled_at",
            )
        }
        for record in values
        if record.get("cleanup_state") != "completed"
    ]
