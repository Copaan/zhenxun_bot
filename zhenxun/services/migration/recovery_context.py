from __future__ import annotations

from dataclasses import asdict, replace
import os

from zhenxun.configs.webui_tls import WebUITLSSettings
from zhenxun.utils.atomic_json import read_json_locked, write_json_locked
from zhenxun.utils.passwords import hash_password, is_password_hash

from .access import private_directory
from .archive import file_hash
from .commit import options_digest
from .errors import MigrationError
from .maintenance_app import ManagementSnapshot
from .paths import contained_path


def save_management_context(store, identity, snapshot, network, *, lease):
    """Keep target authentication separate from restored configuration and jobs API."""
    lease.require_held()
    if lease.project.absolute() != store.project:
        raise MigrationError("migration_management_snapshot_invalid")
    directory = private_directory(store.path("jobs", identity).parent / "management")
    receipt = directory / "context.json"
    if receipt.exists():
        return load_management_context(store, identity, lease=lease)
    if not is_password_hash(snapshot.password):
        snapshot = replace(snapshot, password=hash_password(snapshot.password))
    files = {}
    original_network = asdict(network)
    if network.enabled:
        for field in ("certfile", "keyfile"):
            from pathlib import Path

            source = Path(getattr(network, field))
            source = contained_path(source.parent, source.name, regular=True)
            if source.stat().st_size > 1024 * 1024:
                raise MigrationError("migration_management_certificate_limit")
            data = source.read_bytes()
            target = contained_path(directory, field + ".pem")
            # Private directory permissions precede any credential material.
            with target.open("xb") as output:
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
            if os.name != "nt":
                target.chmod(0o600)
            files[field] = {"path": target.name, "sha256": file_hash(target)}
    value = {
        "schema": 1,
        "job_id": identity,
        "options_sha256": options_digest(store.read("jobs", identity)["options"]),
        "management": asdict(snapshot),
        "network": original_network,
        "files": files,
    }
    write_json_locked(receipt, value)
    if os.name != "nt":
        receipt.chmod(0o600)
    return load_management_context(store, identity, lease=lease)


def load_management_context(store, identity, *, lease):
    lease.require_held()
    if lease.project.absolute() != store.project:
        raise MigrationError("migration_management_snapshot_invalid")
    directory = contained_path(store.path("jobs", identity).parent, "management")
    path = contained_path(directory, "context.json", regular=True)
    if not path.exists() or path.stat().st_size > 64 * 1024:
        raise MigrationError("migration_management_context_unavailable", status=409)
    value = read_json_locked(path, None)
    if (
        not isinstance(value, dict)
        or value.get("schema") != 1
        or value.get("job_id") != identity
        or value.get("options_sha256")
        != options_digest(store.read("jobs", identity)["options"])
    ):
        raise MigrationError("migration_management_context_changed", status=409)
    snapshot = ManagementSnapshot.parse(value.get("management"))
    try:
        network = WebUITLSSettings(**value["network"])
        if network.enabled:
            paths = {}
            for field in ("certfile", "keyfile"):
                entry = value["files"][field]
                target = contained_path(directory, entry["path"], regular=True)
                if file_hash(target) != entry["sha256"]:
                    raise MigrationError(
                        "migration_management_context_changed", status=409
                    )
                paths[field] = str(target)
            network = replace(network, **paths)
    except (TypeError, KeyError, ValueError):
        raise MigrationError(
            "migration_management_context_changed", status=409
        ) from None
    return snapshot, network
