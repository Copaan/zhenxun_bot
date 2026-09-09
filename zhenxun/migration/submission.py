from __future__ import annotations

import asyncio
from dataclasses import asdict
import hashlib
import json
from pathlib import Path

from zhenxun.utils.atomic_json import read_json_locked

from .control import request_control
from .errors import MigrationError
from .snapshot import ExportOptions
from .tasks import TaskStore


async def submit_restore(
    project: Path,
    session: str,
    options: dict,
    *,
    private: dict,
    identity: str | None = None,
) -> dict:
    """Transfer a confirmed restore under the worker's mutation/Operation owner."""
    from zhenxun.services.runtime_mutation import runtime_mutation_coordinator
    from zhenxun.services.startup import startup_coordinator

    from .commit import options_digest
    from .configuration import bound_environment
    from .maintenance_app import ManagementSnapshot

    async def submit():
        from zhenxun.configs.config import Config
        from zhenxun.nonebot_store.storage import pending_transaction
        from zhenxun.plugin_store_transaction import (
            pending_transaction as source_pending,
        )
        from zhenxun.update_service import applied_update_pending, pending_job

        if (
            pending_transaction()
            or source_pending()
            or pending_job()
            or applied_update_pending()
        ):
            raise MigrationError("migration_other_transaction_pending", status=409)
        state = startup_coordinator.snapshot()
        first = options.get("first_deployment") is True
        if state.get("operating_mode") != (
            "setup_only" if first else "normal"
        ) or state.get("state") not in (
            {"management_ready"} if first else {"warmup_ready", "degraded"}
        ):
            raise MigrationError("migration_worker_not_ready", status=409)
        if not options.get("source_trusted") or not options.get(
            "replacement_confirmed"
        ):
            raise MigrationError("migration_restore_confirmation_required")
        bound_environment(options, private.get("configuration"))
        store = TaskStore(project)
        values = {**options, "requester_boot_id": state["boot_id"]}
        from .bootstrap import bootstrap_authority

        management = (
            bootstrap_authority(project).management(session)
            if first
            else ManagementSnapshot.parse(
                {
                    "username": Config.get_config("web-ui", "username"),
                    "password": Config.get_config("web-ui", "password"),
                    "secret": Config.get_config("web-ui", "secret"),
                    "private_only": True,
                }
            )
        )
        if identity is None:
            job = store.create_job(session, "restore", values)
        else:
            existing = store.read("jobs", identity, session=session)
            if existing["action"] != "restore" or existing["options"] != options:
                raise MigrationError("migration_operation_id_conflict", status=409)
            job = store.bind_requester(identity, session, state["boot_id"])
            values = job["options"]
        store.reserve(job["id"])
        try:
            receipt = await asyncio.to_thread(
                request_control,
                project,
                "restore",
                {
                    "task_id": job["id"],
                    "management": asdict(management),
                    "private": private,
                },
            )
        except MigrationError as error:
            recorded = read_json_locked(
                store.path("jobs", job["id"]).parent / "handoff.json", None
            )
            if not recorded and not error.code.startswith("migration_control_"):
                store.transition(job["id"], "failed", error_code=error.code)
            raise
        recorded = read_json_locked(
            store.path("jobs", job["id"]).parent / "handoff.json", None
        )
        if (
            receipt != recorded
            or receipt.get("job_id") != job["id"]
            or receipt.get("launcher_boot_id") != state["launcher_boot_id"]
            or receipt.get("options_sha256") != options_digest(values)
        ):
            raise MigrationError("migration_handoff_unconfirmed", status=409)
        return store.public(store.read("jobs", job["id"]))

    return await runtime_mutation_coordinator.run_owned(
        "migration_restore_handoff", submit, fail_if_busy=True
    )


async def submit_export(
    project: Path, session: str, options: ExportOptions, *, password: str | None = None
) -> dict:
    """Reserve under the worker mutation owner, then transfer durable ownership."""
    from zhenxun.services.runtime_mutation import runtime_mutation_coordinator
    from zhenxun.services.startup import startup_coordinator

    async def submit():
        from zhenxun.configs.config import Config
        from zhenxun.nonebot_store.storage import pending_transaction
        from zhenxun.plugin_store_transaction import (
            pending_transaction as source_pending,
        )
        from zhenxun.update_service import applied_update_pending, pending_job

        from .maintenance_app import ManagementSnapshot

        if (
            pending_transaction()
            or source_pending()
            or pending_job()
            or applied_update_pending()
        ):
            raise MigrationError("migration_other_transaction_pending", status=409)
        if password is not None and (
            not isinstance(password, str) or not 1 <= len(password.encode()) <= 4096
        ):
            raise MigrationError("migration_password_invalid")
        if not password and not options.plaintext_confirmed:
            raise MigrationError("migration_plaintext_confirmation_required")
        management = ManagementSnapshot.parse(
            {
                "username": Config.get_config("web-ui", "username"),
                "password": Config.get_config("web-ui", "password"),
                "secret": Config.get_config("web-ui", "secret"),
                "private_only": True,
            }
        )
        state = startup_coordinator.snapshot()
        if state.get("operating_mode") != "normal" or state.get("state") not in {
            "warmup_ready",
            "degraded",
        }:
            raise MigrationError("migration_worker_not_ready", status=409)
        store = TaskStore(project)
        values = {
            **asdict(options),
            "categories": sorted(options.categories),
            "requester_boot_id": state["boot_id"],
        }
        job = store.create_job(session, "export", values)
        # The persistent reservation blocks mutations across processes and
        # naturally releases them if preparation fails before worker shutdown.
        store.reserve(job["id"])
        try:
            receipt = await asyncio.to_thread(
                request_control,
                project,
                "export",
                {
                    "task_id": job["id"],
                    "archive_password": password,
                    "management": asdict(management),
                },
            )
        except MigrationError as error:
            recorded = read_json_locked(
                store.path("jobs", job["id"]).parent / "handoff.json", None
            )
            uncertain = error.code.startswith("migration_control_")
            if not recorded and not uncertain:
                store.transition(job["id"], "failed", error_code=error.code)
            raise
        recorded = read_json_locked(
            store.path("jobs", job["id"]).parent / "handoff.json", None
        )
        if (
            receipt != recorded
            or receipt.get("job_id") != job["id"]
            or receipt.get("launcher_boot_id") != state["launcher_boot_id"]
            or receipt.get("options_sha256")
            != hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()
        ):
            raise MigrationError("migration_handoff_unconfirmed", status=409)
        return store.public(store.read("jobs", job["id"]))

    return await runtime_mutation_coordinator.run_owned(
        "migration_export_handoff", submit, fail_if_busy=True
    )
