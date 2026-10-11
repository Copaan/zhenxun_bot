from __future__ import annotations

import asyncio
from copy import deepcopy
import os
import time

from filelock import FileLock, Timeout

from zhenxun.utils.atomic_json import (
    mutate_json_locked,
    read_json_locked,
    write_json_locked,
)

from .defaults import RESTORE_FLOW_VERSION, resolve_restore
from .errors import MigrationError
from .inspection import WORKER_GENERATION, _process_created_at
from .tasks import TERMINAL, MigrationBudget

_RUNNERS: dict[tuple[str, str], asyncio.Task] = {}


async def cancel_preparation(application, identity):
    store = application.store
    job = store.request_cancel(identity)
    if preparation_only(store, job):
        task = _RUNNERS.get((str(application.project), identity))
        if task and not task.done():
            task.cancel()
            await asyncio.shield(task)
        elif job["stage"] not in TERMINAL:
            store.transition(
                identity,
                "cancelled",
                error_code="migration_cancelled",
                progress={"not_stopped": True},
            )
    return store.public(store.read("jobs", identity))


def setup_restore_lock(store):
    store._initialize()
    return FileLock(
        str(store.root / "setup-restore.lock"), timeout=0, thread_local=False
    )


def preparation_only(store, job):
    """Identify durable preparation with no evidence of a launcher submission."""
    if (
        not job
        or not job.get("flow")
        or job["stage"] not in {"queued", "preflight", "awaiting_credentials"}
    ):
        return False
    if job["options"].get("requester_boot_id"):
        return False
    directory = store.path("jobs", job["id"]).parent
    return not any(
        (directory / name).exists()
        for name in (
            "handoff.json",
            "restore-application.json",
            "restore-commit.json",
            "restore-stage/files-journal.json.events",
            "database-stage/database-journal.json.events",
            "database-stage/database-journal.json",
        )
    )


def recover_unsubmitted(store):
    """Keep interrupted preparation resumable without treating it as target writes."""
    job = store.active()
    if not preparation_only(store, job):
        return
    owner = job["flow"]
    created = _process_created_at(owner.get("pid", 0))
    runner = _RUNNERS.get((str(store.project), job["id"]))
    same_process = owner.get("pid") == os.getpid()
    if (
        created is not None
        and abs(created - (owner.get("created_at") or 0)) < 2
        and (not same_process or (runner is not None and not runner.done()))
    ):
        return
    if job["stage"] != "awaiting_credentials":
        store.transition(
            job["id"],
            "awaiting_credentials",
            progress={"not_stopped": True, "flow_version": RESTORE_FLOW_VERSION},
        )


async def preview(
    application, session, *, upload_id, first_deployment, private, task_id=None
):
    if type(first_deployment) is not bool:
        raise MigrationError("migration_restore_mode_required")
    if task_id:
        store = application.store
        job = store.read("jobs", task_id)
        if (
            not preparation_only(store, job)
            or job["stage"] != "awaiting_credentials"
            or job["flow"]["upload_id"] != upload_id
            or job["options"].get("first_deployment") != first_deployment
        ):
            raise MigrationError("migration_recovery_stage_invalid", status=409)
        upload = store.read("uploads", upload_id)
    else:
        upload = application.store.read("uploads", upload_id, session=session)
    if upload["stage"] != "sealed" or upload["expires_at"] <= application.store.clock():
        raise MigrationError("migration_upload_not_ready", status=409)
    value = await asyncio.to_thread(
        resolve_restore,
        application.project,
        upload,
        first_deployment=first_deployment,
        private=private,
    )
    return {
        key: value[key]
        for key in ("revision", "summary", "issues", "ready", "flow_version")
    }


async def submit(
    application,
    session,
    *,
    upload_id,
    first_deployment,
    private,
    request_id,
    revision,
    confirmed,
):
    if confirmed is not True or type(first_deployment) is not bool:
        raise MigrationError("migration_destructive_confirmation_required")
    if (
        not isinstance(private, dict)
        or not isinstance(revision, str)
        or len(revision) != 64
    ):
        raise MigrationError("migration_restore_options_invalid")
    store = application.store
    path = store.path("jobs", request_id)
    input_binding = store.private_fingerprint(
        {
            "upload_id": upload_id,
            "first_deployment": first_deployment,
            "private": private,
            "revision": revision,
        }
    )
    try:
        with setup_restore_lock(store):
            if path.exists():
                job = store.read("jobs", request_id, session=session)
                if job.get("flow", {}).get("input_binding") != input_binding:
                    raise MigrationError("migration_operation_id_conflict", status=409)
                if (
                    preparation_only(store, job)
                    and job["stage"] == "queued"
                    and not _RUNNERS.get((str(application.project), request_id))
                ):
                    owner = job["flow"]
                    created = _process_created_at(owner.get("pid", 0))
                    if (
                        owner.get("pid") == os.getpid()
                        or created is None
                        or abs(created - (owner.get("created_at") or 0)) >= 2
                    ):
                        store.reserve(request_id)
                        store.transition(
                            request_id,
                            "awaiting_credentials",
                            progress={"not_stopped": True},
                        )
                        return store.public(store.read("jobs", request_id))
                return store.public(job)
            if store.active():
                raise MigrationError("migration_operation_in_progress", status=409)
            upload = store.read("uploads", upload_id, session=session)
            if upload["stage"] != "sealed" or upload["expires_at"] <= store.clock():
                raise MigrationError("migration_upload_not_ready", status=409)
            flow = {
                "version": RESTORE_FLOW_VERSION,
                "upload_id": upload_id,
                "revision": revision,
                "input_binding": input_binding,
                "pid": os.getpid(),
                "created_at": _process_created_at(os.getpid()),
                "generation": WORKER_GENERATION,
                "deadline_at": time.time() + 6 * 3600,
            }
            job = store.create_job(
                session,
                "restore",
                {
                    "flow_version": RESTORE_FLOW_VERSION,
                    "first_deployment": first_deployment,
                },
                identity=request_id,
                flow=flow,
            )
            try:
                store.reserve(request_id)
            except MigrationError as error:
                store.transition(
                    request_id,
                    "failed",
                    error_code=error.code,
                    progress={"not_stopped": True},
                )
                raise
            store.transition(
                request_id,
                "preflight",
                progress={"not_stopped": True, "flow_version": RESTORE_FLOW_VERSION},
            )
            _start(application, session, job, deepcopy(private))
            return store.public(store.read("jobs", request_id))
    except Timeout:
        raise MigrationError("migration_operation_in_progress", status=409) from None


def _start(application, session, job, private):
    key = (str(application.project), job["id"])
    task = asyncio.create_task(
        _execute(application, session, job, private),
        name="migration-restore-prepare-" + job["id"],
    )
    _RUNNERS[key] = task

    def finished(value):
        if _RUNNERS.get(key) is value:
            _RUNNERS.pop(key, None)
        if not value.cancelled():
            value.exception()

    task.add_done_callback(finished)


async def _execute(application, session, job, private):
    store, identity = application.store, job["id"]
    resolved = {}
    handoff_attempted = False
    try:
        remaining = job["flow"]["deadline_at"] - time.time()
        if remaining <= 0:
            raise MigrationError("migration_budget_exhausted")
        budget = MigrationBudget.start(remaining)
        value = await asyncio.to_thread(
            resolve_restore,
            application.project,
            store.read("uploads", job["flow"]["upload_id"]),
            first_deployment=job["options"]["first_deployment"],
            private=private,
            budget=budget,
        )
        if not value["ready"]:
            raise MigrationError(
                "migration_restore_fields_required", details={"issues": value["issues"]}
            )
        if value["revision"] != job["flow"]["revision"]:
            raise MigrationError("migration_restore_preview_changed", status=409)
        resolved = value["private"]
        if store.read("jobs", identity)["cancel_requested"]:
            raise MigrationError("migration_cancelled")
        options = {**value["options"], "request_id": identity}
        if store.path("preflights", identity).exists():
            from .analysis_process import run_analysis

            prepared = store.read("preflights", identity)
            directory = store.path("preflights", identity).parent
            if not (directory / "analysis.json").exists():
                raise MigrationError(
                    "migration_preflight_interrupted",
                    status=409,
                    details={"preflight_id": identity},
                )
            if prepared["stage"] != "awaiting_confirmation":
                from .analysis import preflight_summary
                from .restore_phases import _digest

                plan = read_json_locked(directory / "analysis.json", {})
                if (
                    plan.get("schema") != 2
                    or plan.get("preflight_id") != identity
                    or plan.get("archive_sha256") != prepared["sha256"]
                    or plan.get("revision")
                    != _digest(
                        {key: item for key, item in plan.items() if key != "revision"}
                    )
                ):
                    raise MigrationError(
                        "migration_preflight_receipt_invalid", status=409
                    )
                prepared.update(
                    target_revision=plan["revision"], summary=preflight_summary(plan)
                )
                write_json_locked(store.path("preflights", identity), prepared)
            await run_analysis(
                application.project,
                "recheck",
                identity=identity,
                private=resolved,
                budget=budget,
            )
            prepared["private_binding"] = application._private_binding(resolved)
            prepared["stage"] = "awaiting_confirmation"
            prepared["owner"] = store.owner(session)
            prepared["expires_at"] = min(job["flow"]["deadline_at"], time.time() + 1800)
            write_json_locked(store.path("preflights", identity), prepared)
        else:
            await application.preflight(
                session,
                upload_id=job["flow"]["upload_id"],
                options=options,
                private=resolved,
                flow_job=identity,
                budget=budget,
            )
        prepared = store.read("preflights", identity)
        job_options = {
            **prepared["options"],
            "preflight_id": identity,
            "target_revision": prepared["target_revision"],
        }

        def bind(current):
            if current["cancel_requested"]:
                raise MigrationError("migration_cancelled")
            if current["stage"] != "preflight" or current.get("flow") != job["flow"]:
                raise MigrationError("migration_restore_preview_changed", status=409)
            current.update(
                options=job_options, stage="queued", revision=current["revision"] + 1
            )
            return current

        mutate_json_locked(store.path("jobs", identity), None, bind)
        if options["first_deployment"]:
            from .bootstrap import bootstrap_authority

            bootstrap_authority(application.project).set_administrator(
                session, resolved["administrator"]
            )
        handoff_attempted = True
        await application.confirm(
            session, identity, private=resolved, replacement_confirmed=True
        )
    except BaseException as error:
        current = store.read("jobs", identity)
        receipt = read_json_locked(
            store.path("jobs", identity).parent / "handoff.json", None
        )
        # A requester bound by submission may already have crossed IPC; only the
        # submission service can prove a failed handoff has not stopped the worker.
        uncertain = (
            handoff_attempted
            and current["options"].get("requester_boot_id")
            and current["stage"] not in TERMINAL
        )
        if not receipt and not uncertain and current["stage"] not in TERMINAL:
            code = getattr(
                error,
                "code",
                "migration_cancelled"
                if isinstance(error, asyncio.CancelledError)
                else "migration_restore_prepare_failed",
            )
            interrupted = (
                isinstance(error, asyncio.CancelledError)
                and not current["cancel_requested"]
            )
            store.transition(
                identity,
                "awaiting_credentials"
                if interrupted
                else "cancelled"
                if code == "migration_cancelled"
                else "failed",
                error_code=None if interrupted else code,
                progress={"not_stopped": True, "flow_version": RESTORE_FLOW_VERSION},
            )
        if isinstance(error, MigrationError):
            public_error = error.public()

            def failure(record):
                record.setdefault("failure", public_error)
                return record

            try:
                mutate_json_locked(store.path("jobs", identity), None, failure)
                diagnostic = error.details.get("database_diagnostic")
                if diagnostic:
                    store.record_database_diagnostic(identity, diagnostic)
            except Exception:
                # The stage and original error remain durable if detail writing fails.
                pass
        current = store.read("jobs", identity)
        if (
            current["stage"] in TERMINAL
            and resolved
            and store.path("preflights", identity).exists()
        ):
            from .candidate_database import cleanup_candidate, public_resource

            try:
                await cleanup_candidate(
                    store, identity, resolved, MigrationBudget.start(30)
                )
            except BaseException:
                pass

            def resource(record):
                record["candidate_resource"] = public_resource(store, identity)
                return record

            try:
                mutate_json_locked(store.path("jobs", identity), None, resource)
            except Exception:
                pass
    finally:
        resolved.clear()
        private.clear()


async def resume(application, session, identity, private):
    """Resume only preparation; accepted launcher handoffs retain their recovery API."""
    store = application.store
    try:
        with setup_restore_lock(store):
            recover_unsubmitted(store)
            job = store.read("jobs", identity)
            if (
                not preparation_only(store, job)
                or job["stage"] != "awaiting_credentials"
            ):
                raise MigrationError("migration_recovery_stage_invalid", status=409)
            if (store.path("jobs", identity).parent / "handoff.json").exists():
                raise MigrationError("migration_handoff_unconfirmed", status=409)
            binding = store.private_fingerprint(
                {
                    "upload_id": job["flow"]["upload_id"],
                    "first_deployment": job["options"]["first_deployment"],
                    "private": private,
                    "revision": job["flow"]["revision"],
                }
            )
            if (
                binding != job["flow"]["input_binding"]
                or time.time() >= job["flow"]["deadline_at"]
            ):
                raise MigrationError("migration_restore_preview_changed", status=409)
            job["owner"] = store.owner(session)
            job["flow"].update(
                pid=os.getpid(),
                created_at=_process_created_at(os.getpid()),
                generation=WORKER_GENERATION,
            )
            write_json_locked(store.path("jobs", identity), job)
            upload = store.read("uploads", job["flow"]["upload_id"])
            upload["owner"] = store.owner(session)
            write_json_locked(store.path("uploads", upload["id"]), upload)
            store.transition(identity, "preflight")
            _start(application, session, job, deepcopy(private))
            return store.public(store.read("jobs", identity))
    except Timeout:
        raise MigrationError("migration_operation_in_progress", status=409) from None
