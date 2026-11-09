"""在原实例在线时准备本任务候选，仅在复核通过后请求自动切换。"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import shutil

import psutil

from . import preflight
from . import state as tasks


def phase(request: dict, name: str, **values) -> None:
    tasks.save(
        request,
        preparation={
            **request.get("preparation", {}),
            "phase": name,
            **values,
        },
    )


def claim(request: dict) -> None:
    """同一 launcher 内也只允许一个实际 worker 执行在线准备。"""
    from .restore import _worker_identity

    with tasks.mutation.mutation_guard(request["operation_id"]):
        current = tasks.load(request["operation_id"])
        if current["revision"] != request["revision"]:
            raise RuntimeError("migration_record_changed")
        previous = request.get("preparation_worker")
        worker = _worker_identity()
        if previous and previous != worker:
            try:
                process = psutil.Process(int(previous["pid"]))
                if process.create_time() == previous["created_at"]:
                    raise RuntimeError("migration_preparation_executor_still_active")
            except psutil.NoSuchProcess:
                pass
        tasks.save(request, preparation_worker=worker, previous_worker=worker)


async def blocking(function, *args):
    """取消后仍等待文件操作结束，不把后台线程留给下一次执行。"""
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            await asyncio.shield(task)
        except Exception:
            pass
        raise


def checkpoint(request: dict) -> None:
    if tasks.cancellation_requested(request):
        raise asyncio.CancelledError


def discard_candidate(request: dict) -> None:
    """仅撤销尚未激活的本任务候选，归属不明时保留材料。"""
    from zhenxun.services.nonebot_store import storage

    with (
        tasks.mutation.mutation_guard(request["operation_id"]),
        storage.transaction_lock(),
    ):
        child = storage.pending_transaction()
        if not child:
            if (request.get("dependencies") or {}).get("staged"):
                raise RuntimeError("migration_preparation_child_missing")
            return
        if child.get("parent_operation_id") != request["operation_id"]:
            raise RuntimeError("migration_dependency_owner_changed")
        if (
            not child.get("preparation_only")
            or child.get("orm_write_started")
            or storage.load_manifest().get("active_generation")
            != child.get("base_manifest", {}).get("active_generation")
        ):
            raise RuntimeError("migration_preparation_activation_unconfirmed")
        child.update(state="failed", rollback_confirmed=True)
        storage.save_pending_transaction(child)
        storage.archive_settled()
        tasks.save(request, dependencies=None, prepared_candidate=None)


def _child(request: dict) -> dict | None:
    from zhenxun.services.nonebot_store import storage

    report = request.get("dependencies") or {}
    child = storage.pending_transaction()
    if not report.get("staged"):
        if child and child.get("parent_operation_id") == request["operation_id"]:
            raise RuntimeError("migration_preparation_child_binding_missing")
        return None
    if (
        not child
        or child.get("parent_operation_id") != request["operation_id"]
        or child.get("revision") != report.get("revision")
        or not child.get("preparation_only")
    ):
        raise RuntimeError("migration_preparation_child_changed")
    return child


async def _dependencies(request: dict, package: Path) -> None:
    from zhenxun.services.nonebot_store import runtime, storage

    from . import backup, restore_deps

    phase(request, "dependencies")
    if (request.get("dependencies") or {}).get("skipped_requirements"):
        raise RuntimeError("migration_failed_dependency_scope_unknown")
    if not request.get("dependencies"):
        saved = json.loads((package / backup.DEPENDENCIES).read_text("utf-8"))
        report = await restore_deps.stage_restore(
            saved,
            request["skip_failed"],
            package / "files" if "plugins" in request["categories"] else None,
            parent_operation_id=request["operation_id"],
        )
        tasks.save(request, dependencies=report)
        if report.get("skipped_requirements"):
            raise RuntimeError("migration_failed_dependency_scope_unknown")
        database = json.loads((package / "database/manifest.json").read_text("utf-8"))
        excluded = []
        for key in report.get("skipped_plugins") or []:
            module = (saved.get("plugins", {}).get(key) or {}).get("module_name")
            if not module:
                raise RuntimeError("migration_failed_plugin_scope_unknown:" + key)
            excluded.extend(
                name
                for name, table in database["tables"].items()
                if str(table.get("model", "")).startswith(module + ".")
            )
        tasks.save(request, excluded_tables=sorted(set(excluded)))
    checkpoint(request)
    child = _child(request)
    if child is None:
        return
    phase(request, "building")
    build = await blocking(runtime._staged_build, child)
    if build is None:
        from zhenxun.services.runtime_mutation import runtime_mutation_coordinator

        async with runtime_mutation_coordinator.operation(
            "migration_preparation", owner=request["operation_id"]
        ):
            build = await runtime.prepare_generation(
                child, cancel_requested=lambda: tasks.cancellation_requested(request)
            )
        with (
            tasks.mutation.mutation_guard(request["operation_id"]),
            storage.transaction_lock(),
        ):
            current = _child(request)
            if current.get("record_revision") != child.get("record_revision"):
                storage.remove_generation(build["generation"])
                raise RuntimeError("migration_preparation_child_changed")
            runtime.stage_generation(current, build)
            child = _child(request)
    tasks.save(
        request,
        prepared_candidate={
            "revision": child["revision"],
            "generation": build["generation"],
            "digest": build["digest"],
        },
    )


def authorize(request: dict) -> None:
    """旧 worker 退出后将已核验候选授权给本次切换，禁止重新联网构建。"""
    from zhenxun.services.nonebot_store import runtime, storage

    with (
        tasks.mutation.mutation_guard(request["operation_id"]),
        storage.transaction_lock(),
    ):
        child = _child(request)
        if child is None:
            return
        build = runtime._staged_build(child)
        proof = request.get("prepared_candidate") or {}
        if not build or any(
            proof.get(key) != build.get(key) for key in ("generation", "digest")
        ):
            raise RuntimeError("migration_prepared_candidate_changed")
        child.update(
            preparation_only=False, prepared_for_migration=True, state="pending_restart"
        )
        storage.save_pending_transaction(child)


def hold(request: dict) -> None:
    """未写入原实例时收回候选激活权限，保留同任务准备材料。"""
    from zhenxun.services.nonebot_store import storage

    report = request.get("dependencies") or {}
    if not report.get("staged"):
        return
    with (
        tasks.mutation.mutation_guard(request["operation_id"]),
        storage.transaction_lock(),
    ):
        child = storage.pending_transaction()
        if (
            not child
            or child.get("parent_operation_id") != request["operation_id"]
            or child.get("revision") != report.get("revision")
            or child.get("orm_write_started")
            or storage.load_manifest().get("active_generation")
            != child.get("base_manifest", {}).get("active_generation")
        ):
            raise RuntimeError("migration_preparation_activation_unconfirmed")
        child.update(
            preparation_only=True, prepared_for_migration=False, state="preparing"
        )
        storage.save_pending_transaction(child)


async def run(
    request: dict,
    password: str | None,
    restart,
    confirmation: str | None = None,
    refresh_attempts: int = 0,
):
    """准备失败保留同任务重试材料，通过复核才进入停写窗口。"""
    from . import backup, restore

    claim(request)
    tasks.save(request, restart_info=None)
    phase(
        request,
        "checking",
        error=None,
        retryable=False,
        confirmation_required=False,
        password_required=False,
    )
    package, _ = restore._paths(request)
    try:
        checkpoint(request)
        if request["kind"] == "restore":
            source = Path(request["archive_path"])
            receipt = request["archive_stat"]
            stat = source.stat()
            if [stat.st_size, stat.st_mtime_ns] != receipt:
                raise RuntimeError("migration_source_package_changed")
            fingerprint = await blocking(preflight.file_digest, source)
            if request.get("archive_digest") not in {None, fingerprint}:
                raise RuntimeError("migration_source_package_changed")
            if (
                not request.get("package_digest")
                and not password
                and await blocking(backup.is_encrypted, source)
            ):
                phase(
                    request,
                    "waiting_credentials",
                    password_required=True,
                    retryable=True,
                    error="请补充原备份包密码以继续准备，原实例未变更",
                )
                return
            await blocking(preflight.archive_check, source, password)
            if not request.get("package_digest"):
                if package.exists():
                    await blocking(shutil.rmtree, package)
                manifest = await blocking(backup.extract, source, package, password)
                if set(request["categories"]) - set(manifest["categories"]):
                    raise RuntimeError("migration_package_categories_changed")
                if fingerprint != await blocking(preflight.file_digest, source):
                    raise RuntimeError("migration_source_package_changed")
                await blocking(preflight.validate_data, package / "database")
                tasks.save(
                    request,
                    archive_digest=fingerprint,
                    package_digest=await blocking(preflight.tree_digest, package),
                    unrestorable_files=manifest["unrestorable_files"],
                )
            elif request["package_digest"] != await blocking(
                preflight.tree_digest, package
            ):
                raise RuntimeError("migration_prepared_package_changed")
        checkpoint(request)
        baseline = preflight.environment()
        if request.get("preparation_environment") not in {None, baseline}:
            discard_candidate(request)
        tasks.save(request, preparation_environment=baseline)
        checked = await preflight.database_check(
            request, package if request["kind"] == "restore" else None
        )
        if request["kind"] == "switch":
            previous = await preflight.database_check(
                {
                    **request,
                    "kind": "restore",
                    "db_url": request["previous_db_url"],
                    "snapshot_required": True,
                }
            )
            if previous["identity"] == checked["identity"]:
                raise RuntimeError("migration_same_database_identity")
        tasks.save(
            request, target_identity=checked["identity"], preflight_database=checked
        )
        file_targets = await blocking(preflight.files_check, request, package)
        if request["kind"] == "restore":
            await _dependencies(request, package)
            phase(request, "orm_check")
            orm = await preflight.orm_check(request, package)
            tasks.save(request, orm_preflight=orm)
        checkpoint(request)
        report = request.get("dependencies") or {}
        impact = {
            name: report.get(name)
            for name in (
                "nearby",
                "skipped_plugins",
                "platform_only",
                "unsupported_requirements",
            )
        }
        impact["files"] = sorted(file_targets)
        impact["structure"] = checked["structure"]
        impact_digest = preflight.digest(impact)
        previous_impact = request.get("confirmed_impact")
        changed = bool(report.get("nearby") or report.get("skipped_plugins"))
        if previous_impact and previous_impact != impact_digest:
            changed = True
        if (
            changed
            and confirmation != impact_digest
            and previous_impact != impact_digest
        ):
            phase(
                request,
                "waiting_confirmation",
                confirmation_required=True,
                impact_digest=impact_digest,
                changes=impact,
                retryable=True,
            )
            return
        tasks.save(
            request, confirmed_impact=impact_digest, prepared_target_files=file_targets
        )
        phase(request, "rechecking", checks=checked, confirmation_required=False)
        refreshed = await preflight.database_check(
            request, package if request["kind"] == "restore" else None
        )
        if (
            refreshed["identity"] != checked["identity"]
            or refreshed["structure"] != checked["structure"]
        ):
            raise RuntimeError("migration_preflight_database_changed")
        if file_targets != await blocking(preflight.files_check, request, package):
            raise RuntimeError("migration_preflight_files_changed")
        if preflight.environment() != baseline:
            raise RuntimeError("migration_preflight_environment_changed")
        if request["kind"] == "restore":
            await preflight.orm_check(request, package)
        if request["kind"] == "restore" and request["package_digest"] != await blocking(
            preflight.tree_digest, package
        ):
            raise RuntimeError("migration_prepared_package_changed")
        checkpoint(request)
        tasks.save(request, preflight_ready=True, original_modified=False)
        phase(request, "switching", retryable=False)
        await restore._submit(request, restart)
    except asyncio.CancelledError:
        if tasks.cancellation_requested(request):
            await cancel(request)
        else:
            phase(
                request,
                "interrupted",
                retryable=True,
                error="准备已暂停，原实例未变更；重启后可继续原任务",
            )
        raise
    except Exception as error:
        if request["state"] == "preparing":
            if (
                str(error)
                in {
                    "migration_preflight_environment_changed",
                    "migration_preflight_database_changed",
                    "migration_preflight_files_changed",
                }
                and refresh_attempts < 1
            ):
                return await run(
                    request, password, restart, confirmation, refresh_attempts + 1
                )
            phase(request, "blocked", retryable=True, error=restore._message(error))
        else:
            raise
    finally:
        if request["state"] == "preparing":
            tasks.save(request, preparation_worker=None)


async def cancel(request: dict) -> None:
    """执行器退出后清理本任务候选，不触发原实例或数据库回退。"""
    from . import restore

    try:
        discard_candidate(request)
        await blocking(restore.cleanup, request)
    except (OSError, RuntimeError) as error:
        phase(
            request, "cleanup_pending", error=restore._message(error), retryable=False
        )
        tasks.save(request, cleanup_error=restore._message(error))
        return
    tasks.finish(
        request, state="canceled", original_modified=False, preparation_worker=None
    )
