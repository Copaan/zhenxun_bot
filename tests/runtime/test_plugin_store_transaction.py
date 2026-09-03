from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def source_transaction(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from zhenxun import plugin_store_transaction as transaction

    monkeypatch.chdir(tmp_path)
    root = tmp_path / "data" / "runtime" / "plugin-store-transaction"
    monkeypatch.setattr(transaction, "ROOT", root)
    monkeypatch.setattr(transaction, "PENDING_FILE", root / "pending-v1.json")
    monkeypatch.setattr(transaction, "_TRANSACTION_LOCK", root / ".transaction.lock")
    monkeypatch.setattr(
        transaction,
        "LIFECYCLE_INDEX",
        tmp_path / "data" / "runtime" / "lifecycle-index-v2.json",
    )
    return transaction


def _plugin(tmp_path: Path, name: str, value: str) -> Path:
    path = tmp_path / "zhenxun" / "plugins" / f"{name}.py"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")
    return path


def test_staged_update_keeps_live_source_until_launcher_apply(
    tmp_path: Path, source_transaction
) -> None:
    live = _plugin(tmp_path, "demo", "VALUE = 1\n")
    candidate = tmp_path / "candidate.py"
    candidate.write_text("VALUE = 2\n", encoding="utf-8")

    result = source_transaction.stage_operation(
        action="update",
        store_key="official:demo",
        module="demo",
        runtime_module="zhenxun.plugins.demo",
        live_path=live,
        candidate_path=candidate,
        receipt=None,
        reason="plugin_restart_required",
    )

    assert result["apply_mode"] == "restart_pending"
    assert live.read_text(encoding="utf-8") == "VALUE = 1\n"
    assert source_transaction.apply_pending_transaction()
    assert live.read_text(encoding="utf-8") == "VALUE = 2\n"


def test_startup_verification_reads_current_runtime_index(
    tmp_path: Path, source_transaction
) -> None:
    live = _plugin(tmp_path, "demo", "VALUE = 1\n")
    source_transaction.stage_operation(
        action="install",
        store_key="official:demo",
        module="demo",
        runtime_module="zhenxun.plugins.demo",
        live_path=live,
        candidate_path=live,
        base_source_path=tmp_path / "missing",
        base_digest="missing",
        receipt=None,
        reason="orm_model_new_plugin",
    )
    live.unlink()
    assert source_transaction.apply_pending_transaction()
    source_transaction.LIFECYCLE_INDEX.parent.mkdir(parents=True, exist_ok=True)
    source_transaction.LIFECYCLE_INDEX.write_text(
        '{"version":2,"plugins":['
        '{"plugin_id":"demo","module":"zhenxun.plugins.demo"}]}',
        encoding="utf-8",
    )

    verified, detail = source_transaction.startup_verification()

    assert verified
    assert detail == {"failed": []}


def test_partial_source_switch_failure_restores_every_live_path(
    tmp_path: Path, source_transaction, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = _plugin(tmp_path, "first", "VALUE = 1\n")
    second = _plugin(tmp_path, "second", "VALUE = 1\n")
    first_candidate = tmp_path / "first-new.py"
    second_candidate = tmp_path / "second-new.py"
    first_candidate.write_text("VALUE = 2\n", encoding="utf-8")
    second_candidate.write_text("VALUE = 2\n", encoding="utf-8")
    for name, live, candidate in (
        ("first", first, first_candidate),
        ("second", second, second_candidate),
    ):
        source_transaction.stage_operation(
            action="update",
            store_key=f"official:{name}",
            module=name,
            runtime_module=f"zhenxun.plugins.{name}",
            live_path=live,
            candidate_path=candidate,
            receipt=None,
            reason="plugin_restart_required",
        )

    original_copy = source_transaction._copy
    failed_once = False

    def fail_second_candidate(source: Path, target: Path) -> None:
        nonlocal failed_once
        if target.resolve() == second and source.name == "new" and not failed_once:
            failed_once = True
            raise OSError("locked")
        original_copy(source, target)

    monkeypatch.setattr(source_transaction, "_copy", fail_second_candidate)

    assert not source_transaction.apply_pending_transaction()
    assert first.read_text(encoding="utf-8") == "VALUE = 1\n"
    assert second.read_text(encoding="utf-8") == "VALUE = 1\n"
    pending = source_transaction.pending_transaction()
    assert pending
    assert pending["state"] == "failed"


def test_install_then_uninstall_collapses_to_no_pending_change(
    tmp_path: Path, source_transaction
) -> None:
    candidate = _plugin(tmp_path, "demo", "VALUE = 1\n")
    result = source_transaction.stage_operation(
        action="install",
        store_key="official:demo",
        module="demo",
        runtime_module="zhenxun.plugins.demo",
        live_path=candidate,
        candidate_path=candidate,
        base_source_path=tmp_path / "missing",
        base_digest="missing",
        receipt=None,
        reason="plugin_restart_required",
    )
    collapsed = source_transaction.stage_operation(
        action="uninstall",
        store_key="official:demo",
        module="demo",
        runtime_module="zhenxun.plugins.demo",
        live_path=candidate,
        candidate_path=None,
        receipt=None,
        reason="plugin_restart_required",
    )

    assert result["operation_id"] in collapsed["removed_operation_ids"]
    assert collapsed["apply_mode"] == "rolled_back"
    assert source_transaction.pending_transaction() is None


def test_external_source_change_makes_pending_transaction_stale(
    tmp_path: Path, source_transaction
) -> None:
    live = _plugin(tmp_path, "demo", "VALUE = 1\n")
    candidate = tmp_path / "candidate.py"
    candidate.write_text("VALUE = 2\n", encoding="utf-8")
    source_transaction.stage_operation(
        action="update",
        store_key="official:demo",
        module="demo",
        runtime_module="zhenxun.plugins.demo",
        live_path=live,
        candidate_path=candidate,
        receipt=None,
        reason="plugin_restart_required",
    )
    live.write_text("VALUE = 3\n", encoding="utf-8")

    assert not source_transaction.apply_pending_transaction()
    assert live.read_text(encoding="utf-8") == "VALUE = 3\n"
    pending = source_transaction.pending_transaction()
    assert pending
    assert pending["failure_reasons"] == [{"code": "plugin_transaction_stale"}]


def test_uninstall_allows_plugin_runtime_files_to_change(
    tmp_path: Path, source_transaction
) -> None:
    live = tmp_path / "demo"
    live.mkdir()
    (live / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    runtime_database = live / "runtime.db"
    runtime_database.write_bytes(b"before")
    source_transaction.stage_operation(
        action="uninstall",
        store_key="official:demo",
        module="demo",
        runtime_module="zhenxun.plugins.demo",
        live_path=live,
        candidate_path=None,
        receipt=None,
        reason="plugin_restart_required",
    )
    runtime_database.write_bytes(b"changed while plugin is running")

    assert source_transaction.apply_pending_transaction()
    assert not live.exists()
    pending = source_transaction.pending_transaction()
    assert pending
    assert pending["state"] == "verification_pending"


def test_source_dependencies_join_managed_nonebot_generation(
    tmp_path: Path, source_transaction, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.nonebot_store import dependencies, storage

    store_root = tmp_path / "data" / "runtime" / "nonebot-store"
    monkeypatch.setattr(storage, "STORE_ROOT", store_root)
    monkeypatch.setattr(storage, "MANIFEST_FILE", store_root / "manifest-v1.json")
    monkeypatch.setattr(
        storage, "PENDING_FILE", store_root / "pending-transaction-v1.json"
    )
    monkeypatch.setattr(dependencies, "protected_core", lambda: {})
    storage.save_manifest(storage.default_manifest())
    candidate = _plugin(tmp_path, "demo", "VALUE = 1\n")
    source_transaction.stage_operation(
        action="install",
        store_key="official:demo",
        module="demo",
        runtime_module="zhenxun.plugins.demo",
        live_path=candidate,
        candidate_path=candidate,
        base_source_path=tmp_path / "missing",
        base_digest="missing",
        receipt=None,
        reason="plugin_dependencies_changed",
        dependency_packages={"humanize": "4.16.0"},
    )

    assert source_transaction.prepare_dependency_transaction()
    pending = storage.pending_transaction()
    assert pending
    assert pending["target_manifest"]["packages"]["humanize"] == {"version": "4.16.0"}
    assert pending["source_transaction_revision"]


def test_source_build_confirmation_joins_managed_generation(
    tmp_path: Path, source_transaction, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.nonebot_store import dependencies, storage

    store_root = tmp_path / "data" / "runtime" / "nonebot-store"
    monkeypatch.setattr(storage, "STORE_ROOT", store_root)
    monkeypatch.setattr(storage, "MANIFEST_FILE", store_root / "manifest-v1.json")
    monkeypatch.setattr(
        storage, "PENDING_FILE", store_root / "pending-transaction-v1.json"
    )
    monkeypatch.setattr(dependencies, "protected_core", lambda: {})
    storage.save_manifest(storage.default_manifest())
    candidate = _plugin(tmp_path, "demo", "VALUE = 1\n")
    source_transaction.stage_operation(
        action="install",
        store_key="official:demo",
        module="demo",
        runtime_module="zhenxun.plugins.demo",
        live_path=candidate,
        candidate_path=candidate,
        base_source_path=tmp_path / "missing",
        base_digest="missing",
        receipt=None,
        reason="plugin_dependencies_changed",
        dependency_packages={"sdist-only-helper": "1.0.0"},
        source_build_confirmed=True,
    )

    assert source_transaction.prepare_dependency_transaction()
    pending = storage.pending_transaction()
    assert pending
    assert pending["source_build_confirmed"] is True


def test_failed_source_transaction_does_not_rebuild_dependency_layer(
    tmp_path: Path, source_transaction, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.nonebot_store import storage

    candidate = _plugin(tmp_path, "demo", "VALUE = 1\n")
    source_transaction.stage_operation(
        action="install",
        store_key="official:demo",
        module="demo",
        runtime_module="zhenxun.plugins.demo",
        live_path=candidate,
        candidate_path=candidate,
        base_source_path=tmp_path / "missing",
        base_digest="missing",
        receipt=None,
        reason="plugin_dependencies_changed",
        dependency_packages={"humanize": "4.16.0"},
    )
    pending = source_transaction.pending_transaction()
    assert pending
    pending["state"] = "failed"
    source_transaction._write_pending(pending)
    save_calls = []
    monkeypatch.setattr(storage, "save_pending_transaction", save_calls.append)

    assert source_transaction.prepare_dependency_transaction()
    assert save_calls == []


@pytest.mark.asyncio
async def test_shared_store_coordinator_is_reentrant_and_rejects_parallel() -> None:
    from zhenxun.plugin_store_coordinator import (
        StoreOperationBusyError,
        plugin_store_operation_coordinator,
    )

    entered = asyncio.Event()
    release = asyncio.Event()

    async def owner() -> None:
        async with plugin_store_operation_coordinator.operation():
            async with plugin_store_operation_coordinator.operation():
                entered.set()
                await release.wait()

    task = asyncio.create_task(owner())
    await entered.wait()
    with pytest.raises(StoreOperationBusyError, match="plugin_operation_in_progress"):
        async with plugin_store_operation_coordinator.operation():
            pass
    release.set()
    await task


def test_restart_status_counts_nonebot_operations_not_unique_reasons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from zhenxun import plugin_store_transaction
    from zhenxun.builtin_plugins.web_ui import restart_service
    from zhenxun.nonebot_store import storage

    monkeypatch.setattr(restart_service, "_current_access_urls", lambda: [])
    monkeypatch.setattr(
        restart_service,
        "load_webui_tls_settings",
        lambda: SimpleNamespace(
            scheme="http", enabled=False, redirect_enabled=False, redirect_port=0
        ),
    )
    monkeypatch.setattr(
        restart_service,
        "get_pending_restart_reasons",
        lambda: ["plugin_restart_required"],
    )
    monkeypatch.setattr(
        restart_service,
        "get_pending_restart_items",
        lambda: [
            {
                "source": "webui.nonebot-store",
                "reasons": ["plugin_restart_required"],
                "updated_at": 1,
            }
        ],
    )
    monkeypatch.setattr(
        storage,
        "pending_transaction",
        lambda: {
            "state": "pending_restart",
            "operations": [
                {
                    "operation_id": "one",
                    "project_link": "plugin-one",
                    "action": "install",
                },
                {
                    "operation_id": "two",
                    "project_link": "plugin-two",
                    "action": "install",
                },
            ],
        },
    )
    monkeypatch.setattr(plugin_store_transaction, "public_transaction", lambda: None)

    status = restart_service.restart_status_data()

    assert status["pending_count"] == 2
    assert {item["operation_id"] for item in status["pending_items"]} == {
        "one",
        "two",
    }


def test_transaction_verification_status_reports_both_stores(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from zhenxun import plugin_store_transaction
    from zhenxun.builtin_plugins.web_ui import restart_service
    from zhenxun.nonebot_store import storage

    monkeypatch.setattr(
        storage, "load_manifest", lambda: {"pending_verification": True}
    )
    monkeypatch.setattr(
        plugin_store_transaction,
        "pending_transaction",
        lambda: {"state": "verification_pending"},
    )

    status = restart_service.transaction_verification_status()

    assert status == {
        "transaction_verification_pending": True,
        "transaction_verification_sources": ["nonebot_store", "zhenxun_store"],
    }
