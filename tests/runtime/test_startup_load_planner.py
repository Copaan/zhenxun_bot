from __future__ import annotations

import asyncio
from pathlib import Path
import time

import pytest


def _record(tmp_path: Path, source: str) -> dict:
    from zhenxun.services.startup_load import _file_record

    path = tmp_path / "plugin.py"
    path.write_text(source, encoding="utf-8")
    return _file_record(path, "demo.plugin", None)


def test_static_scan_distinguishes_pydantic_and_orm_models(tmp_path: Path) -> None:
    pydantic = _record(
        tmp_path,
        "from pydantic import BaseModel\n"
        "class Config(BaseModel):\n"
        "    value: int = 1\n",
    )
    orm = _record(
        tmp_path,
        "from tortoise.models import Model as DbModel\nclass Row(DbModel):\n    pass\n",
    )

    assert "orm_model" not in pydantic["reasons"]
    assert "orm_model" in orm["reasons"]


def test_static_scan_only_flags_import_time_calls(tmp_path: Path) -> None:
    deferred = _record(
        tmp_path,
        "def install(app):\n    app.include_router(object())\n",
    )
    import_time = _record(tmp_path, "app.mount('/assets', object())\n")

    assert "fastapi_route" not in deferred["reasons"]
    assert "fastapi_route" in import_time["reasons"]


def test_static_scan_detects_nonebot_orm_requirement(tmp_path: Path) -> None:
    record = _record(
        tmp_path,
        "from nonebot import require\nrequire('nonebot_plugin_orm')\n",
    )

    assert "nonebot_plugin_orm" in record["reasons"]
    assert record["requires"] == ["nonebot_plugin_orm"]


def test_topological_order_is_stable_and_promotes_dependencies(tmp_path: Path) -> None:
    from zhenxun.services.startup_load import PlannedPlugin, StartupLoadPlanner

    def entry(plugin_id: str) -> PlannedPlugin:
        return PlannedPlugin(
            source="source",
            plugin_id=plugin_id,
            module_name=f"demo.{plugin_id}",
            root=tmp_path / plugin_id,
            manager=None,
        )

    planner = StartupLoadPlanner()
    planner.entries = {name: entry(name) for name in ("c", "a", "b")}
    planner.entries["c"].phase = "critical_preload"
    planner.entries["c"].dependencies = {"b"}
    planner.entries["b"].dependencies = {"a"}

    planner._promote_critical_dependencies()

    assert planner._topological_order() == ["a", "b", "c"]
    assert all(item.phase == "critical_preload" for item in planner.entries.values())


def test_management_ready_requires_bound_listener(monkeypatch) -> None:
    from zhenxun.services.startup import StartupCoordinator

    coordinator = StartupCoordinator()
    monkeypatch.setattr(coordinator, "persist", lambda: None)

    coordinator.begin_stage("management")
    coordinator.finish_stage("management")
    assert coordinator.state == "starting"

    coordinator.mark_server_bound()
    assert coordinator.state == "management_ready"


@pytest.mark.asyncio
async def test_late_readiness_waiters_do_not_block(monkeypatch) -> None:
    from zhenxun.services.startup import StartupCoordinator

    coordinator = StartupCoordinator()
    monkeypatch.setattr(coordinator, "persist", lambda: None)

    coordinator.mark_server_bound()
    coordinator.begin_stage("runtime")
    coordinator.finish_stage("runtime")

    await asyncio.wait_for(coordinator.wait_server_bound(), timeout=0.1)
    await asyncio.wait_for(coordinator.wait_runtime_ready(), timeout=0.1)


def test_static_scan_reuses_unchanged_file_record(tmp_path: Path) -> None:
    from zhenxun.services.startup_load import _file_record

    path = tmp_path / "plugin.py"
    path.write_text("VALUE = 1\n", encoding="utf-8")
    first = _file_record(path, "demo.plugin", None)

    second = _file_record(path, "demo.plugin", first)
    assert second == first

    time.sleep(0.002)
    path.write_text("VALUE = 2\n", encoding="utf-8")
    third = _file_record(path, "demo.plugin", second)
    assert third["digest"] != first["digest"]
