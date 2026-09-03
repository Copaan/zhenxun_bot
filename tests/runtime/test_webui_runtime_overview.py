from __future__ import annotations

import asyncio
import importlib
import json
import sys
from types import SimpleNamespace

import pytest


def _overview_module():
    module_name = "zhenxun.builtin_plugins.web_ui.api.tabs.dashboard.overview"
    if module_name in sys.modules:
        return sys.modules[module_name]
    import nonebot

    app = nonebot.get_app()
    middleware_stack = app.middleware_stack
    app.middleware_stack = None
    try:
        return importlib.import_module(module_name)
    finally:
        app.middleware_stack = middleware_stack


def _service(status: str, code: str, label: str):
    from zhenxun.builtin_plugins.web_ui.api.tabs.dashboard.model import (
        RuntimeServiceStatus,
    )

    return RuntimeServiceStatus(
        status=status,
        code=code,
        label=label,
        detail=f"{label} status",
    )


@pytest.mark.asyncio
async def test_overview_probe_cache_coalesces_concurrent_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    overview = _overview_module()

    await overview.reset_probe_cache_for_tests()
    calls = 0

    async def run_probes():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return _service("ok", "database_ready", "数据库"), _service(
            "ok", "cache_memory_ready", "缓存"
        )

    monkeypatch.setattr(overview, "_run_probes", run_probes)
    results = await asyncio.gather(*(overview._get_probes(False) for _ in range(8)))
    assert calls == 1
    assert all(result[0].code == "database_ready" for result in results)

    await overview._get_probes(False)
    assert calls == 1
    await overview._get_probes(True)
    assert calls == 2
    await overview.reset_probe_cache_for_tests()


@pytest.mark.asyncio
async def test_overview_shared_probe_survives_one_cancelled_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    overview = _overview_module()
    await overview.reset_probe_cache_for_tests()
    started = asyncio.Event()
    release = asyncio.Event()

    async def run_probes():
        started.set()
        await release.wait()
        return _service("ok", "database_ready", "数据库"), _service(
            "ok", "cache_memory_ready", "缓存"
        )

    monkeypatch.setattr(overview, "_run_probes", run_probes)
    first = asyncio.create_task(overview._get_probes(True))
    await started.wait()
    second = asyncio.create_task(overview._get_probes(True))
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    release.set()

    result = await second
    assert result[0].code == "database_ready"
    await overview.reset_probe_cache_for_tests()


@pytest.mark.asyncio
async def test_overview_degrades_individual_services_and_builds_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    overview = _overview_module()

    async def probes(force: bool):
        assert force
        return _service("critical", "database_unavailable", "数据库"), _service(
            "warning", "redis_unavailable", "缓存"
        )

    protocol = SimpleNamespace(
        onebot_v11_connected=False,
        qq_official_enabled=True,
        qq_official_connected=False,
        qq_webhook_mode="external",
        connections=[],
    )
    monkeypatch.setattr(overview, "_get_probes", probes)
    monkeypatch.setattr(overview, "build_protocol_status", lambda: protocol)
    monkeypatch.setattr(overview.setup_access, "state", lambda: "restart_pending")

    result = await overview.build_runtime_overview(force=True)
    assert result.overall_status == "critical"
    assert {issue.code for issue in result.issues} == {
        "database_unavailable",
        "redis_unavailable",
        "protocol_not_connected",
        "restart_pending",
    }
    assert {issue.action_route for issue in result.issues} == {
        "/protocol",
        "/system",
    }
    payload = result.model_dump() if hasattr(result, "model_dump") else result.dict()
    serialized = json.dumps(payload, ensure_ascii=False, default=str).lower()
    assert "secret" not in serialized
    assert "password" not in serialized
    assert "openid" not in serialized


@pytest.mark.asyncio
async def test_overview_reports_connected_adapters_without_remote_bot_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    overview = _overview_module()

    async def probes(force: bool):
        assert not force
        return _service("ok", "database_ready", "数据库"), _service(
            "ok", "cache_memory_ready", "缓存"
        )

    connections = [
        SimpleNamespace(self_id="client", platform="onebot_v11", adapter="OneBot V11"),
        SimpleNamespace(self_id="official", platform="qq_official", adapter="QQ"),
    ]
    protocol = SimpleNamespace(
        onebot_v11_connected=True,
        qq_official_enabled=True,
        qq_official_connected=True,
        qq_webhook_mode="builtin_https",
        connections=connections,
    )
    monkeypatch.setattr(overview, "_get_probes", probes)
    monkeypatch.setattr(overview, "build_protocol_status", lambda: protocol)
    monkeypatch.setattr(overview.setup_access, "state", lambda: "configured")
    now = overview.time.time()
    monkeypatch.setattr(overview, "bot_live", {"client": now - 12, "official": now - 7})

    result = await overview.build_runtime_overview()
    assert result.overall_status == "ok"
    assert result.protocols.connection_count == 2
    assert [bot.connect_seconds for bot in result.bots] == [12, 7]
    assert not result.issues


@pytest.mark.asyncio
async def test_overview_probe_failure_is_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    overview = _overview_module()

    async def broken_database():
        raise RuntimeError("sensitive database detail")

    async def healthy_cache():
        return _service("ok", "cache_memory_ready", "缓存")

    monkeypatch.setattr(overview, "_probe_database", broken_database)
    monkeypatch.setattr(overview, "_probe_cache", healthy_cache)
    database, cache = await overview._run_probes()
    assert database.code == "database_probe_failed"
    assert database.status == "critical"
    assert "sensitive" not in database.detail
    assert cache.code == "cache_memory_ready"


@pytest.mark.asyncio
async def test_overview_protocol_failure_does_not_fail_dashboard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    overview = _overview_module()

    async def probes(force: bool):
        return _service("ok", "database_ready", "数据库"), _service(
            "ok", "cache_disabled", "缓存"
        )

    monkeypatch.setattr(overview, "_get_probes", probes)
    monkeypatch.setattr(
        overview,
        "build_protocol_status",
        lambda: (_ for _ in ()).throw(RuntimeError("provider payload")),
    )
    monkeypatch.setattr(overview.setup_access, "state", lambda: "configured")

    result = await overview.build_runtime_overview()
    assert result.overall_status == "warning"
    assert [issue.code for issue in result.issues] == ["protocol_status_unavailable"]
    assert result.protocols.connection_count == 0


def test_overview_route_requires_authentication() -> None:
    _overview_module()
    from zhenxun.builtin_plugins.web_ui.api.tabs.dashboard import router

    route = next(item for item in router.routes if item.path.endswith("/overview"))
    assert route.dependant.dependencies
