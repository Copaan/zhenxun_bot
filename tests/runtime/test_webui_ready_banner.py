from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
import socket
from types import ModuleType
from typing import Any

import pytest


def _load_ready_banner() -> ModuleType:
    path = Path("zhenxun/builtin_plugins/web_ui/ready_banner.py").resolve()
    spec = importlib.util.spec_from_file_location("_webui_ready_banner_test", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _new_ready_banner() -> Any:
    return _load_ready_banner().UvicornReadyBanner()


def _unused_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _record(calls: list[str], event: asyncio.Event, value: str) -> None:
    calls.append(value)
    event.set()


@pytest.mark.asyncio
async def test_banner_runs_only_after_tcp_listener_is_ready() -> None:
    ready = _new_ready_banner()
    calls: list[str] = []
    event = asyncio.Event()
    port = _unused_port()

    assert ready.arm(
        lambda: _record(calls, event, "ready"),
        host="0.0.0.0",
        port=port,
        timeout=2,
    )
    await asyncio.sleep(0.1)
    assert calls == []

    server = await asyncio.start_server(lambda _r, w: w.close(), "127.0.0.1", port)
    try:
        await asyncio.wait_for(event.wait(), timeout=2)
    finally:
        server.close()
        await server.wait_closed()

    assert calls == ["ready"]


@pytest.mark.asyncio
async def test_banner_is_emitted_once() -> None:
    ready = _new_ready_banner()
    calls: list[str] = []
    event = asyncio.Event()
    server = await asyncio.start_server(lambda _r, w: w.close(), "127.0.0.1", 0)
    port = int(server.sockets[0].getsockname()[1])
    try:
        assert ready.arm(
            lambda: _record(calls, event, "ready"), host="127.0.0.1", port=port
        )
        assert not ready.arm(
            lambda: calls.append("duplicate"), host="127.0.0.1", port=port
        )
        await asyncio.wait_for(event.wait(), timeout=2)
        assert calls == ["ready"]
        assert not ready.arm(lambda: calls.append("late"), host="127.0.0.1", port=port)
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_reset_cancels_probe_and_allows_next_startup() -> None:
    ready = _new_ready_banner()
    calls: list[str] = []
    event = asyncio.Event()
    port = _unused_port()
    assert ready.arm(lambda: calls.append("cancelled"), host="127.0.0.1", port=port)
    ready.reset()
    await asyncio.sleep(0)

    server = await asyncio.start_server(lambda _r, w: w.close(), "127.0.0.1", port)
    try:
        await asyncio.sleep(0.1)
        assert calls == []
        assert ready.arm(
            lambda: _record(calls, event, "next"), host="127.0.0.1", port=port
        )
        await asyncio.wait_for(event.wait(), timeout=2)
    finally:
        server.close()
        await server.wait_closed()
    assert calls == ["next"]


@pytest.mark.asyncio
async def test_probe_timeout_does_not_emit_and_can_be_rearmed() -> None:
    ready = _new_ready_banner()
    calls: list[str] = []
    port = _unused_port()
    assert ready.arm(
        lambda: calls.append("ready"),
        host="127.0.0.1",
        port=port,
        timeout=0.05,
    )
    await asyncio.sleep(0.15)
    assert calls == []
    assert ready.arm(
        lambda: calls.append("next"), host="127.0.0.1", port=port, timeout=0.05
    )
    ready.reset()


@pytest.mark.asyncio
async def test_banner_awaits_async_callback_and_reset_cancels_it() -> None:
    ready = _new_ready_banner()
    callback_started = asyncio.Event()
    callback_finished = asyncio.Event()
    release = asyncio.Event()
    server = await asyncio.start_server(lambda _r, w: w.close(), "127.0.0.1", 0)
    port = int(server.sockets[0].getsockname()[1])

    async def callback() -> None:
        callback_started.set()
        await release.wait()
        callback_finished.set()

    try:
        assert ready.arm(callback, host="127.0.0.1", port=port)
        await asyncio.wait_for(callback_started.wait(), timeout=2)
        assert not callback_finished.is_set()

        ready.reset()
        release.set()
        await asyncio.sleep(0)
        assert not callback_finished.is_set()
    finally:
        server.close()
        await server.wait_closed()
