from __future__ import annotations

import multiprocessing
from pathlib import Path
import time

import pytest


def _write_pending_restart(path: str, index: int) -> None:
    from zhenxun.utils import restart_state

    restart_state._RESTART_STATE_FILE = Path(path)

    def update(state: dict) -> None:
        pending = state.setdefault("pending_restarts", {})
        pending[f"worker:{index}"] = {
            "reasons": [f"reason:{index}"],
            "updated_at": float(index),
        }

    restart_state.mutate_restart_state(update)


def test_chat_history_sanitizes_every_string_field() -> None:
    from zhenxun.builtin_plugins.chat_history import chat_message
    from zhenxun.models.chat_history import ChatHistory

    record = ChatHistory(
        user_id="u\x00ser",
        group_id="g\x00roup",
        text="te\x00xt",
        plain_text="pl\x00ain",
        bot_id="b\x00ot",
        platform="q\x00q",
    )
    assert chat_message._sanitize_chat_history(record) == 6
    assert all(
        "\x00" not in getattr(record, field) for field in chat_message._STRING_FIELDS
    )


def test_current_config_yaml_uses_runtime_validation_chain() -> None:
    from zhenxun.builtin_plugins.web_ui.config_validation import validate_simple_yaml

    content = Path("data/config.yaml").read_text(encoding="utf-8")
    validate_simple_yaml(content)


@pytest.mark.asyncio
async def test_launcher_restart_is_delayed_until_response_can_flush(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.utils import _restart_utils, restart_state

    state_file = tmp_path / "restart.json"
    monkeypatch.setattr(restart_state, "_RESTART_STATE_FILE", state_file)
    monkeypatch.setenv("ZHENXUN_LAUNCHER_PID", "123")
    monkeypatch.setattr(_restart_utils, "_restart_pending", False)

    ok, _ = await _restart_utils.request_restart("test")
    assert ok
    state = restart_state.read_restart_state()
    assert state["launcher_not_before"] > time.time()
    assert restart_state.consume_launcher_restart_signal() is False
    state["launcher_not_before"] = 0
    restart_state.write_restart_state(state)
    assert restart_state.consume_launcher_restart_signal() is True


@pytest.mark.asyncio
async def test_duplicate_launcher_restart_reuses_pending_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.utils import _restart_utils, restart_state

    state_file = tmp_path / "restart.json"
    monkeypatch.setattr(restart_state, "_RESTART_STATE_FILE", state_file)
    monkeypatch.setenv("ZHENXUN_LAUNCHER_PID", "123")
    monkeypatch.setattr(_restart_utils, "_restart_pending", False)

    first_ok, _ = await _restart_utils.request_restart("first")
    first_state = restart_state.read_restart_state()
    second_ok, message = await _restart_utils.request_restart("second")

    assert first_ok
    assert second_ok
    assert "继续等待" in message
    assert restart_state.read_restart_state() == first_state
    assert first_state["pending_request"]["source"] == "first"


@pytest.mark.asyncio
async def test_dependency_restart_upgrades_existing_restart_action(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.utils import _restart_utils, restart_state

    state_file = tmp_path / "restart.json"
    monkeypatch.setattr(restart_state, "_RESTART_STATE_FILE", state_file)
    monkeypatch.setenv("ZHENXUN_LAUNCHER_PID", "123")
    monkeypatch.setattr(_restart_utils, "_restart_pending", False)

    first_ok, _ = await _restart_utils.request_restart("settings")
    second_ok, _ = await _restart_utils.request_dependency_restart(
        "dependency-repair", {Path("uv.lock"), Path("pyproject.toml")}
    )

    state = restart_state.read_restart_state()
    assert first_ok
    assert second_ok
    assert state["launcher_action"] == "sync_dependencies_restart"
    assert state["dependency_paths"] == ["pyproject.toml", "uv.lock"]


@pytest.mark.asyncio
async def test_direct_worker_restart_does_not_terminate_itself(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.utils import _restart_utils

    state_file = tmp_path / "restart.json"
    from zhenxun.utils import restart_state

    monkeypatch.setattr(restart_state, "_RESTART_STATE_FILE", state_file)
    monkeypatch.delenv("ZHENXUN_LAUNCHER_PID", raising=False)
    monkeypatch.setattr(_restart_utils, "_restart_pending", False)
    ok, message = await _restart_utils.request_restart("test")
    assert not ok
    assert "手动重启" in message
    assert not state_file.exists()


def test_restart_state_preserves_parallel_process_updates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.utils import restart_state

    state_file = tmp_path / "restart.json"
    context = multiprocessing.get_context("spawn")
    processes = [
        context.Process(target=_write_pending_restart, args=(str(state_file), index))
        for index in range(6)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=15)
        assert process.exitcode == 0

    monkeypatch.setattr(restart_state, "_RESTART_STATE_FILE", state_file)
    state = restart_state.read_restart_state()
    assert sorted(state["pending_restarts"]) == [
        f"worker:{index}" for index in range(6)
    ]
    assert not list(tmp_path.glob("*.tmp"))
