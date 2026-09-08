import asyncio
from contextvars import Context
import os
from pathlib import Path
import time
from typing import Any
from uuid import uuid4

import nonebot
from nonebot.adapters import Bot

from zhenxun.services.lifecycle import LifecycleContext
from zhenxun.services.lifecycle.deadline import remaining_timeout
from zhenxun.services.log import logger
from zhenxun.services.startup import startup_coordinator
from zhenxun.utils import restart_state as _restart_state_module
from zhenxun.utils.manager.priority_manager import PriorityLifecycle
from zhenxun.utils.restart_state import (
    _ACTION_RESTART,
    _ACTION_SYNC_DEPENDENCIES,
    _DEPENDENCY_PATHS_KEY,
    _LAUNCHER_ACTION_KEY,
    _LAUNCHER_NOT_BEFORE_KEY,
    _PENDING_RESTARTS_KEY,
    clear_restart_tickets,
    consume_restart_ticket,
    mutate_restart_state,
    read_restart_state,
)
from zhenxun.utils.restart_state import (
    issue_restart_ticket as _issue_restart_ticket,
)

_LEGACY_RESTART_MARK = Path() / "is_restart"
_LEGACY_RESTART_SCRIPT = Path() / "restart.sh"
_LEGACY_CONFIGURE_RESTART_PREFIX = ".configure_restart"
_PENDING_REQUEST_KEY = "pending_request"

_restart_pending: bool = False
_receipt_lock = asyncio.Lock()
_RECEIPT_COMPONENT = "runtime:restart_receipts"
_RECEIPT_ATTEMPTS = (0, 1, 4, 14, 44)
_RECEIPT_TIMEOUT = 5
_RECEIPT_CANCEL_GRACE = 1
_receipt_context: LifecycleContext | None = None
_receipt_jobs: dict[str, tuple[Bot, asyncio.Task]] = {}
_receipt_sends: dict[str, tuple[Bot, asyncio.Task]] = {}
_receipt_replacements: dict[str, Bot] = {}
_receipt_revoked: set[asyncio.Task] = set()
# Compatibility for callers that patched both legacy modules in tests.
_RESTART_STATE_FILE = _restart_state_module._RESTART_STATE_FILE


def _cleanup_legacy_restart_artifacts() -> None:
    legacy_paths = [_LEGACY_RESTART_MARK, _LEGACY_RESTART_SCRIPT]
    legacy_paths.extend(Path().glob(f"{_LEGACY_CONFIGURE_RESTART_PREFIX}*"))
    for path in legacy_paths:
        if not path.exists():
            continue
        try:
            path.unlink()
            logger.info(f"已清理旧重启遗留文件: {path.name}", "重启")
        except Exception as e:
            logger.warning(f"清理旧重启遗留文件失败: {path.name} | {e}", "重启")


def issue_restart_ticket(source: str, *, ttl_seconds: int = 600) -> None:
    _issue_restart_ticket(source, ttl_seconds=ttl_seconds)
    logger.info(f"已记录重启授权，来源: {source}", "重启")


def mark_restart_pending(source: str, reasons: set[str] | list[str]) -> None:
    normalized = sorted(
        {str(reason).strip() for reason in reasons if str(reason).strip()}
    )
    if not normalized:
        clear_restart_pending(source)
        return

    def update(state: dict[str, Any]) -> None:
        pending = state.get(_PENDING_RESTARTS_KEY)
        if not isinstance(pending, dict):
            pending = {}
        pending[source] = {
            "reasons": normalized,
            "updated_at": time.time(),
        }
        state[_PENDING_RESTARTS_KEY] = pending

    mutate_restart_state(update)


def clear_restart_pending(source: str | None = None) -> None:
    def clear(state: dict[str, Any]) -> None:
        if source is None:
            state.pop(_PENDING_RESTARTS_KEY, None)
            return
        pending = state.get(_PENDING_RESTARTS_KEY)
        if not isinstance(pending, dict):
            return
        pending.pop(source, None)
        if pending:
            state[_PENDING_RESTARTS_KEY] = pending
        else:
            state.pop(_PENDING_RESTARTS_KEY, None)

    mutate_restart_state(clear)


def clear_restart_ticket_if_idle() -> None:
    def clear(state: dict[str, Any]) -> None:
        pending = state.get(_PENDING_RESTARTS_KEY)
        if isinstance(pending, dict) and pending:
            return
        clear_restart_tickets(state)

    mutate_restart_state(clear)


def get_pending_restart_reasons() -> list[str]:
    state = read_restart_state()
    pending = state.get(_PENDING_RESTARTS_KEY)
    if not isinstance(pending, dict):
        return []
    reasons: set[str] = set()
    for item in pending.values():
        if not isinstance(item, dict):
            continue
        values = item.get("reasons")
        if isinstance(values, list):
            reasons.update(str(value) for value in values if str(value).strip())
    return sorted(reasons)


def get_pending_restart_items() -> list[dict[str, Any]]:
    state = read_restart_state()
    pending = state.get(_PENDING_RESTARTS_KEY)
    if not isinstance(pending, dict):
        return []
    result: list[dict[str, Any]] = []
    for source, item in sorted(pending.items()):
        if not isinstance(item, dict):
            continue
        reasons = item.get("reasons")
        if not isinstance(reasons, list):
            reasons = []
        result.append(
            {
                "source": str(source),
                "reasons": sorted(
                    {str(reason) for reason in reasons if str(reason).strip()}
                ),
                "updated_at": float(item.get("updated_at") or 0),
            }
        )
    return result


async def _schedule_restart() -> tuple[bool, str]:
    global _restart_pending
    if _restart_pending:
        logger.info("重启已在进行中，复用当前重启请求。", "重启")
        return True, "重启已在进行中，正在继续等待当前重启完成。"
    _restart_pending = True
    logger.info("已标记重启请求，等待 launcher 接管下一代 worker...", "重启")
    return True, "重启请求已提交"


async def request_restart(
    source: str,
    *,
    receipt_bot_id: str | None = None,
    receipt_user_id: str | None = None,
    receipt_group_id: str | None = None,
    receipt_channel_id: str | None = None,
    receipt_platform_scope: str | None = None,
    require_ticket: str | None = None,
) -> tuple[bool, str]:
    if not os.getenv("ZHENXUN_LAUNCHER_PID"):
        return False, "当前不是 launcher 托管模式，请手动重启真寻。"
    if _restart_pending:
        return await _schedule_restart()
    try:

        def update(state: dict[str, Any]) -> tuple[str, str]:
            if state.get(_LAUNCHER_ACTION_KEY) in {
                _ACTION_RESTART,
                _ACTION_SYNC_DEPENDENCIES,
            }:
                return "duplicate", ""
            if require_ticket:
                ok, message = consume_restart_ticket(state, require_ticket)
                if not ok:
                    return "rejected", message
            pending_request: dict[str, Any] = {
                "request_id": uuid4().hex,
                "source": source,
                "requested_at": time.time(),
            }
            if receipt_bot_id and (receipt_user_id or receipt_group_id):
                pending_request["receipt"] = {
                    "bot_id": str(receipt_bot_id),
                    "user_id": str(receipt_user_id) if receipt_user_id else None,
                    "group_id": str(receipt_group_id) if receipt_group_id else None,
                    "channel_id": (
                        str(receipt_channel_id) if receipt_channel_id else None
                    ),
                }
                if receipt_platform_scope:
                    pending_request["receipt"]["platform_scope"] = (
                        str(receipt_platform_scope).strip().lower()
                    )
            state[_PENDING_REQUEST_KEY] = pending_request
            state[_LAUNCHER_ACTION_KEY] = _ACTION_RESTART
            state[_LAUNCHER_NOT_BEFORE_KEY] = time.time() + 1.0
            return "created", ""

        result, detail = mutate_restart_state(update)
    except Exception as e:
        logger.error(f"写入重启状态失败: {e}", "重启")
        return False, "写入重启状态失败。"
    if result == "rejected":
        return False, detail
    if result == "duplicate":
        return await _schedule_restart()

    ok, message = await _schedule_restart()
    logger.info(f"收到重启请求，来源: {source}", "重启")
    return ok, message


def _validated_dependency_paths(paths: set[Path]) -> list[str]:
    root = Path().resolve()
    root_files = {
        (root / "pyproject.toml").resolve(),
        (root / "uv.lock").resolve(),
    }
    plugin_roots = [
        (root / "zhenxun" / "plugins").resolve(),
        (root / "zhenxun" / "builtin_plugins").resolve(),
    ]
    result: list[str] = []
    for raw_path in paths:
        path = raw_path.resolve()
        if path in root_files:
            result.append(path.relative_to(root).as_posix())
            continue
        if path.name not in {"requirement.txt", "requirements.txt"} or not any(
            path.is_relative_to(base) for base in plugin_roots
        ):
            raise ValueError("dependency_path_not_allowed")
        result.append(path.relative_to(root).as_posix())
    return sorted(set(result))


async def request_dependency_restart(source: str, paths: set[Path]) -> tuple[bool, str]:
    if not os.getenv("ZHENXUN_LAUNCHER_PID"):
        return False, "当前不是 launcher 托管模式，请手动同步依赖并重启真寻。"
    try:
        dependency_paths = _validated_dependency_paths(paths)
    except ValueError:
        return False, "依赖路径不在允许范围内。"
    if not dependency_paths:
        return await request_restart(source)

    try:

        def update(state: dict[str, Any]) -> None:
            existing_paths = state.get(_DEPENDENCY_PATHS_KEY, [])
            if not isinstance(existing_paths, list):
                existing_paths = []
            state.setdefault(
                _PENDING_REQUEST_KEY,
                {
                    "request_id": uuid4().hex,
                    "source": source,
                    "requested_at": time.time(),
                },
            )
            state[_LAUNCHER_ACTION_KEY] = _ACTION_SYNC_DEPENDENCIES
            state[_DEPENDENCY_PATHS_KEY] = sorted(
                set(existing_paths) | set(dependency_paths)
            )
            state[_LAUNCHER_NOT_BEFORE_KEY] = time.time() + 1.0

        mutate_restart_state(update)
    except Exception as e:
        logger.error(f"写入依赖重启状态失败: {e}", "重启")
        return False, "写入依赖重启状态失败。"
    ok, message = await _schedule_restart()
    logger.info("收到同步依赖后重启请求", "重启")
    return ok, message


async def handle_restart_connect(bot: Bot) -> None:
    if (
        _receipt_context is None
        or not _receipt_context.accepting
        or not startup_coordinator.runtime_ready
        or _restart_pending
    ):
        return
    async with _receipt_lock:
        pending = read_restart_state().get(_PENDING_REQUEST_KEY)
        if not isinstance(pending, dict):
            return
        receipt = pending.get("receipt")
        if not isinstance(receipt, dict):
            return
        if not _receipt_matches_bot(receipt, bot):
            return
        # Upgrade old receipts atomically without changing their private target.
        if not pending.get("request_id"):

            def identify(state: dict[str, Any]) -> None:
                if state.get(_PENDING_REQUEST_KEY) == pending:
                    state[_PENDING_REQUEST_KEY] = {**pending, "request_id": uuid4().hex}

            mutate_restart_state(identify)
            pending = read_restart_state().get(_PENDING_REQUEST_KEY)
            if not isinstance(pending, dict) or pending.get("receipt") != receipt:
                return
        request_id = str(pending["request_id"])
        active = [
            entry
            for registry in (_receipt_jobs, _receipt_sends)
            if (entry := registry.get(request_id)) and not entry[1].done()
        ]
        if active:
            if all(
                owner is bot and task not in _receipt_revoked for owner, task in active
            ):
                return
            _receipt_replacements[request_id] = bot
            await _cancel_receipt_tasks([task for _, task in active], _receipt_context)
            _resume_receipt_replacement(request_id, pending)
            return
        _spawn_receipt_job(bot, pending)


def _receipt_matches_bot(receipt: dict[str, Any], bot: Bot) -> bool:
    from zhenxun.utils.platform import PlatformUtils

    scope = receipt.get("platform_scope")
    return str(receipt.get("bot_id") or "") == str(bot.self_id) and (
        not scope or scope == PlatformUtils.get_platform_scope(bot)
    )


def _track_receipt_task(registry, request_id, bot, task, pending) -> None:
    registry[request_id] = (bot, task)

    def finished(completed: asyncio.Task) -> None:
        current = registry.get(request_id)
        if current and current[1] is completed:
            registry.pop(request_id, None)
        _receipt_revoked.discard(completed)
        _resume_receipt_replacement(request_id, pending)

    task.add_done_callback(finished)


def _spawn_receipt_job(bot: Bot, pending: dict[str, Any]) -> None:
    context = _receipt_context
    if context is None or not context.accepting or _restart_pending:
        return
    request_id = str(pending["request_id"])
    task = Context().run(
        context.spawn_task,
        _retry_restart_receipt(bot, pending),
        name=f"restart-receipt:{request_id}",
        persistent=False,
    )
    _track_receipt_task(_receipt_jobs, request_id, bot, task, pending)


def _resume_receipt_replacement(request_id: str, pending: dict[str, Any]) -> None:
    if any(
        entry and not entry[1].done()
        for entry in (_receipt_jobs.get(request_id), _receipt_sends.get(request_id))
    ):
        return
    bot = _receipt_replacements.pop(request_id, None)
    if bot is not None and read_restart_state().get(_PENDING_REQUEST_KEY) == pending:
        _spawn_receipt_job(bot, pending)


async def _cancel_receipt_tasks(
    tasks: list[asyncio.Task], context: LifecycleContext | None
) -> bool:
    active = {task for task in tasks if not task.done()}
    for task in active:
        if task not in _receipt_revoked:
            _receipt_revoked.add(task)
            task.cancel()
    if not active:
        return True
    _, unconfirmed = await asyncio.wait(
        active, timeout=remaining_timeout(_RECEIPT_CANCEL_GRACE)
    )
    if unconfirmed:
        if context is not None:
            context.kernel.require_recovery(_RECEIPT_COMPONENT)
        logger.warning("重启回执任务取消未确认，保留任务归属并要求恢复。", "重启")
    return not unconfirmed


async def handle_restart_disconnect(bot: Bot) -> None:
    for request_id, replacement in list(_receipt_replacements.items()):
        if replacement is bot:
            _receipt_replacements.pop(request_id, None)
    tasks = [
        task
        for registry in (_receipt_jobs, _receipt_sends)
        for owner, task in registry.values()
        if owner is bot
    ]
    await _cancel_receipt_tasks(tasks, _receipt_context)


async def _attempt_restart_receipt(
    bot: Bot, pending: dict[str, Any], context: LifecycleContext
) -> bool:
    request_id = str(pending["request_id"])
    send = Context().run(
        context.spawn_task,
        _handle_restart_receipt(bot, pending, clear_on_success=False),
        name=f"restart-receipt-send:{request_id}",
        persistent=False,
    )
    _track_receipt_task(_receipt_sends, request_id, bot, send, pending)
    try:
        done, _ = await asyncio.wait({send}, timeout=_RECEIPT_TIMEOUT)
        if not done:
            # A timed-out send must finish cancellation before another can start.
            return not await _cancel_receipt_tasks([send], context)
        if send.cancelled() or send in _receipt_revoked:
            return True
        if send.result():
            _clear_restart_receipt(pending)
            return True
        return False
    except asyncio.CancelledError:
        await _cancel_receipt_tasks([send], context)
        raise


async def _retry_restart_receipt(bot: Bot, pending: dict[str, Any]) -> None:
    context = _receipt_context
    if context is None:
        return
    started = time.monotonic()
    for offset in _RECEIPT_ATTEMPTS:
        await asyncio.sleep(max(0, started + offset - time.monotonic()))
        if (
            _restart_pending
            or read_restart_state().get(_PENDING_REQUEST_KEY) != pending
        ):
            return
        try:
            if await _attempt_restart_receipt(bot, pending, context):
                return
        except Exception as error:
            logger.warning(
                f"重启回执发送失败: {type(error).__name__}，已保留待重试。", "重启"
            )


async def _sweep_restart_receipts() -> None:
    await startup_coordinator.wait_runtime_ready()
    for bot in list(nonebot.get_bots().values()):
        await handle_restart_connect(bot)


@PriorityLifecycle.on_startup(
    priority=0, stage="runtime", component_id=_RECEIPT_COMPONENT, pass_context=True
)
async def _start_restart_receipts(context: LifecycleContext) -> None:
    global _receipt_context
    _receipt_context = context
    # The component owns this work beyond the startup hook's temporary lease.
    Context().run(
        context.spawn_task,
        _sweep_restart_receipts(),
        name="restart-receipt-sweep",
        persistent=False,
    )


@PriorityLifecycle.on_shutdown(priority=0, component_id=_RECEIPT_COMPONENT)
async def _stop_restart_receipts() -> None:
    global _receipt_context
    context = _receipt_context
    _receipt_context = None
    _receipt_replacements.clear()
    tasks = [
        task
        for registry in (_receipt_jobs, _receipt_sends)
        for _, task in registry.values()
    ]
    await _cancel_receipt_tasks(tasks, context)


async def _handle_restart_receipt(
    bot: Bot,
    expected_request: dict[str, Any] | None = None,
    *,
    clear_on_success: bool = True,
) -> bool:
    state = read_restart_state()
    pending_request = state.get(_PENDING_REQUEST_KEY)
    if not isinstance(pending_request, dict):
        return True
    if expected_request is not None and pending_request != expected_request:
        return True

    source = str(pending_request.get("source", "unknown"))
    receipt = pending_request.get("receipt")
    if not isinstance(receipt, dict):
        logger.info(f"检测到重启完成，来源: {source}", "重启")

        def clear_unaddressed(value):
            if value.get(_PENDING_REQUEST_KEY) == pending_request:
                value.pop(_PENDING_REQUEST_KEY, None)

        mutate_restart_state(clear_unaddressed)
        return True

    expected_bot_id = str(receipt.get("bot_id", ""))
    receipt_user_id = str(receipt.get("user_id") or "")
    if not _receipt_matches_bot(receipt, bot):
        logger.debug(
            f"重启回执等待目标 Bot 连接: source={source} bot={expected_bot_id}"
        )
        return False

    logger.info(f"检测到重启完成，来源: {source}", "重启")

    from zhenxun.configs.config import BotConfig
    from zhenxun.utils.message import MessageUtils
    from zhenxun.utils.platform import PlatformUtils

    target = PlatformUtils.get_target(
        user_id=receipt_user_id,
        group_id=str(receipt.get("group_id") or "") or None,
        channel_id=str(receipt.get("channel_id") or "") or None,
    )
    if target:
        try:
            await MessageUtils.build_message(
                f"{BotConfig.self_nickname}已成功重启！"
            ).send(target, bot=bot)
        except Exception as e:
            logger.warning(
                f"重启已完成，但回执发送失败: {type(e).__name__}；"
                "已保留回执，等待重试。",
                "重启",
            )
            return False
    else:
        logger.warning("未找到重启回执目标，已保留回执。", "重启")
        return False

    if clear_on_success:
        _clear_restart_receipt(pending_request)
    return True


def _clear_restart_receipt(pending_request: dict[str, Any]) -> None:
    def clear_request(value: dict[str, Any]) -> None:
        current = value.get(_PENDING_REQUEST_KEY)
        if current == pending_request:
            value.pop(_PENDING_REQUEST_KEY, None)

    mutate_restart_state(clear_request)


def _finalize_restart_state_on_startup() -> None:
    result: dict[str, Any] = {}

    def finalize(state: dict[str, Any]) -> None:
        state.pop(_PENDING_RESTARTS_KEY, None)
        clear_restart_tickets(state)
        pending_request = state.get(_PENDING_REQUEST_KEY)
        if not isinstance(pending_request, dict):
            return
        pending_request.setdefault("request_id", uuid4().hex)
        result.update(pending_request)
        if not isinstance(pending_request.get("receipt"), dict):
            state.pop(_PENDING_REQUEST_KEY, None)

    mutate_restart_state(finalize)
    if not result:
        return
    source = str(result.get("source", "unknown"))
    if isinstance(result.get("receipt"), dict):
        logger.info(f"检测到待发送的重启回执，来源: {source}", "重启")
    else:
        logger.info(f"检测到重启完成，来源: {source}", "重启")


@PriorityLifecycle.on_startup(priority=0, stage="management", timeout=10)
async def _cleanup_restart_artifacts() -> None:
    _cleanup_legacy_restart_artifacts()
    _finalize_restart_state_on_startup()


@PriorityLifecycle.on_shutdown(priority=99)
async def _notify_restart_shutdown() -> None:
    if _restart_pending:
        logger.info("launcher 将在当前 worker 退出后接管重启。", "重启")
