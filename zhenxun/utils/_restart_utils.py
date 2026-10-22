import asyncio
import contextlib
import os
from pathlib import Path
import time
from typing import Any

from nonebot.adapters import Bot

from zhenxun.services.log import logger
from zhenxun.services.startup import startup_coordinator
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

_RECEIPT_READY_TIMEOUT = 60

_restart_pending: bool = False


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
    from zhenxun.services.nonebot_store.environment import (
        REQUIREMENT_FILES,
        SOURCE_PLUGIN_ROOT,
    )

    root = Path().resolve()
    root_files = {
        (root / "pyproject.toml").resolve(),
        (root / "uv.lock").resolve(),
    }
    plugin_root = (root / SOURCE_PLUGIN_ROOT).resolve()
    result: list[str] = []
    for raw_path in paths:
        path = raw_path.resolve()
        if path in root_files:
            result.append(path.relative_to(root).as_posix())
            continue
        if path.name not in REQUIREMENT_FILES or not path.is_relative_to(plugin_root):
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
                {"source": source, "requested_at": time.time()},
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
    """目标 Bot 连接后发送一次重启回执，发送失败不再重试

    参数:
        bot: 已连接的 Bot
    """
    pending = read_restart_state().get(_PENDING_REQUEST_KEY)
    if _restart_pending or not isinstance(pending, dict):
        return
    receipt = pending.get("receipt")
    if not isinstance(receipt, dict) or not _receipt_matches_bot(receipt, bot):
        return
    if not startup_coordinator.runtime_ready:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                startup_coordinator.wait_runtime_ready(), _RECEIPT_READY_TIMEOUT
            )
        if not startup_coordinator.runtime_ready:
            return

    def take(state: dict[str, Any]) -> bool:
        if state.get(_PENDING_REQUEST_KEY) != pending:
            return False
        state.pop(_PENDING_REQUEST_KEY, None)
        return True

    if not mutate_restart_state(take, write_if=bool):
        return
    logger.info(f"检测到重启完成，来源: {pending.get('source', 'unknown')}", "重启")

    from zhenxun.configs.config import BotConfig
    from zhenxun.utils.message import MessageUtils
    from zhenxun.utils.platform import PlatformUtils

    target = PlatformUtils.get_target(
        user_id=str(receipt.get("user_id") or "") or None,
        group_id=str(receipt.get("group_id") or "") or None,
        channel_id=str(receipt.get("channel_id") or "") or None,
        bot=bot,
    )
    if not target:
        logger.warning("未找到重启回执目标，已跳过发送。", "重启")
        return
    try:
        await MessageUtils.build_message(f"{BotConfig.self_nickname}已成功重启！").send(
            target, bot=bot
        )
    except Exception as e:
        logger.warning(f"发送重启回执失败: {type(e).__name__}", "重启")


def _receipt_matches_bot(receipt: dict[str, Any], bot: Bot) -> bool:
    from zhenxun.utils.platform import PlatformUtils

    scope = receipt.get("platform_scope")
    return str(receipt.get("bot_id") or "") == str(bot.self_id) and (
        not scope or scope == PlatformUtils.get_platform_scope(bot)
    )


def _finalize_restart_state_on_startup() -> None:
    result: dict[str, Any] = {}

    def finalize(state: dict[str, Any]) -> None:
        state.pop(_PENDING_RESTARTS_KEY, None)
        clear_restart_tickets(state)
        pending_request = state.get(_PENDING_REQUEST_KEY)
        if not isinstance(pending_request, dict):
            return
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
