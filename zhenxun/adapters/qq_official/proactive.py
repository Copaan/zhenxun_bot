"""QQ 官方主动消息：群与单聊的主动消息许可记录与发送频率限制"""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone
import time

from nonebot.adapters import Event
from nonebot.adapters.qq.event import (
    C2CMsgReceiveEvent,
    C2CMsgRejectEvent,
    FriendDelEvent,
    GroupDelRobotEvent,
    GroupMsgReceiveEvent,
    GroupMsgRejectEvent,
)

from .context import OfficialReplyUnavailable, digest_identifier
from .models import QQProactivePermission

# 主动消息无权限、用户拒收主动消息
DENIED_CODES = frozenset({40034105, 40054013})
# 因无权限被拒后，超过该时长再重新尝试，以免漏收开启事件后一直无法发送
DENIED_RETRY_AFTER = timedelta(hours=24)
_WINDOW_SECONDS = 60.0
# 按未认证机器人的限制：每个场景每分钟 30 条，每个群或用户每分钟 20 条
_BOT_PER_MINUTE = 30
_TARGET_PER_MINUTE = 20
_MAX_TRACKED_TARGETS = 10_000

_sent: dict[tuple[str, str, str], deque[float]] = {}
_limit_lock = asyncio.Lock()


class ProactiveMessageDenied(OfficialReplyUnavailable):
    """群未开启或用户关闭了主动消息，未向平台发送请求"""


def proactive_target(scene: str, target: str) -> str:
    """生成主动消息许可记录中的目标标识

    参数:
        scene: group 或 c2c
        target: 群 openid 或用户 openid

    返回:
        str: 群为 openid 原文，单聊为 openid 摘要
    """
    return target if scene == "group" else digest_identifier(target)


async def proactive_allowed(app_id: str, scene: str, target: str) -> bool | None:
    """查询主动消息许可

    参数:
        app_id: 机器人 AppID
        scene: group 或 c2c
        target: 群 openid 或用户 openid

    返回:
        bool | None: True 已允许，False 近期被拒绝，None 未知
    """
    row = await QQProactivePermission.get_or_none(
        app_id=app_id, scene=scene, target=proactive_target(scene, target)
    )
    if row is None:
        return None
    if row.allowed:
        return True
    recent = await QQProactivePermission.filter(
        id=row.id, updated_at__gte=datetime.now(timezone.utc) - DENIED_RETRY_AFTER
    ).exists()
    return False if recent else None


async def set_proactive_allowed(
    app_id: str, scene: str, target: str, allowed: bool
) -> None:
    """记录主动消息许可，状态未变化时不写库

    参数:
        app_id: 机器人 AppID
        scene: group 或 c2c
        target: 群 openid 或用户 openid
        allowed: 是否允许
    """
    key = proactive_target(scene, target)
    row = await QQProactivePermission.get_or_none(
        app_id=app_id, scene=scene, target=key
    )
    if row is None:
        await QQProactivePermission.update_or_create(
            app_id=app_id, scene=scene, target=key, defaults={"allowed": allowed}
        )
    elif row.allowed != allowed or not allowed:
        row.allowed = allowed
        await row.save(update_fields=["allowed", "updated_at"])


async def forget_proactive_target(app_id: str, scene: str, target: str) -> None:
    """删除主动消息许可记录

    参数:
        app_id: 机器人 AppID
        scene: group 或 c2c
        target: 群 openid 或用户 openid
    """
    await QQProactivePermission.filter(
        app_id=app_id, scene=scene, target=proactive_target(scene, target)
    ).delete()


async def proactive_groups(app_id: str) -> list[str]:
    """获取已开启主动消息的群

    参数:
        app_id: 机器人 AppID

    返回:
        list[str]: 群 openid 列表
    """
    rows = await QQProactivePermission.filter(
        app_id=app_id, scene="group", allowed=True
    ).values("target")
    return [row["target"] for row in rows]


async def record_switch_event(app_id: str, event: Event) -> None:
    """按群管理员或用户的主动消息开关事件、移出群与删除好友事件更新许可记录

    参数:
        app_id: 机器人 AppID
        event: 官方事件
    """
    if isinstance(event, GroupMsgReceiveEvent | GroupMsgRejectEvent):
        await set_proactive_allowed(
            app_id,
            "group",
            event.group_openid,
            isinstance(event, GroupMsgReceiveEvent),
        )
    elif isinstance(event, C2CMsgReceiveEvent | C2CMsgRejectEvent):
        await set_proactive_allowed(
            app_id, "c2c", event.openid, isinstance(event, C2CMsgReceiveEvent)
        )
    elif isinstance(event, GroupDelRobotEvent):
        await forget_proactive_target(app_id, "group", event.group_openid)
    elif isinstance(event, FriendDelEvent):
        await forget_proactive_target(app_id, "c2c", event.openid)


async def acquire_proactive_slot(app_id: str, scene: str, target: str) -> None:
    """按机器人和接收方两个维度限制主动消息频率，超出时等待

    参数:
        app_id: 机器人 AppID
        scene: group 或 c2c
        target: 群 openid 或用户 openid
    """
    limits = (
        ((app_id, scene, ""), _BOT_PER_MINUTE),
        ((app_id, scene, proactive_target(scene, target)), _TARGET_PER_MINUTE),
    )
    while True:
        async with _limit_lock:
            now = time.monotonic()
            if len(_sent) > _MAX_TRACKED_TARGETS:
                for key in [
                    k
                    for k, v in _sent.items()
                    if not v or now - v[-1] >= _WINDOW_SECONDS
                ]:
                    del _sent[key]
            wait = 0.0
            for key, limit in limits:
                stamps = _sent.setdefault(key, deque())
                while stamps and now - stamps[0] >= _WINDOW_SECONDS:
                    stamps.popleft()
                if len(stamps) >= limit:
                    wait = max(wait, stamps[0] + _WINDOW_SECONDS - now)
            if wait <= 0:
                for key, _ in limits:
                    _sent[key].append(now)
                return
        await asyncio.sleep(wait)


__all__ = [
    "DENIED_CODES",
    "ProactiveMessageDenied",
    "acquire_proactive_slot",
    "forget_proactive_target",
    "proactive_allowed",
    "proactive_groups",
    "record_switch_event",
    "set_proactive_allowed",
]
