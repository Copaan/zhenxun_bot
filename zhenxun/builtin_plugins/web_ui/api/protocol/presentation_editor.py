"""QQ 编辑器的联系人选择及私聊测试。"""

import asyncio
from copy import deepcopy
from typing import Literal

from fastapi import APIRouter, HTTPException, Response
import nonebot
from nonebot.adapters.qq.exception import ActionFailed

from zhenxun.adapters.qq_official.presentation import (
    CARD_KINDS,
    build_card_message,
    editor_contacts,
    editor_test_event,
    empty_card,
    validate_cards,
)

from ...base_model import Result
from ...utils import authentication, webui_db_call
from .presentation import PresentationMutation, _mutate, _saved_bot

router = APIRouter(prefix="/qq", dependencies=[authentication()])


@router.get("/{app_id}/contacts")
async def get_contacts(
    app_id: str, response: Response, scope: Literal["c2c", "group"] = "c2c"
) -> Result:
    """按当前 AppID 读取可确认归属的有限联系人。"""
    _saved_bot(app_id)
    response.headers["Cache-Control"] = "no-store"
    items = {item["openid"]: item for item in editor_contacts(app_id, scope)}
    if scope == "group":
        from zhenxun.models.bot_group_policy import BotGroupMembership
        from zhenxun.services.platform_identity import native_id

        try:
            rows = await webui_db_call(
                BotGroupMembership.filter(
                    bot_id=f"qq_api:{app_id}", platform_scope="qq_api", channel_id=""
                )
                .order_by("-last_seen")
                .limit(100)
                .values("group_id", "group_name"),
                "QQEditor.contacts",
            )
        except Exception as error:
            raise HTTPException(
                503, "群联系人暂不可读取，请稍后刷新；仍可手动填写当前账号的 OpenID。"
            ) from error
        for row in rows:
            openid = native_id(row["group_id"], "qq_api", group=True, bot_id=app_id)
            if openid:
                items[openid] = {
                    "openid": openid,
                    "label": (row["group_name"] or "群名称未提供")
                    + " · "
                    + openid[-6:],
                    "can_test": False,
                }
    return Result.ok(
        {
            "items": list(items.values())[:100],
            "message": (
                "用户列表来自本次运行收到的私聊，未获取昵称时显示标识尾号；重启后请重新私聊机器人。"
                if scope == "c2c"
                else "只列出当前账号最近记录的群，不保证机器人仍在群中。"
            ),
        }
    )


def prepare_test(app_id: str, payload: dict):
    """核对草稿、账号与私聊归属，并在发送前构造消息。"""
    from zhenxun.adapters.qq_official.bot import ZhenxunQQBot

    if set(payload) != {"kind", "card", "target"}:
        raise ValueError("测试参数必须包含卡片类型、草稿和接收人。")
    kind, target = payload["kind"], payload["target"]
    if (
        kind not in CARD_KINDS
        or not isinstance(target, str)
        or not target
        or len(target) > 128
    ):
        raise ValueError("卡片类型或接收人无效。")
    cards = {key: empty_card() for key in CARD_KINDS}
    cards[kind] = deepcopy(payload["card"])
    validate_cards(cards)
    cards[kind]["enabled"] = True
    validate_cards(cards)
    config = _saved_bot(app_id)
    bot = nonebot.get_bots().get(app_id)
    if not isinstance(bot, ZhenxunQQBot) or bot.bot_info.secret != config["secret"]:
        raise ValueError("当前机器人消息连接不可用，请连接后再测试。")
    event = editor_test_event(app_id, target)
    return bot, event, build_card_message(cards[kind], private=kind != "welcome_group")


async def deliver_test(prepared) -> dict:
    """通过已有被动回复机制测试草稿，未知结果不重发。"""
    from zhenxun.adapters.qq_official.context import OfficialReplyUnavailable

    bot, event, message = prepared
    try:
        receipt = await asyncio.wait_for(
            bot.send(event, message, passive_only=True), timeout=20
        )
    except OfficialReplyUnavailable:
        return {
            "state": "failed",
            "message": "私聊回复窗口不可用，测试未发送。请接收人重新私聊机器人。",
        }
    except ActionFailed as error:
        rejected = error.status_code and 400 <= error.status_code < 500
        return {
            "state": "failed" if rejected else "uncertain",
            "message": "QQ 拒绝了测试消息。"
            if rejected
            else "测试结果未知，请先在 QQ 核对，不要重复发送。",
            "diagnostic": {
                "provider_code": error.code,
                "http_status": error.status_code,
            },
        }
    except Exception:
        return {
            "state": "uncertain",
            "message": "测试结果未知，请先在 QQ 核对，不要重复发送。",
        }
    if receipt is None:
        return {
            "state": "uncertain",
            "message": "未取得发送回执，请先在 QQ 核对，不要重复发送。",
        }
    return {"state": "confirmed", "message": "测试消息已被 QQ 接受，不代表用户已阅读。"}


@router.post("/{app_id}/cards/test")
async def test_card(app_id: str, request: PresentationMutation) -> Result:
    return await _mutate(app_id, request, "card_test", "/cards/test")
