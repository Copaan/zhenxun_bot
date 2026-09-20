from nonebot import on_message
from nonebot.adapters import Bot, Event
from nonebot.plugin import PluginMetadata
from nonebot.rule import Rule
from nonebot.typing import T_State
from nonebot_plugin_alconna import Alconna, Args, Match, on_alconna
from nonebot_plugin_uninfo import Uninfo

from zhenxun.configs.utils import Command, PluginExtraData
from zhenxun.services.account_binding import (
    binding_status,
    confirm_unbind,
    issue_binding,
    issue_unbind,
    redeem_binding,
)
from zhenxun.services.account_binding_routing import binding_argument
from zhenxun.services.business_identity import (
    BusinessIdentityError,
    resolve_business_identity,
)
from zhenxun.services.message_execution import current_execution
from zhenxun.utils.message import MessageUtils

__plugin_meta__ = PluginMetadata(
    name="QQ账号绑定",
    description="将官方 QQ / 频道身份关联到已验证的 QQ 业务账号",
    usage=(
        "官方侧：绑定QQ 目标QQ；OneBot 私聊直接发码，群聊 @Bot 发码；"
        "绑定状态；解绑QQ"
    ),
    extra=PluginExtraData(
        author="zhenxun",
        version="1.0",
        commands=[
            Command(command="绑定QQ"),
            Command(command="绑定状态"),
            Command(command="解绑QQ"),
        ],
    ).to_dict(),
)


async def _binding_rule(bot: Bot, event: Event, state: T_State) -> bool:
    argument = binding_argument(bot, event)
    if argument is None:
        return False
    state["binding_argument"] = argument
    return True


bind = on_message(rule=Rule(_binding_rule), priority=5, block=True)
status = on_alconna(Alconna("绑定状态"), priority=5, block=True)
unbind = on_alconna(Alconna("解绑QQ", Args["value?", str]), priority=5, block=True)


@bind.handle()
async def bind_account(bot: Bot, event: Event, session: Uninfo, state: T_State):
    try:
        actor = await resolve_business_identity(bot, event, session)
        argument = state["binding_argument"]
        if argument.upper().startswith("ZX-"):
            result = await redeem_binding(actor, argument)
            message = (
                f"绑定完成，当前使用 QQ 主账号：{result['storage_key']}。"
                "原账号资产保留，不自动合并。"
            )
            if result.get("superseded"):
                message = "该申请此前已完成，关联关系此后有变化，请发送绑定状态查询。"
        else:
            execution = current_execution.get()
            code = await issue_binding(
                actor, argument, operation_id=execution.identity if execution else None
            )
            message = (
                f"请使用 QQ {argument} 在 OneBot 私聊直接发送：\n{code}\n"
                "群聊请 @当前 Bot 后发送同一绑定码，无需命令头。\n"
                "5 分钟内有效。绑定后共用该 QQ 的签到、金币、银行和道具；"
                "原账号资产保留。"
            )
    except BusinessIdentityError as error:
        message = str(error)
    await MessageUtils.build_message(message).finish()


@status.handle()
async def show_status(bot: Bot, event: Event, session: Uninfo):
    try:
        actor = await resolve_business_identity(bot, event, session)
        rows = await binding_status(actor)
        message = "\n".join(
            f"身份：{row['identity_id']}\n"
            f"{row['domain']}/{row['app_id']}/{row['scene']} "
            f"账号：{row['storage_key']} 修订：{row['revision']} "
            f"{'已绑定' if row['bound'] else '独立账号'}"
            + ("（历史归属待核验）" if not row["verified"] else "")
            for row in rows
        )
    except BusinessIdentityError as error:
        message = str(error)
    await MessageUtils.build_message(message).finish()


@unbind.handle()
async def unbind_account(bot: Bot, event: Event, session: Uninfo, value: Match[str]):
    try:
        actor = await resolve_business_identity(bot, event, session)
        argument = value.result.strip() if value.available else ""
        if argument.upper().startswith("ZX-"):
            result = await confirm_unbind(actor, argument)
            message = (
                f"解绑完成，恢复原账号：{result['storage_key']}。"
                "绑定期间资产留在 QQ 主账号。"
            )
            if result.get("superseded"):
                message = (
                    "该解绑申请此前已完成，关联关系此后有变化，请发送绑定状态查询。"
                )
        else:
            execution = current_execution.get()
            code = await issue_unbind(
                actor,
                argument or None,
                operation_id=execution.identity if execution else None,
            )
            message = (
                f"请在当前身份发送：解绑QQ {code}\n5 分钟内有效。"
                "解绑不返还或拆分绑定期间资产，也不重置签到和领取记录。"
            )
    except BusinessIdentityError as error:
        message = str(error)
    await MessageUtils.build_message(message).finish()
