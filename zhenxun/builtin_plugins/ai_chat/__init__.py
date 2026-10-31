from copy import deepcopy
from itertools import count
import time
from typing import Any, Literal

from nonebot import on_message
from nonebot.adapters import Bot, Event
from nonebot.plugin import PluginMetadata
from nonebot.typing import T_State
from nonebot_plugin_alconna import Alconna, UniMsg, on_alconna
from nonebot_plugin_apscheduler import scheduler
from nonebot_plugin_uninfo import Uninfo

from zhenxun.configs.utils import (
    Command,
    ConfigUIModel,
    PluginExtraData,
    RegisterConfig,
)
from zhenxun.models.ai_agent_call import AIAgentCall
from zhenxun.models.ai_agent_run import AIAgentRun
from zhenxun.models.ai_token_usage import AITokenUsage
from zhenxun.services.ai.run.context import NoneBotDeps
from zhenxun.services.ai.run.dsh import DshRuntime, DshSettings
from zhenxun.services.ai.utils.runtime import ContextUtils
from zhenxun.services.log import logger
from zhenxun.utils.manager.priority_manager import PriorityLifecycle
from zhenxun.utils.message import MessageUtils
from zhenxun.utils.platform import PlatformUtils
from zhenxun.utils.pydantic_compat import model_dump

from ._buffer import AtWaiter, ChatBuffer, ChatLine, sender_name, summarize
from ._data_source import MODULE, AIChatManager
from ._proactive import PROACTIVE_KEY, Proactive
from ._reply import quote_reply
from ._tasks import register_task_job

_ORDER = count(1)


def _options(**labels: str) -> list[dict[str, str]]:
    """配置项的可选值

    参数:
        labels: 值到 WebUI 显示名称的映射

    返回:
        list[dict[str, str]]: 可选值与显示名称
    """
    return [{"value": value, "label": label} for value, label in labels.items()]


def _when(
    path: str, value: Any, operator: Literal["eq", "neq"] = "eq"
) -> dict[str, Any]:
    """配置项在 WebUI 中的显示条件

    参数:
        path: 作为条件的配置键
        value: 比较的值
        operator: eq 等于时显示，neq 不等于时显示

    返回:
        dict[str, Any]: 显示条件
    """
    return {"path": path, "operator": operator, "value": value}


def _config(
    section: str,
    key: str,
    value: Any,
    type_: Any,
    label: str,
    description: str,
    **ui: Any,
) -> RegisterConfig:
    """AI 聊天配置项，WebUI 按分组和登记顺序展示

    参数:
        section: WebUI 中的分组
        key: 配置键
        value: 默认值
        type_: 值类型
        label: WebUI 中的名称
        description: 配置说明
        ui: 其余展示设置，如 options、unit、secret、advanced、visible_when

    返回:
        RegisterConfig: 配置项
    """
    return RegisterConfig(
        key=key,
        value=value,
        help=description,
        default_value=deepcopy(value),
        type=type_,
        ui=ConfigUIModel(label=label, section=section, order=next(_ORDER), **ui),
    )


__plugin_meta__ = PluginMetadata(
    name="AI 聊天",
    description="和 Bot 用自然语言聊天",
    usage="""
    @Bot、回复 Bot 的消息、私聊、以 / 或唤醒词开头即可聊天
    群聊中全群共用一段对话记忆，Bot 能看到上次发言之后群里的新消息
    只 @ Bot 不说话时，Bot 会等你 15 秒内的下一条消息一起回复
    群聊中可按概率主动插话（在按群设置或聊天设置中调整概率）
    指令:
        停止对话 / /停止对话: 中止自己发起的对话
        重置对话 / /重置对话: 清空当前群或私聊的对话记忆（群聊限群管理员）
    """.strip(),
    extra=PluginExtraData(
        author="HibiKier",
        version="0.1",
        menu_type="AI",
        commands=[Command(command="停止对话"), Command(command="重置对话")],
        configs=[
            _config(
                "唤醒",
                "WAKE_WORDS",
                [],
                list[str],
                "唤醒词",
                "消息以唤醒词开头时回复；@Bot、回复 Bot 的消息和私聊总会回复",
            ),
            _config(
                "唤醒",
                "SLASH_WAKE",
                True,
                bool,
                "/ 开头唤醒",
                "消息以 / 开头时回复，/ 不发给 AI；与已注册的指令同名时执行指令",
            ),
            _config(
                "回复",
                "CONTEXT_SIZE",
                20,
                int,
                "群聊背景消息数",
                "回复时一并交给 AI 的群聊近期消息条数，0 为不附带",
                unit="条",
            ),
            _config(
                "回复",
                "REPLY_QUOTE",
                False,
                bool,
                "引用触发消息",
                "群聊回复时引用触发回复的那条消息",
            ),
            _config(
                "回复",
                "SPLIT_REPLY",
                False,
                bool,
                "分段发送",
                "把回复按句拆成多条消息依次发送",
            ),
            _config(
                "回复",
                "SPLIT_INTERVAL",
                "1.5,3.5",
                str,
                "分段间隔",
                "每段发送前等待的秒数，写成“最小,最大”时随机取值",
                visible_when=_when("SPLIT_REPLY", True),
            ),
            _config(
                "回复",
                "SPLIT_MAX_CHARS",
                0,
                int,
                "分段字数上限",
                "回复超过该字数时整条发送、不再分段，0 为不限制",
                unit="字",
                visible_when=_when("SPLIT_REPLY", True),
            ),
            _config(
                "回复",
                "SPLIT_PATTERN",
                "",
                str,
                "分段规则",
                "正则表达式，在每个匹配之后切分，匹配内容保留在前一段；"
                "留空时按句末标点切分，换行始终切分",
                placeholder="[。！？!?]|哈哈+",
                visible_when=_when("SPLIT_REPLY", True),
            ),
            _config(
                "回复",
                "SPLIT_CLEANUP",
                "",
                str,
                "分段清理规则",
                "正则表达式，从每一段中删除匹配内容，如段尾的句号",
                placeholder="[。，]$",
                visible_when=_when("SPLIT_REPLY", True),
            ),
            _config(
                "回复",
                "LONG_REPLY_CHARS",
                1500,
                int,
                "长回复字数",
                "回复超过该字数时按长回复方式发送，0 为不区分长回复",
                unit="字",
            ),
            _config(
                "回复",
                "LONG_REPLY",
                "forward",
                str,
                "长回复方式",
                "长回复的发送方式；QQ 官方不支持合并转发，会发成图片",
                options=_options(text="直接发送", forward="合并转发", image="转为图片"),
            ),
            _config(
                "回复",
                "VOICE_REPLY",
                "off",
                str,
                "语音回复",
                "用语音合成默认模型把回复转成语音发送",
                options=_options(off="关闭", voice="用户发语音时", always="总是"),
            ),
            _config(
                "回复",
                "SEARCH_HINT",
                "我去搜一下，稍等～",
                str,
                "搜索提示",
                "AI 联网搜索前先发送的一句话，留空不发送；模型自带的联网搜索不会触发",
            ),
            _config(
                "主动插话",
                "PROACTIVE_ENABLED",
                True,
                bool,
                "主动插话",
                "群里没人叫 Bot 时按概率让 AI 决定是否插话；不影响主动任务",
            ),
            _config(
                "主动插话",
                "ACTIVE_PROBABILITY",
                0.0,
                float,
                "插话概率",
                "每条没叫 Bot 的群消息触发插话的概率（0~1），0 为不插话；可按群设置",
                visible_when=_when("PROACTIVE_ENABLED", True),
            ),
            _config(
                "主动插话",
                "PROACTIVE_COOLDOWN",
                0,
                int,
                "插话冷却",
                "同一个群两次插话的最小间隔，0 为不限制；可按群设置",
                unit="秒",
                visible_when=_when("PROACTIVE_ENABLED", True),
            ),
            _config(
                "主动插话",
                "PROACTIVE_DAILY_TOKENS",
                0,
                int,
                "每日插话预算",
                "所有群的插话每天共用的 token 上限，0 为不限制",
                unit="tokens",
                visible_when=_when("PROACTIVE_ENABLED", True),
            ),
            _config(
                "发送与表情",
                "SEND_TOOLS",
                True,
                bool,
                "发送类工具",
                "允许 AI 在当前会话里另外发消息、戳一戳、发表情",
            ),
            _config(
                "发送与表情",
                "SEND_PER_MINUTE",
                8,
                int,
                "每分钟发送上限",
                "每个会话里 AI 每分钟最多使用发送类工具的次数，0 为不限",
                unit="次",
                visible_when=_when("SEND_TOOLS", True),
            ),
            _config(
                "发送与表情",
                "SEND_PER_HOUR",
                60,
                int,
                "每小时发送上限",
                "每个会话里 AI 每小时最多使用发送类工具的次数，0 为不限",
                unit="次",
                visible_when=_when("SEND_TOOLS", True),
            ),
            _config(
                "发送与表情",
                "STICKER_COLLECT",
                True,
                bool,
                "收藏表情",
                "允许 AI 收藏群里的表情包，每个会话每分钟最多 2 张、每小时 10 张",
            ),
            _config(
                "发送与表情",
                "STICKER_LIMIT",
                200,
                int,
                "表情库上限",
                "表情库最多保存的表情数，0 为不限",
                unit="张",
            ),
            _config(
                "发送与表情",
                "STICKER_AUTO_DESCRIBE",
                False,
                bool,
                "自动补写表情备注",
                "在后台给没有备注的表情补写备注、标签和适用场合，"
                "使用视觉默认模型（未配置时用对话模型）",
            ),
            _config(
                "发送与表情",
                "STICKER_QQ_SYNC",
                False,
                bool,
                "同步到 QQ 收藏",
                "AI 收藏表情时同时加入 Bot 账号的 QQ 收藏表情"
                "（需要协议端支持 add_custom_face，如 SnowLuma）",
            ),
            _config(
                "知识库与角色识别",
                "KNOWLEDGE_MODE",
                "inject",
                str,
                "知识库检索方式",
                "自动检索：每次对话前按消息检索并附给 AI；按需检索：由 AI 调用检索工具",
                options=_options(inject="自动检索", tool="按需检索"),
            ),
            _config(
                "知识库与角色识别",
                "CHARACTER_RECOGNITION",
                "off",
                str,
                "角色识别",
                "开启后 AI 可按需识别图片中的二次元角色；本地模型在“角色识别”页安装",
                options=_options(
                    off="关闭",
                    cloud="仅云端",
                    lite="轻量本地模型 + 云端",
                    full="完整本地模型 + 云端",
                ),
            ),
            _config(
                "知识库与角色识别",
                "CHARACTER_MIN_SCORE",
                0.5,
                float,
                "最低识别分",
                "低于该分数的候选不采用；分数为各来源分数按权重相加",
                visible_when=_when("CHARACTER_RECOGNITION", "off", "neq"),
            ),
            _config(
                "知识库与角色识别",
                "CHARACTER_SAUCENAO_KEY",
                "",
                str,
                "SauceNAO API Key",
                "填写后增加 SauceNAO 以图搜图来源",
                secret=True,
                visible_when=_when("CHARACTER_RECOGNITION", "off", "neq"),
            ),
            _config(
                "主动任务",
                "TASK_LIMIT",
                20,
                int,
                "任务总数上限",
                "对话中创建的定时、延时任务的总数上限",
                unit="个",
            ),
            _config(
                "主动任务",
                "TASK_USER_LIMIT",
                5,
                int,
                "每人任务上限",
                "每位用户最多创建的任务数，0 为不限制；超级用户不受限制",
                unit="个",
            ),
            _config(
                "主动任务",
                "TASK_CATCH_UP",
                30,
                int,
                "错过补跑时限",
                "任务错过执行时间（如 Bot 停机）后在该时间内仍会补跑，超过则跳过本次；"
                "对新建或修改的任务生效",
                unit="分钟",
            ),
            _config(
                "Agent 与执行器",
                "AGENT_ENABLED",
                False,
                bool,
                "超级用户 Agent",
                "为超级用户提供 HTTP 请求、沙箱、MCP 等工具，调用记录在 Agent 页；"
                "普通用户不会获得这些工具",
            ),
            _config(
                "Agent 与执行器",
                "AGENT_RUNNER",
                "local",
                str,
                "回复执行器",
                "使用 DeepSeek Harness 时工具、知识库与记忆不生效，"
                "主动插话仍在本地执行；"
                "可按群设置",
                options=_options(local="本地 Agent", dsh="DeepSeek Harness"),
            ),
            _config(
                "额度与安全",
                "DAILY_TOKEN_LIMIT",
                0,
                int,
                "每人每日 token",
                "每位用户每天可用的 token 上限，0 为不限制",
                unit="tokens",
            ),
            _config(
                "额度与安全",
                "COST_GOLD",
                0,
                int,
                "每次消耗金币",
                "每次对话扣除的金币，0 为免费",
                unit="金币",
            ),
            _config(
                "额度与安全",
                "USER_RATE_LIMIT",
                10,
                int,
                "每人每分钟次数",
                "每位用户每分钟最多唤醒的次数，超出时只提示一次；0 为不限制，"
                "超级用户不受限制",
                unit="次",
            ),
            _config(
                "额度与安全",
                "SESSION_RATE_LIMIT",
                30,
                int,
                "每个会话每分钟次数",
                "每个群或私聊每分钟最多回复的次数，超出的排队等待，"
                "排队超过 10 条时丢弃；"
                "0 为不限制",
                unit="次",
            ),
            _config(
                "额度与安全",
                "SAFETY_WORDS",
                [],
                list[str],
                "安全词",
                "用户消息命中时不调用 AI，AI 回复命中时按“安全词处理”处理；"
                "写成 /…/ 为正则",
            ),
            _config(
                "额度与安全",
                "SAFETY_MODE",
                "block",
                str,
                "安全词处理",
                "AI 回复命中安全词时的处理方式",
                options=_options(block="整条拦截", mask="命中部分替换为 *"),
            ),
            _config(
                "知识库与角色识别",
                "KNOWLEDGE_BASES",
                [],
                list[int],
                "全局知识库",
                "没有单独设置的群和私聊使用的知识库 id，在“知识库”页管理",
                advanced=True,
            ),
            _config(
                "知识库与角色识别",
                "CHARACTER_SOURCES",
                {},
                dict,
                "角色识别来源",
                "各识别来源的开关与权重，在“角色识别”页调整",
                advanced=True,
            ),
            _config(
                "知识库与角色识别",
                "HF_ENDPOINT",
                "https://hf-mirror.com",
                str,
                "Hugging Face 地址",
                "下载角色识别模型与角色库的地址，可换成镜像",
                advanced=True,
            ),
            _config(
                "Agent 与执行器",
                "DISABLED_SKILLS",
                [],
                list[str],
                "停用的技能",
                "停用的技能名，在“技能”页管理；技能文件位于 "
                "data/ai/skills/<名称>/SKILL.md",
                advanced=True,
            ),
            _config(
                "Agent 与执行器",
                "DSH",
                model_dump(DshSettings()),
                dict,
                "DeepSeek Harness 设置",
                "bin：dsh 路径，留空在 PATH 中查找；"
                "provider / model：所有会话共用的模型；"
                "api_provider：提供 API Key 的服务商，留空用 DeepSeek 预设服务商；"
                "timeout：每轮超时秒数；allow_tools：保留 dsh 本地工具，"
                "只有超级用户的会话能通过审批",
                advanced=True,
                visible_when=_when("AGENT_RUNNER", "dsh"),
            ),
        ],
    ).to_dict(),
)


async def _rule(bot: Bot, event: Event, message: UniMsg, state: T_State) -> bool:
    if not message:
        return False
    deps = NoneBotDeps(bot=bot, event=event)
    group_id = ContextUtils.extract_group_id(deps)
    line = None
    if group_id:
        user_id = ContextUtils.extract_user_id(deps) or ""
        if await AIChatManager.blocked(bot, group_id, user_id):
            return False
        message_id = getattr(event, "message_id", None)
        line = ChatLine(
            str(message_id) if message_id is not None else None,
            user_id,
            sender_name(event, user_id),
            summarize(message),
            time.time(),
        )
        ChatBuffer.record(bot.self_id, group_id, line)
        if AtWaiter.feed(bot.self_id, group_id, user_id, message, line.message_id):
            return False
    if await AIChatManager.should_reply(event, message, group_id):
        return True
    if not group_id or not line or not line.text:
        return False
    Proactive.observe(bot.self_id, group_id, line)
    if await Proactive.roll(bot, group_id):
        state[PROACTIVE_KEY] = True
        return True
    return False


_chat_matcher = on_message(rule=_rule, priority=999, block=False)
_stop_matcher = on_alconna(
    Alconna("停止对话"), aliases={"/停止对话"}, priority=5, block=True
)
_reset_matcher = on_alconna(
    Alconna("重置对话"), aliases={"/重置对话"}, priority=5, block=True
)


@Bot.on_called_api
async def _record_bot_send(
    bot: Bot, exception: Exception | None, api: str, data: dict[str, Any], result: Any
):
    if exception is not None:
        return
    if api == "post_group_messages" and (openid := data.get("group_openid")):
        group_id = f"qq_api:{bot.self_id}:group:{openid}"
        data = {"group_id": group_id, "message": data.get("content") or ""}
    elif api not in ("send_msg", "send_group_msg"):
        return
    group_id = data.get("group_id")
    if group_id and await AIChatManager.blocked(bot, str(group_id), None):
        return
    ChatBuffer.record_bot_send(bot.self_id, data, result)


@_chat_matcher.handle()
async def _(bot: Bot, event: Event, message: UniMsg, state: T_State):
    await AIChatManager.chat(
        bot, event, message, proactive=bool(state.get(PROACTIVE_KEY))
    )


@_stop_matcher.handle()
async def _(bot: Bot, event: Event):
    deps = NoneBotDeps(bot=bot, event=event)
    count = await AIChatManager.stop(
        AIChatManager.session_id(bot, event), ContextUtils.extract_user_id(deps)
    )
    await MessageUtils.build_message(
        "已停止你发起的对话。" if count else "你当前没有进行中的对话。"
    ).finish(reply_to=quote_reply(ContextUtils.extract_group_id(deps)))


@_reset_matcher.handle()
async def _(bot: Bot, event: Event, session: Uninfo):
    group_id = ContextUtils.extract_group_id(NoneBotDeps(bot=bot, event=event))
    quote = quote_reply(group_id)
    if (
        group_id
        and not PlatformUtils.is_superuser(bot, session.user.id)
        and not await PlatformUtils.is_group_admin(bot, session)
    ):
        await MessageUtils.build_message(
            "群聊的对话记忆是全群共用的，只有群管理员可以重置。"
        ).finish(reply_to=quote)
    await AIChatManager.reset(AIChatManager.session_id(bot, event))
    await MessageUtils.build_message(
        "已清空本群的对话记忆。" if group_id else "已清空你的对话记忆。"
    ).finish(reply_to=quote)


register_task_job()


@PriorityLifecycle.on_startup(
    priority=90,
    stage="warmup",
    timeout=60,
    component_id="warmup:ai_chat_persona_upgrade",
    depends_on=("management:database",),
    failure_policy="degrade",
)
async def _():
    await AIChatManager.upgrade_personas()


@PriorityLifecycle.on_shutdown(
    priority=60, timeout=15, component_id="runtime:ai_chat_dsh"
)
async def _():
    await DshRuntime.close()


@scheduler.scheduled_job("cron", hour=4, minute=50, id="ai_chat_token_cleanup")
async def _():
    deleted = await AITokenUsage.cleanup()
    if deleted:
        logger.info(f"已清理 {deleted} 条过期的 AI 对话用量记录", MODULE)
    deleted = await AIAgentCall.cleanup() + await AIAgentRun.cleanup()
    if deleted:
        logger.info(f"已清理 {deleted} 条过期的 Agent 运行记录", MODULE)
