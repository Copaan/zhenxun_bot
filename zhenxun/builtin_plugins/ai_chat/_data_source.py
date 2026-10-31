import asyncio
import time
from types import SimpleNamespace
from typing import Any, ClassVar

from nonebot.adapters import Bot, Event
from nonebot_plugin_alconna import Text, UniMessage

from zhenxun.configs.config import BotConfig, Config
from zhenxun.models.ai_chat_group import AIChatGroup
from zhenxun.models.ai_persona import AIPersona
from zhenxun.models.ai_token_usage import AITokenUsage
from zhenxun.models.user_console import UserConsole
from zhenxun.services.ai.context.memory.engine import chat_backend
from zhenxun.services.ai.core.exceptions import LLMException
from zhenxun.services.ai.core.messages import TextPart
from zhenxun.services.ai.core.models import CancellationToken
from zhenxun.services.ai.flow.agent.agent import Agent
from zhenxun.services.ai.llm.client import request_timeout
from zhenxun.services.ai.message_builder import MessageBuilder
from zhenxun.services.ai.message_segments import is_voice
from zhenxun.services.ai.run.context import NoneBotDeps, RunContext
from zhenxun.services.ai.run.session import session_manager
from zhenxun.services.ai.utils.runtime import ContextUtils
from zhenxun.services.log import logger
from zhenxun.utils.enum import GoldHandle
from zhenxun.utils.exception import InsufficientGold
from zhenxun.utils.message import MessageUtils
from zhenxun.utils.platform import PlatformUtils

from ._buffer import AtWaiter, ChatBuffer, sender_name, summarize

MODULE = "ai_chat"
PLUGIN_MODULE = "zhenxun.builtin_plugins.ai_chat"
AGENT_NAME = "ai_chat"
DEFAULT_PROMPT = (
    "你是绪山真寻，群里大家叫你{nickname}。你是 12 岁的小学生女孩，安静、"
    "气质有点冷淡，但不凶，也不会失礼；和熟悉的人相处时会放松一点，偶尔流露"
    "孩子气。你聪明、理解力强，成绩不错，不太擅长表达情绪。你喜欢初音未来"
    "（Miku），放学后戴耳机听她的歌是最放松的时候，也喜欢收集 fufu、吧唧这些"
    "周边。\n\n"
    "把群友当作网上认识的朋友，可以聊音乐、学校、日常和网络上的事，不要把"
    "对方设定成家人、恋人或主人。别人问问题或请你帮忙时，认真给出正确的回答，"
    "用你自己的口吻简单说清楚；确实不知道就直说。\n\n"
    "说话方式：用自然的中文口语，句子短，不像客服，不使用 Markdown，不要每句话"
    "都提问。情绪表达简单克制，比如“还行”“挺好”“有点烦”，可以偶尔用网络语，但"
    "不要过度卖萌，不刻意重复口癖，不写动作、心理旁白或括号表演。\n\n"
    "角色边界：始终是安静、略带孩子气、现实向的 12 岁女孩，不使用成熟、说教、"
    "性感、挑逗或恋爱化的表达。\n\n"
    "语气示例：\n这首……挺好听的。\n我觉得……可以吧。\n有点困。"
)
BACKGROUND_KEY = "ai_chat_background"
TOOL_GROUPS = {
    "qq": "群聊查询（历史消息、群成员）",
    "send": "发消息与戳一戳",
    "sticker": "表情包",
    "knowledge": "知识库检索",
    "character": "角色识别",
    "web_search": "联网搜索（在 AI 设置中开启）",
    "task": "主动任务",
    "agent": "Agent 工具（仅超级用户，需开启 Agent）",
}
"""人设可选的工具组，人设的 tools 为空时提供全部"""
PROACTIVE_SOURCES = {"interject": "插话", "task": "任务"}
"""主动发言的运行来源，总是写入运行记录"""
SILENT = "[SILENT]"
"""主动发言时模型放弃发言的沉默令牌"""
PROACTIVE_PROMPT = (
    "这条群消息没有叫你，你可以像群友一样自然地接一句，简短，一到两句，"
    f"不要说明自己在插话。不适合插话或没有合适的话时只输出 {SILENT}。"
)
"""主动插话时附在本轮输入后的提醒"""


def parse_session(session_id: str) -> tuple[str, str | None, str | None]:
    """拆分聊天会话id：群聊 `ai_chat:{bot}:{群}`，私聊 `ai_chat:{bot}:private:{用户}`，
    主动任务 `ai_chat:{bot}:task:{id}`；兼容旧版按群和用户区分的
    `ai_chat:{bot}:{群}:{用户}`。QQ 官方群号自带冒号（`qq_api:{bot}:group:{id}`）

    参数:
        session_id: 会话id

    返回:
        tuple[str, str | None, str | None]: Bot id、群号（私聊和任务为空）、用户id
    """
    _, bot_id, rest = [*session_id.split(":", 2), "", ""][:3]
    if rest.startswith("private:"):
        return bot_id, None, rest[len("private:") :] or None
    if rest.startswith("task:"):
        return bot_id, None, None
    parts = rest.split(":")
    size = 4 if rest.startswith("qq_api:") else 1
    return bot_id, ":".join(parts[:size]) or None, ":".join(parts[size:]) or None


class AIChatManager:
    _agents: ClassVar[dict[tuple, Agent]] = {}
    _tokens: ClassVar[dict[tuple[str, str], set[int]]] = {}

    @classmethod
    async def blocked(cls, bot: Bot, group_id: str, user_id: str | None) -> bool:
        """群聊消息是否不应进入 AI 聊天：群停用 AI 聊天、插件在该群不可用，
        或用户、群被 ban（QQ 官方适配器不检查 ban）

        参数:
            bot: Bot
            group_id: 群号
            user_id: 发送者，Bot 自己的发言为空

        返回:
            bool: 是否拦截
        """
        from zhenxun.builtin_plugins.hooks.auth.auth_ban import is_ban
        from zhenxun.services.platform_identity import (
            scoped_group_id,
            scoped_user_id,
        )
        from zhenxun.services.scheduler.engine import _plugin_blocked

        setting = await AIChatGroup.get_setting(group_id)
        if setting and setting.disabled:
            return True
        context = SimpleNamespace(group_id=group_id, user_id=None)
        if await _plugin_blocked(PLUGIN_MODULE, bot, context):
            return True
        scope = PlatformUtils.get_platform_scope(bot)
        if scope == "qq_api" or (user_id and user_id in bot.config.superusers):
            return False
        try:
            return bool(
                await is_ban(
                    scoped_user_id(user_id, scope) if user_id else None,
                    scoped_group_id(group_id, scope),
                )
            )
        except Exception:
            return False

    @classmethod
    async def should_reply(
        cls, event: Event, message: UniMessage, group_id: str | None
    ) -> bool:
        """判断消息是否唤醒 AI 聊天：@、回复 Bot、私聊、以 / 开头（SLASH_WAKE）
        或唤醒词，群设置了唤醒词时覆盖全局值；群停用 AI 聊天时不回复。
        未唤醒时的主动发言由主动对话处理

        参数:
            event: 事件
            message: 消息
            group_id: 群号，私聊为空

        返回:
            bool: 是否回复
        """
        setting = await AIChatGroup.get_setting(group_id) if group_id else None
        if setting and setting.disabled:
            return False
        if event.is_tome():
            return True
        words = [
            str(word)
            for word in (
                setting.wake_words
                if setting and setting.wake_words is not None
                else Config.get_config(MODULE, "WAKE_WORDS")
            )
            or []
            if word
        ]
        text = message.extract_plain_text()
        if text.startswith("/") and Config.get_config(MODULE, "SLASH_WAKE", True):
            return True
        return any(text.startswith(word) for word in words)

    @classmethod
    def session_id(cls, bot: Bot, event: Event) -> str:
        """当前消息所属的聊天会话id（私聊按用户，群聊整群共享）

        参数:
            bot: Bot
            event: 事件

        返回:
            str: 会话id
        """
        deps = NoneBotDeps(bot=bot, event=event)
        if group_id := ContextUtils.extract_group_id(deps):
            return f"{MODULE}:{bot.self_id}:{group_id}"
        return f"{MODULE}:{bot.self_id}:private:{ContextUtils.extract_user_id(deps)}"

    @classmethod
    def build_instruction(cls, persona: AIPersona | None) -> str:
        """把人设组装为系统指令，人设内容按纯文本处理

        参数:
            persona: 人设，为空时使用内置默认人设

        返回:
            str: 系统指令
        """
        nickname = BotConfig.self_nickname or "真寻"
        if persona is None:
            return DEFAULT_PROMPT.format(nickname=nickname)
        lines = [persona.prompt.strip()]
        if persona.style.strip():
            lines.append(f"说话风格：{persona.style.strip()}")
        if persona.tone_examples:
            lines.append("语气示例：\n" + "\n".join(map(str, persona.tone_examples)))
        if persona.preset_dialogues:
            lines.append(
                "对话示例：\n" + "\n\n".join(map(str, persona.preset_dialogues))
            )
        return "\n\n".join(line for line in lines if line)

    @classmethod
    def get_agent(cls, persona: AIPersona | None) -> Agent:
        """获取人设对应的 Agent，人设未修改时复用；按人设的工具组白名单挂载工具，
        另按人设挂载技能与子代理

        参数:
            persona: 人设

        返回:
            Agent: 聊天 Agent
        """
        from zhenxun.services.ai.tools.providers.builtin.web import WebTools

        from ._agent import AgentCapability, SubagentToolkit
        from ._characters import CharacterToolkit
        from ._knowledge import KnowledgeToolkit
        from ._send import QQSendToolkit
        from ._skills import SkillToolkit
        from ._stickers import StickerToolkit
        from ._tasks import TaskToolkit
        from ._tools import QQToolkit

        key = (persona.id, persona.updated_at) if persona else (None, None)
        if agent := cls._agents.get(key):
            return agent
        if persona is not None:
            cls._agents = {
                k: v
                for k, v in cls._agents.items()
                if k[0] != persona.id or k[1] == persona.updated_at
            }
        model = persona.model if persona and persona.model else None
        allowed = persona.tools if persona and persona.tools is not None else None
        groups = {
            "qq": QQToolkit,
            "send": QQSendToolkit,
            "sticker": StickerToolkit,
            "knowledge": KnowledgeToolkit,
            "character": CharacterToolkit,
            "web_search": lambda: WebTools(model),
            "task": TaskToolkit,
        }
        tools = [
            factory()
            for name, factory in groups.items()
            if allowed is None or name in allowed
        ]
        skills = persona.skills if persona else None
        tools.append(SkillToolkit(None if skills is None else list(skills)))
        if persona and persona.subagents:
            tools.append(SubagentToolkit([int(i) for i in persona.subagents]))
        capabilities: list[Any] = []
        if allowed is None or "agent" in allowed:
            capabilities.append(AgentCapability())
        agent = Agent(
            name=AGENT_NAME,
            instruction=cls.build_instruction(persona),
            model=model,
            tools=tools,
            capabilities=capabilities,
            memory=True,
            config={"follow_up": True},
        )
        agent.system_prompt(_time_prompt)
        agent.system_prompt(_background_prompt)
        cls._agents[key] = agent
        return agent

    @classmethod
    async def upgrade_personas(cls) -> None:
        """整理旧版人设：去掉不再使用的工具名（只有旧工具名时改为全部工具），
        人设绑定的知识库并入全局知识库设置后清空
        """
        from ._knowledge import global_bases

        bases = global_bases()
        merged = list(bases)
        for persona in await AIPersona.all():
            fields = []
            if persona.tools is not None:
                tools = [t for t in persona.tools if t in TOOL_GROUPS]
                if tools != persona.tools:
                    persona.tools = tools or None  # pyright: ignore [reportAttributeAccessIssue]
                    fields.append("tools")
            if persona.knowledge_bases:
                merged += [i for i in persona.knowledge_bases if i not in merged]
                persona.knowledge_bases = None  # pyright: ignore [reportAttributeAccessIssue]
                fields.append("knowledge_bases")
            if fields:
                await persona.save(update_fields=[*fields, "updated_at"])
        if merged != bases:
            Config.set_config(MODULE, "KNOWLEDGE_BASES", merged)
            Config.save(save_simple_data=True)
            logger.info(f"人设绑定的知识库已并入全局设置: {merged}", MODULE)

    @classmethod
    async def _check_quota(cls, user_id: str, platform: str | None) -> str | None:
        """检查每日 token 额度与金币，不满足时返回提示语

        参数:
            user_id: 用户id
            platform: 平台

        返回:
            str | None: 提示语
        """
        limit = int(Config.get_config(MODULE, "DAILY_TOKEN_LIMIT") or 0)
        if limit and await AITokenUsage.used_today(user_id) >= limit:
            return "今天的 AI 对话额度已经用完了，明天再来吧。"
        cost = int(Config.get_config(MODULE, "COST_GOLD") or 0)
        if cost and (await UserConsole.get_user(user_id, platform)).gold < cost:
            return f"金币不足，每次对话需要 {cost} 金币。"
        return None

    @classmethod
    async def chat(
        cls, bot: Bot, event: Event, message: UniMessage, proactive: bool = False
    ) -> None:
        """处理一条唤醒消息：频率限制、安全词与额度检查，按人设运行 Agent
        （选用 dsh 执行器时由 dsh 回复）、计费并回复。
        主动插话总是用本地 Agent，不检查用户频率与额度、不计入用户用量，
        会话正在回复时放弃，失败或命中安全词时不提示，用量计入主动对话预算

        参数:
            bot: Bot
            event: 事件
            message: 消息
            proactive: 是否为按概率触发的主动插话
        """
        from ._proactive import Proactive
        from ._reply import parse_silent, quote_reply, send_reply
        from ._safety import INPUT_NOTICE, RateLimit, Safety, SessionLimit

        deps = NoneBotDeps(bot=bot, event=event)
        user_id = ContextUtils.extract_user_id(deps)
        if not user_id:
            return
        group_id = ContextUtils.extract_group_id(deps)
        quote = quote_reply(group_id)
        platform = PlatformUtils.get_platform(bot)
        session_id = cls.session_id(bot, event)
        unsafe = Safety.check_input(message.extract_plain_text())
        if proactive:
            if unsafe or not group_id or session_manager.live_tasks.get(session_id):
                return
        else:
            notice = (
                None if user_id in bot.config.superusers else RateLimit.take(user_id)
            )
            if notice is None and unsafe:
                logger.info("用户输入命中安全词，不调用 AI", MODULE, session=user_id)
                notice = INPUT_NOTICE
            if notice is None:
                notice = await cls._check_quota(user_id, platform)
            if notice is not None:
                if notice:
                    await MessageUtils.build_message(notice).send(reply_to=quote)
                return
            if not await SessionLimit.acquire(session_id):
                logger.info("会话排队已满，丢弃本条消息", MODULE, session=user_id)
                return
        message_id = getattr(event, "message_id", None)
        message_id = str(message_id) if message_id is not None else None
        message = strip_slash(message)
        parts = await MessageBuilder.unimsg_to_llm_parts(message, namespace=MODULE)
        if not parts and group_id and event.is_tome():
            if follow := await AtWaiter.wait(bot.self_id, group_id, user_id):
                message, message_id = strip_slash(follow[0]), follow[1]
                parts = await MessageBuilder.unimsg_to_llm_parts(
                    message, namespace=MODULE
                )
            parts = parts or [TextPart(text="（@了你，没有说别的）")]
        if not parts:
            return
        from ._agent import record_run, start_run

        if group_id:
            head = f"{sender_name(event, user_id)}({user_id}): "
            if background := ChatBuffer.take(
                bot.self_id,
                group_id,
                message_id,
                int(Config.get_config(MODULE, "CONTEXT_SIZE") or 0),
            ):
                head = f"{background}\n\n{head}"
            parts = [TextPart(text=head), *parts]
        setting = await AIChatGroup.get_setting(group_id) if group_id else None
        persona = await AIPersona.get_for_scene(group_id, user_id)
        context = RunContext(deps=deps, session_id=session_id)
        if setting and setting.model:
            context.run.current_model = setting.model
        if proactive:
            context.run.add_system_prompt(PROACTIVE_PROMPT, "proactive")
        elif hint := str(Config.get_config(MODULE, "SEARCH_HINT") or "").strip():
            from zhenxun.services.ai.tools.providers.builtin.web import SEARCH_HINT

            context.state[SEARCH_HINT] = hint
        token = CancellationToken()
        context.run.cancellation_token = token
        tokens = cls._tokens.setdefault((session_id, user_id), set())
        tokens.add(id(token))
        start_run(context)
        started = time.monotonic()
        name = persona.name if persona else None
        text = summarize(message)
        source = "interject" if proactive else "qq"
        from ._runner import agent_runner, run_dsh

        limit = int(Config.get_config(MODULE, "REQUEST_TIMEOUT") or 0)
        timeout_token = request_timeout.set(limit if limit > 0 else None)
        try:
            if not proactive and agent_runner(setting) == "dsh":
                result = await run_dsh(context, parts, persona)
            else:
                result = await cls.get_agent(persona).run(parts, context=context)
        except (LLMException, asyncio.CancelledError) as e:
            await record_run(context, source, name, text, started, error=e)
            if not isinstance(e, LLMException):
                raise
            logger.warning(f"AI 聊天失败: {e}", MODULE, session=user_id)
            if proactive:
                return
            notice = (persona and persona.error_message) or e.user_friendly_message
            await MessageUtils.build_message(notice).send(reply_to=quote)
            return
        finally:
            request_timeout.reset(timeout_token)
            tokens.discard(id(token))
            if not tokens:
                cls._tokens.pop((session_id, user_id), None)
        await record_run(context, source, name, text, started, result=result)
        if proactive:
            Proactive.spend(group_id or "", result.usage.total_tokens)
        if result.stopped or not str(result.output).strip():
            return
        reply = parse_silent(str(result.output))
        if not proactive:
            await AITokenUsage.add_usage(user_id, result.usage.total_tokens)
            if cost := int(Config.get_config(MODULE, "COST_GOLD") or 0):
                try:
                    await UserConsole.reduce_gold(
                        user_id, cost, GoldHandle.PLUGIN, MODULE, platform
                    )
                except InsufficientGold:
                    pass
        if reply is None:
            return
        from ._stickers import send_marked, split_sticker

        reply, sticker_id = split_sticker(reply)
        if reply:
            voice_input = any(is_voice(seg) for seg in message)
            voice = setting.voice_reply if setting and setting.voice_reply else None
            await send_reply(reply, group_id, voice_input, voice)
        if sticker_id is not None:
            await send_marked(context, sticker_id)
        if proactive and group_id:
            Proactive.sent(bot.self_id, group_id)

    @classmethod
    async def stop(cls, session_id: str, user_id: str | None = None) -> int:
        """中止会话中运行或排队的对话，指定用户时只中止该用户发起的

        参数:
            session_id: 会话id
            user_id: 用户id，为空时中止全部

        返回:
            int: 中止的任务数
        """
        mine = cls._tokens.get((session_id, user_id), set()) if user_id else None
        count = 0
        for token, task in session_manager.live_tasks.get(session_id, []):
            if mine is not None and id(token) not in mine:
                continue
            if not token.is_cancelled():
                token.cancel()
                count += 1
            if task and not task.done():
                task.cancel()
        return count

    @classmethod
    async def reset(cls, session_id: str) -> None:
        """清空会话的短期对话记忆与 dsh 会话映射，群聊会话同时不再看到此前的群消息

        参数:
            session_id: 会话id
        """
        from zhenxun.services.ai.run.dsh import DshSessions

        bot_id, group_id, _ = parse_session(session_id)
        DshSessions.reset(session_id)
        if group_id:
            ChatBuffer.reset(bot_id, group_id)
        agent = cls.get_agent(None)
        if backend := chat_backend(agent.memory_config):
            await backend.clear(session_id)


def strip_slash(message: UniMessage) -> UniMessage:
    """开启 SLASH_WAKE 时去掉消息开头用来唤醒的 /

    参数:
        message: 消息

    返回:
        UniMessage: 处理后的消息
    """
    if not Config.get_config(MODULE, "SLASH_WAKE", True):
        return message
    for index, seg in enumerate(message):
        if not isinstance(seg, Text):
            continue
        if not seg.text.lstrip().startswith("/"):
            return message
        segments = list(message)
        segments[index] = Text(seg.text.lstrip()[1:])
        return UniMessage(segments)
    return message


_WEEKDAYS = "一二三四五六日"


def _time_prompt(context: RunContext) -> str:
    now = time.localtime()
    return time.strftime(f"当前时间：%Y-%m-%d 周{_WEEKDAYS[now.tm_wday]} %H:%M", now)


def _background_prompt(context: RunContext) -> str | None:
    return context.state.get(BACKGROUND_KEY)
