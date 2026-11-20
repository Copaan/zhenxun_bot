"""确定性的聊天控制入口，不通过模型解释或执行管理命令。"""

from datetime import date
import hashlib
import json
import time

from tortoise.functions import Sum

from zhenxun.models.ai_chat_conversation import AIChatConversation
from zhenxun.models.ai_chat_group import AIChatGroup
from zhenxun.models.ai_persona import AIPersona
from zhenxun.services.ai.run.context import NoneBotDeps, RunContext
from zhenxun.services.ai.utils.runtime import PermissionUtils

from ._agent import bind_source
from ._data_source import AIChatManager
from ._sessions import Conversations

# 名称、用法、权限范围、独立简写。
COMMANDS = (
    ("sid", "/sid · /ai status", "all", True),
    ("stop", "/stop [运行ID]", "all", True),
    ("reset", "/reset（群管理员或超管／本人私聊）", "all", True),
    ("new", "/new [名称]", "history", True),
    ("ls", "/ls [页码] [--archived]", "all", True),
    ("switch", "/switch <编号或ID>", "history", True),
    ("archive", "/archive", "history", True),
    ("rename", "/rename <名称>", "local_history", True),
    ("unarchive", "/unarchive <编号或ID>", "local_history", True),
    ("del", "/del", "local_history", True),
    ("history", "/history [页码] · /stats", "all", True),
    ("stats", "", "all", True),
    (
        "provider",
        "/provider [chat|tts|stt] [编号或名称|default] · --inherit",
        "admin",
        True,
    ),
    (
        "model",
        "/model [chat|tts|stt] [编号|模型全名|group:组名|default]",
        "admin",
        True,
    ),
    ("persona", "/persona list|view <名称>|<名称>|unset", "admin", True),
    ("llm", "/llm status|on|off|default", "admin", True),
    ("runner", "/ai runner status（全局切换在 WebUI）", "admin", False),
    ("agent", "/ai agent status|on|off", "admin", False),
    (
        "memory",
        "/ai memory list|search <词>|add <内容>|del <ID>|organize",
        "all",
        False,
    ),
    ("proactive", "/ai proactive status|on|off", "admin", False),
    ("task", "/ai task list|show|pause|resume|run|del <ID>", "all", False),
    (
        "dsh",
        "/ai dsh status|ls|new|switch <ID>|reset|archive|model|thinking|mode",
        "dsh",
        False,
    ),
    ("approval", "/ai approval · /ai answer <问题ID> <回答>", "all", False),
)
ALIASES = {name for name, _, _, alias in COMMANDS if alias}
SUBCOMMANDS = {name for name, *_ in COMMANDS} | {"help", "status", "answer"}
AGENT_PRIVATE = "Agent 会话在私聊和各群之间共用，请私聊管理，避免在群内暴露桌面内容。"


class CommandDenied(ValueError):
    """权限不足；群聊中未 @ Bot 的控制命令被拒绝时不回复"""


async def command_help(context: RunContext, runner: str) -> str:
    """按当前可信身份、场景和执行器展示已提供的控制命令。"""
    admin = await PermissionUtils.check_superuser(context)
    history = admin or not context.get_group_id()
    allowed = {
        "all": True,
        "admin": admin,
        "history": history,
        "local_history": history and runner == "local",
        "dsh": admin and runner == "dsh",
    }
    dsh_usage = {"new": "/new", "ls": "/ls", "switch": "/switch <会话ID>"}
    lines = [
        dsh_usage.get(name, text) if runner == "dsh" else text
        for name, text, scope, _ in COMMANDS
        if text and allowed[scope]
    ]
    lines.append("/ai task stop <计划ID> <运行ID>；只操作有权限的计划。")
    return "\n".join(
        [
            "AI 控制（独立简写冲突时可使用 /ai 前缀）",
            *lines,
            f"当前执行器：{runner}。执行器全局生效，切换历史不更换执行器。",
            "/new 从新背景边界开始，旧历史和长期记忆保留。",
            "群内会话命令始终管理本群普通对话；个人 Agent 请在私聊或控制台管理。",
            "停止只针对本人从当前入口发起的运行。",
            "DSH 原生选择不被本地模型覆盖；不支持的远端操作会明确拒绝。"
            if runner == "dsh"
            else "模型、人设与执行器修改限超管。",
            "/llm off 会跳过目标场景的 AI 计划，不删除计划或关闭原生宿主。",
        ]
    )


_lists: dict[tuple, tuple[float, str, list[str]]] = {}


def command_text(text: str) -> str | None:
    """解析独立 AI 入口，已有插件占用的简写不接管。"""
    parts = text.strip().split(maxsplit=1)
    if not parts:
        return None
    head, rest = parts[0], parts[1] if len(parts) > 1 else ""
    if head == "/ai":
        sub = rest.split(maxsplit=1)[0] if rest.strip() else "help"
        return (rest.strip() or "help") if sub in SUBCOMMANDS else None
    if not head.startswith("/") or head[1:] not in ALIASES:
        return None
    from arclet.alconna import command_manager
    from nonebot.rule import TrieRule

    if head in TrieRule.prefix:
        return None
    for command in command_manager.get_commands():
        name = str(command.command)
        if head == name or head in {str(p) + name for p in command.prefixes}:
            return None
    return head[1:] + (" " + rest if rest else "")


def select_number(key: tuple, items: list[tuple[str, str]], value: str | None):
    """编号只在同一主体读过且内容未变化的列表中有效。"""
    fingerprint = hashlib.sha256(
        json.dumps(items, ensure_ascii=False).encode()
    ).hexdigest()
    now = time.monotonic()
    for old, entry in list(_lists.items()):
        if entry[0] < now - 300:
            _lists.pop(old, None)
    if value is None:
        if len(_lists) >= 512:
            _lists.pop(next(iter(_lists)))
        _lists[key] = (now, fingerprint, [i[0] for i in items])
        return (
            "\n".join(
                f"{i}. {label} [{ident}]" for i, (ident, label) in enumerate(items, 1)
            )
            or "暂无项目。"
        )
    if value.isdigit():
        cached = _lists.get(key)
        if (
            not cached
            or cached[1] != fingerprint
            or not 1 <= int(value) <= len(cached[2])
        ):
            raise ValueError("列表已变化或编号失效，请先重新读取列表。")
        return cached[2][int(value) - 1]
    matches = [
        ident
        for ident, label in items
        if value in {ident, label, label.removesuffix("（当前）")}
    ]
    if len(matches) != 1:
        raise ValueError("没有唯一匹配项，请先读取列表并使用完整 ID。")
    return matches[0]


async def conversation_pairs(
    scene: str, current: str | None, archived: bool
) -> list[tuple[str, str]]:
    """场景内的本地对话列表

    参数:
        scene: 场景标识
        current: 当前对话 ID
        archived: 是否列出已归档对话

    返回:
        list[tuple[str, str]]: 对话 ID 与名称，当前对话带标记
    """
    items = await AIChatConversation.filter(scene_id=scene, archived=archived).order_by(
        "id"
    )
    return [
        (i.session_id, i.name + ("（当前）" if i.session_id == current else ""))
        for i in items
    ]


async def require_admin(context: RunContext) -> None:
    if not await PermissionUtils.check_superuser(context):
        raise CommandDenied("此管理操作仅限真寻超级用户。")


async def require_scene(context: RunContext, *, reset: bool = False) -> None:
    if context.get_group_id() and not (
        await PermissionUtils.check_superuser(context)
        or (reset and await PermissionUtils.check_admin_level(context, 1))
    ):
        raise CommandDenied(
            "群对话由全群共享，此操作需要" + ("群管理员或超管。" if reset else "超管。")
        )


async def stop(context: RunContext, run_id: str = "") -> str:
    """按发起人和场景停止运行；指定运行时不误停后来的请求。"""
    from zhenxun.services.ai.run.dsh import DshSessions
    from zhenxun.services.ai.run.dsh_control import DshControl
    from zhenxun.services.ai.run.session import session_manager

    from ._runner import dsh_settings

    caller = context.run.caller

    def within(active):
        other = active.run.caller
        return (
            other
            and other.source == "qq"
            and (other.platform, other.bot_id, other.group_id)
            == (caller.platform, caller.bot_id, caller.group_id)
            and other.user_id == caller.user_id
        )

    captured = [
        (ident, active)
        for ident, (active, task) in list(session_manager.runs.items())
        if (not run_id or ident == run_id)
        and task
        and not task.done()
        and within(active)
    ]
    if run_id in session_manager.runs and not captured:
        raise ValueError("不能停止不属于当前入口的运行。")
    completed = [await session_manager.stop(ident) for ident, _ in captured]
    states = [
        active.run.operations.get("dsh", {}).get("stop", "unknown")
        for _, active in captured
        if active.state.get("_dsh_binding")
    ]
    settings = dsh_settings()
    records = DshSessions.for_settings(settings)
    owners = {records.owner(context, "chat")}
    if await PermissionUtils.check_superuser(context):
        owners.add(records.owner(context, "agent"))
    captured_ids = {ident for ident, _ in captured}
    for item in records._read()["owners"].values():
        flight = item.get("inflight") or {}
        if (
            item["owner"] not in owners
            or not flight
            or flight.get("run_id") in captured_ids
        ):
            continue
        if run_id and flight.get("run_id") != run_id:
            continue
        from zhenxun.models.ai_agent_run import AIAgentRun

        row = await AIAgentRun.get_or_none(run_id=flight.get("run_id"))
        if (
            not row
            or row.source != "qq"
            or row.group_id != caller.group_id
            or str(row.user_id) != caller.user_id
        ):
            continue
        result = await DshControl.stop_owned(settings, dict(item), flight.get("run_id"))
        states.append(result["state"])
    if captured or states:
        return stop_message(states, all(completed))
    return "没有当前权限范围内的登记运行；未对无归属的原生活动执行停止。"


def stop_message(states: list[str], local_done: bool) -> str:
    """分别说明本地收尾与原生前台停止证据。"""
    if not local_done:
        return "停止中，清理尚未完成；请重新查询状态。"
    if any(
        state
        not in {
            "confirmed",
            "foreground_confirmed",
            "foreground_idle",
            "target_removed",
        }
        for state in states
    ):
        return "本地停止请求已处理；远端执行结果待核验，未宣称已停止。"
    if states:
        return "所属远端前台请求已停止或已移除；不代表其他原生后台任务结束。"
    return "已停止对应本地运行并完成收尾。"


async def execute(bot, event, text: str) -> str:
    """以真实发送者执行一条控制命令，错误不交给模型处理。"""
    context = RunContext(deps=NoneBotDeps(bot=bot, event=event))
    bind_source(context, "qq")
    scene = AIChatManager.session_id(bot, event)
    pieces = text.strip().split(maxsplit=1)
    command = pieces[0] if pieces else "help"
    argument = pieces[1].strip() if len(pieces) > 1 else ""
    if command in {"reset", "del", "archive"} and argument:
        raise ValueError("此操作只针对当前对话，不接受额外目标参数。")
    if command == "stop":
        return await stop(context, argument)
    if command == "agent":
        await require_admin(context)
        if argument not in {"", "status", "on", "off"}:
            raise ValueError("用法：/ai agent on|off|status")
        personal = await Conversations.personal(
            context, scene, {"on": True, "off": False}.get(argument)
        )
        return "本人在当前入口的个人 Agent：" + ("开启" if personal else "关闭")
    personal = await Conversations.personal(context, scene)
    group = context.get_group_id()
    scope = (
        "本群普通对话" if group else "本人个人 Agent" if personal else "本人普通私聊"
    )
    destination = "本人个人 Agent" if personal else scope
    routing = f"下一条定向消息：{destination}；会话命令：{scope}。"
    if personal and not group:
        context.state["_personal_scene"] = personal
        scene = personal
    snapshot = await Conversations.snapshot(scene, create=False)
    context.session_id = snapshot["session_id"]
    context.state["_conversation"] = snapshot
    resolved = context.state["_scene_configuration"] = await Conversations.resolve(
        scene,
        snapshot,
        inspect_policy=command in {"sid", "status", "llm"},
        owner=context.run.caller if personal and not group else None,
    )
    group, user = context.get_group_id(), context.get_user_id()
    setting = await AIChatGroup.get_setting(group) if group else None
    from ._runner import scene_session

    runner = resolved["effective"]["runner"]
    key = (context.run.caller.platform, str(bot.self_id), user, scene)
    if command in {"help", ""}:
        return routing + "\n" + await command_help(context, runner)
    if command in {"sid", "status"}:
        persona = await Conversations.persona(snapshot, group, user)
        AIChatManager.configuration(context, setting, persona, runner)
        active = (
            scene_session(context, snapshot, runner)
            if runner == "dsh" or snapshot["session_id"]
            else "无，请新建或选择已有对话"
        )
        native = ""
        if runner == "dsh":
            from zhenxun.services.ai.run.dsh import DshSessions

            from ._runner import dsh_settings

            mode = "agent" if not group and personal else "chat"
            records = DshSessions.for_settings(dsh_settings())
            item = records._read()["owners"].get(records.owner(context, mode), {})
            model = item.get("model")
            native = (
                f"\nDSH：{dsh_settings().transport} / {mode}"
                f"\n原生选择（上次确认）：{model or '尚未确认，请查询 DSH 状态'}"
            )
        return (
            f"{routing}\n场景：{scene}\n当前对话：{active}\n"
            f"执行器：{runner}\n模型：{context.state['_configuration']['model']}\n"
            f"人设：{persona.name if persona else '内置默认'}\n"
            + (
                "场景已停用"
                if resolved["effective"]["disabled"]
                else "AI 开关已开启，仍受插件启停、封禁与账号权限限制"
            )
            + f"\nTTS：{resolved['effective']['tts'] or '继承工具/未配置'}"
            + f" · STT：{resolved['effective']['stt'] or '继承工具/未配置'}"
            + f"\n覆盖字段：{', '.join(resolved['overrides']) or '无，全部继承'}"
            + (
                "\n阻断原因：" + "；".join(resolved["blockers"])
                if resolved["blockers"]
                else ""
            )
            + native
        )
    if command in {"approval", "answer"}:
        from zhenxun.services.ai.run.hitl import HITLController

        return await HITLController.command(context, command, argument)
    if command == "dsh" and runner != "dsh":
        return "全局执行器为内置，未操作 DSH 会话。"
    if command == "dsh":
        await require_admin(context)
        from ._runner import dsh_command

        return await dsh_command(context, argument)
    if runner == "dsh" and command in {"new", "ls", "switch", "archive", "reset"}:
        if command != "ls":
            await require_scene(context, reset=command == "reset")
        from ._runner import dsh_command

        return (
            scope
            + "：\n"
            + await dsh_command(
                context,
                command + (" " + argument if argument else ""),
                current_mode=True,
            )
        )
    if runner == "dsh" and command in {
        "del",
        "rename",
        "unarchive",
    }:
        return (
            "当前为 DSH；请用 /ai dsh 控制原生会话，"
            "不会修改本地对话或假装删除远端历史。"
        )
    if command == "ls":
        archived = "--archived" in argument.split()
        page = argument.replace("--archived", "").strip() or "1"
        if not page.isdigit() or not 1 <= int(page) <= 10000:
            raise ValueError("页码须为正整数。")
        pairs = await conversation_pairs(scene, snapshot["session_id"], archived)
        listing = select_number((*key, "sessions", archived), pairs, None)
        listing = listing.splitlines()
        start = (int(page) - 1) * 10
        return (
            scope
            + "：\n"
            + ("\n".join(listing[start : start + 10]) or "本页没有对话。")
        )
    if command in {"new", "switch", "rename", "archive", "unarchive", "del", "reset"}:
        await require_scene(context, reset=command == "reset")
        target = None
        if command in {"switch", "unarchive"}:
            archived = command == "unarchive"
            pairs = await conversation_pairs(scene, snapshot["session_id"], archived)
            target = select_number((*key, "sessions", archived), pairs, argument)
        result = await Conversations.change(
            scene,
            {"del": "delete"}.get(command, command),
            target=target,
            name=argument or None,
            expected_revision=snapshot["revision"],
        )
        if command == "new":
            return f"已新建{scope}。旧对话和长期记忆保留。"
        current = result["current_id"] or "无，下次聊天新建"
        return f"{scope}已完成 {command}；当前对话：{current}。长期记忆保留。"
    if command == "history":
        if runner == "local" and snapshot["session_id"] is None:
            return "当前没有活跃对话；请用 /ls 查看并选择已有对话。"
        from zhenxun.models.ai_chat_message import AIChatMessage

        if argument and (not argument.isdigit() or int(argument) < 1):
            raise ValueError("页码须为正整数。")
        rows = (
            await AIChatMessage.filter(
                session_id=scene_session(context, snapshot, runner)
            )
            .order_by("-id")
            .offset((int(argument or 1) - 1) * 10)
            .limit(10)
        )
        # 历史展示遵循送达标记，工具调用不作为普通聊天输出。
        lines = []
        for row in reversed(rows):
            meta = row.metadata or {}
            if row.role not in {"user", "assistant"} or (
                meta.get("chat_run")
                and not (meta.get("chat_settled") and meta.get("chat_visible"))
            ):
                continue
            content = row.content
            value = (
                content
                if isinstance(content, str)
                else "".join(
                    p.get("text", "") for p in content or [] if p.get("type") == "text"
                )
            )
            if value:
                lines.append(f"{row.role}: {value[:800]}")
        return scope + "：\n" + ("\n".join(lines) or "本页没有可显示的已结算消息。")
    if command == "stats":
        from zhenxun.models.ai_agent_run import AIAgentRun
        from zhenxun.models.ai_token_usage import AITokenUsage
        from zhenxun.services.platform_identity import scoped_user_id
        from zhenxun.utils.platform import PlatformUtils

        usage = []
        if runner == "dsh" or snapshot["session_id"]:
            usage = (
                await AIAgentRun.filter(
                    session_id=scene_session(context, snapshot, runner)
                )
                .annotate(total=Sum("tokens"))
                .values("total")
            )
        used = await AITokenUsage.used_today(
            scoped_user_id(user, PlatformUtils.get_platform_scope(bot))
        )
        total = (usage[0]["total"] or 0) if usage else 0
        return (
            f"{scope}已记录总 token：{total}\n"
            f"本人今日（{date.today()}）额度用量：{used}\n"
            "历史输入/输出分项未记录时不可推算。"
        )
    if command in {"provider", "model", "persona", "llm", "runner"}:
        await require_admin(context)
        return await configure(context, snapshot, command, argument, key)
    if command == "memory":
        if runner == "dsh" and argument.strip() == "organize":
            return "DSH 尚未桥接长期整理；本地记忆管理不表示已注入 DSH。"
        if not snapshot["session_id"] and argument.strip() == "organize":
            return "当前没有活跃对话，未启动整理。"
        return await memory_command(context, argument)
    if command == "task":
        return await task_command(context, argument)
    if command == "proactive":
        await require_admin(context)
        if not group:
            raise ValueError("主动插话开关只适用于当前群。")
        value = dict((setting.proactive if setting else None) or {})
        if argument in {"", "status"}:
            return "本群主动对话设置：" + json.dumps(value, ensure_ascii=False)
        if argument not in {"on", "off"}:
            raise ValueError("用法：/ai proactive status|on|off")
        value["mode"] = "probability" if argument == "on" else "off"
        await AIChatGroup.save_setting(group, proactive=value)
        return "已保存本群主动对话设置。"
    return "未识别的 AI 控制命令，未发送给模型。使用 /ai help 查看用法。"


async def configure(context, snapshot, command, argument, key) -> str:
    """修改场景显式覆盖；模型选择按聊天、合成及识别能力隔离。"""

    settings = {}
    resolved = context.state["_scene_configuration"]
    if command == "runner":
        return f"全局执行器：{resolved['effective']['runner']}。仅可在 WebUI 切换。"
    if command == "llm":
        name = "disabled"
        if argument in {"", "status"}:
            value = resolved["effective"][name]
            label = "关闭" if value else "开启"
            return (
                f"{command} 有效值：{label}；来源：{resolved['sources'][name]}。"
                f"范围：{resolved['scope']}。插件启停、封禁和账号权限仍独立生效。"
                + (
                    "阻断原因：" + "；".join(resolved["blockers"])
                    if resolved["blockers"]
                    else ""
                )
            )
        if argument not in {"on", "off", "default"}:
            raise ValueError("有效值：on / off / default")
        settings[name] = None if argument == "default" else argument == "off"
    elif command == "persona":
        people = await AIPersona.filter(enabled=True).order_by("id")
        pairs = [(str(p.id), p.name) for p in people]
        if argument in {"", "list"}:
            return select_number((*key, command), pairs, None)
        if argument.startswith("view "):
            target = argument[5:].strip()
            person = next((p for p in people if target in {str(p.id), p.name}), None)
            if not person:
                raise ValueError("没有此人设。")
            return f"{person.name}\n{person.prompt[:1800]}"
        settings["persona_id"] = (
            None
            if argument == "unset"
            else int(select_number((*key, command), pairs, argument))
        )
    else:
        task, _, rest = argument.partition(" ")
        if task in {"chat", "tts", "stt"}:
            argument = rest.strip()
        else:
            task = "chat"
        if resolved["effective"]["runner"] == "dsh" and task == "chat":
            return "当前使用 DSH 原生模型，请使用 /ai dsh model。"
        field = "model" if task == "chat" else task
        choices = Conversations.models(task)
        pairs = (
            [
                (name, name)
                for name in dict.fromkeys(
                    item["provider"] for item in choices if item["provider"]
                )
            ]
            if command == "provider"
            else [(item["id"], item["label"]) for item in choices]
        )
        if not argument:
            return f"{task} 可选项：\n" + select_number(
                (*key, command, task), pairs, None
            )
        if argument == "--inherit" and command == "provider":
            settings.update(model=None, tts=None, stt=None)
        elif argument == "default":
            settings[field] = None
        else:
            if command == "model" and "/" not in argument and not argument.isdigit():
                named = [
                    ident for ident, _ in pairs if ident.partition("/")[2] == argument
                ]
                if len(named) == 1:
                    argument = named[0]
            selected = select_number((*key, command, task), pairs, argument)
            if command == "provider":
                models = [
                    item["id"] for item in choices if item["provider"] == selected
                ]
                previous = str(resolved["effective"].get(field) or "").partition("/")[2]
                selected = next(
                    (model for model in models if model.partition("/")[2] == previous),
                    models[0],
                )
            settings[field] = selected.removeprefix("group:")
    await Conversations.change(
        snapshot["scene_id"],
        "configure",
        expected_revision=snapshot["revision"],
        settings=settings,
    )
    return (
        "已保存场景设置，聊天与目标场景的 AI 计划共同使用；切换历史不换设置。"
        "语音模型选择不会自动开启语音回复；DSH 原生模型与模式不受本地模型选择覆盖。"
    )


async def memory_command(context, argument: str) -> str:
    from zhenxun.models.ai_memory import AIMemory

    from ._memory import Memory, context_scope, scope_filter
    from ._memory_tools import MemoryToolkit

    action, _, value = argument.partition(" ")
    scope = context_scope(context)
    if scope is None:
        raise ValueError("不能确认记忆归属。")
    if action in {"", "list"}:
        rows = (
            await AIMemory.filter(archived=False, **scope_filter(scope))
            .order_by("-id")
            .limit(20)
        )
        return "真寻本地记忆：\n" + (
            "\n".join(f"#{m.id} {m.content[:200]}" for m in rows) or "暂无记忆。"
        )
    toolkit = MemoryToolkit()
    if action == "search" and value:
        result = await toolkit.recall_memory(context, value)
    elif action == "add" and value:
        result = await toolkit.memorize(context, value)
    elif action == "del" and value.isdigit():
        result = await toolkit.forget(context, int(value))
    elif action == "organize":
        await require_scene(context, reset=True)
        row = await Memory.summarize(context.session_id, retry=True)
        from zhenxun.models.ai_memory_cursor import AIMemoryCursor

        cursor = await AIMemoryCursor.get_or_none(session_id=context.session_id)
        if cursor and (cursor.state or {}).get("status") in {"pending", "retry_wait"}:
            return "整理未完成，区段已保留；请在整理页查看待处理原因。"
        return (
            "已整理当前对话。" if row else "整理没有产生新记忆；详细状态请查看整理页。"
        )
    else:
        raise ValueError(
            "用法：/ai memory list|search <词>|add <内容>|del <ID>|organize"
        )
    return str(result.output)


async def task_command(context, argument: str) -> str:
    from zhenxun.services.scheduler import scheduler_manager

    from ._tasks import AITasks

    action, _, value = argument.partition(" ")
    jobs = await AITasks.visible(context)
    if not await PermissionUtils.check_superuser(context):
        jobs = [
            job
            for job in jobs
            if str(job.bot_id) == context.run.caller.bot_id
            and not (job.job_kwargs or {}).get("management_subject")
        ]
    if action in {"", "list"}:
        return (
            "\n".join(
                f"#{j.id} {j.name} · {j.last_run_status or '等待'}" for j in jobs[:30]
            )
            or "没有可管理的计划。"
        )
    parts = value.split()
    job = next((j for j in jobs if parts and str(j.id) == parts[0]), None)
    if job is None:
        raise ValueError("计划不存在或无权管理。")
    if action == "show":
        return json.dumps(AITasks.item(job), ensure_ascii=False, default=str)[:2500]
    if action == "del":
        await AITasks.remove(job.id)
        return "已停止当前运行并删除计划。"
    if action == "stop":
        from zhenxun.services.ai.run.session import session_manager

        if len(parts) != 2:
            raise ValueError("停止本次执行须带运行ID：/ai task stop <计划ID> <运行ID>")
        entry = session_manager.runs.get(parts[1])
        if entry and str(getattr(entry[0].deps, "schedule_id", "")) != str(job.id):
            raise ValueError("运行不属于此计划。")
        if entry is None:
            return "此运行已不在本地登记；未确认远端结束，可用 /stop <运行ID> 核验。"
        done = await session_manager.stop(parts[1])
        states = (
            [entry[0].run.operations.get("dsh", {}).get("stop", "unknown")]
            if entry[0].state.get("_dsh_binding")
            else []
        )
        return stop_message(states, done)
    handler = {
        "pause": scheduler_manager.pause_schedule,
        "resume": scheduler_manager.resume_schedule,
        "run": scheduler_manager.trigger_now,
    }.get(action)
    if handler is None:
        raise ValueError("不支持的计划操作。")
    _, message = await handler(job.id)
    return message
