from arclet.alconna import Alconna, Args, Option, Subcommand
from arclet.alconna.action import store_false
from nonebot.permission import SUPERUSER
from nonebot_plugin_alconna import on_alconna


def detach_notice(detached: dict[str, str | None]) -> str:
    """生成账号解除共享策略的提示

    参数:
        detached: 账号与其解除的共享策略名称

    返回:
        str: 提示文本，没有账号解除共享策略时为空
    """
    items = [f"{bot_id}「{name}」" for bot_id, name in detached.items() if name]
    return f"\n已解除共享策略并转为独立设置：{'、'.join(items)}" if items else ""


bot_manage = on_alconna(
    Alconna(
        "bot_manage",
        Subcommand(
            "task",
            Option(
                "list",
                action=store_false,
                help_text="查看 bot_id 下的所有可用被动",
            ),
            Option("-b|--bot", Args["bot_id", str], help_text="指定 bot_id"),
            Subcommand(
                "enable",
                Args["feature_name?", str],
            ),
            Subcommand(
                "disable",
                Args["feature_name?", str],
            ),
        ),
        Subcommand(
            "plugin",
            Option(
                "list",
                action=store_false,
                help_text="查看 bot_id 下的所有可用插件",
            ),
            Option("-b|--bot", Args["bot_id", str], help_text="指定 bot_id"),
            Subcommand(
                "enable",
                Args["plugin_name?", str],
            ),
            Subcommand(
                "disable",
                Args["plugin_name?", str],
            ),
        ),
        Subcommand(
            "full_function",
            Subcommand(
                "enable",
                Args["bot_id?", str],
            ),
            Subcommand(
                "disable",
                Args["bot_id?", str],
            ),
        ),
        Subcommand(
            "bot_switch",
            Subcommand(
                "enable",
                Args["bot_id?", str],
            ),
            Subcommand(
                "disable",
                Args["bot_id?", str],
            ),
        ),
    ),
    permission=SUPERUSER,
    priority=5,
    block=True,
)

bot_manage.shortcut(
    r"bot被动状态",
    command="bot_manage",
    arguments=["task", "list"],
    prefix=True,
)

bot_manage.shortcut(
    r"bot开启被动\s*(?P<name>.+)",
    command="bot_manage",
    arguments=["task", "enable", "{name}"],
    prefix=True,
)

bot_manage.shortcut(
    r"bot关闭被动\s*(?P<name>.+)",
    command="bot_manage",
    arguments=["task", "disable", "{name}"],
    prefix=True,
)

bot_manage.shortcut(
    r"bot开启(全部|所有)被动",
    command="bot_manage",
    arguments=["task", "enable"],
    prefix=True,
)

bot_manage.shortcut(
    r"bot关闭(全部|所有)被动",
    command="bot_manage",
    arguments=["task", "disable"],
    prefix=True,
)

bot_manage.shortcut(
    r"bot插件列表",
    command="bot_manage",
    arguments=["plugin", "list"],
    prefix=True,
)

bot_manage.shortcut(
    r"bot开启(全部|所有)插件",
    command="bot_manage",
    arguments=["plugin", "enable"],
    prefix=True,
)

bot_manage.shortcut(
    r"bot关闭(全部|所有)插件",
    command="bot_manage",
    arguments=["plugin", "disable"],
    prefix=True,
)

bot_manage.shortcut(
    r"bot开启\s*(?P<name>.+)",
    command="bot_manage",
    arguments=["plugin", "enable", "{name}"],
    prefix=True,
)

bot_manage.shortcut(
    r"bot关闭\s*(?P<name>.+)",
    command="bot_manage",
    arguments=["plugin", "disable", "{name}"],
    prefix=True,
)

bot_manage.shortcut(
    r"bot休眠\s*(?P<bot_id>.+)?",
    command="bot_manage",
    arguments=["bot_switch", "disable", "{bot_id}"],
    prefix=True,
)

bot_manage.shortcut(
    r"bot醒来\s*(?P<bot_id>.+)?",
    command="bot_manage",
    arguments=["bot_switch", "enable", "{bot_id}"],
    prefix=True,
)
