from pathlib import Path

import nonebot

from zhenxun.configs.config import Config

Config.add_plugin_config(
    "hook",
    "CHECK_NOTICE_INFO_CD",
    300,
    help="群检测，个人权限检测等各种检测提示信息cd",
    default_value=300,
    type=int,
    ui={
        "label": "检测提示冷却",
        "section": "权限检测",
        "component": "number",
        "order": 10,
        "minimum": 0,
        "unit": "秒",
    },
)

Config.add_plugin_config(
    "hook",
    "MALICIOUS_BAN_TIME",
    30,
    help="恶意命令触发检测触发后ban的时长（分钟）",
    default_value=30,
    type=int,
    ui={
        "label": "封禁时长",
        "section": "恶意触发检测",
        "component": "number",
        "order": 30,
        "minimum": 1,
        "unit": "分钟",
    },
)

Config.add_plugin_config(
    "hook",
    "MALICIOUS_CHECK_TIME",
    5,
    help="恶意命令触发检测规定时间内（秒）",
    default_value=5,
    type=int,
    ui={
        "label": "检测时间窗",
        "section": "恶意触发检测",
        "component": "number",
        "order": 20,
        "minimum": 1,
        "unit": "秒",
    },
)

Config.add_plugin_config(
    "hook",
    "MALICIOUS_BAN_COUNT",
    6,
    help="恶意命令触发检测最大触发次数",
    default_value=6,
    type=int,
    ui={
        "label": "最大触发次数",
        "section": "恶意触发检测",
        "component": "number",
        "order": 40,
        "minimum": 1,
    },
)

Config.add_plugin_config(
    "hook",
    "MALICIOUS_CHECK_MODE",
    "off",
    help="恶意触发检测模式：off=关闭，blacklist=仅列表插件检测，whitelist=列表插件跳过检测",
    default_value="off",
    type=str,
    ui={
        "label": "检测模式",
        "section": "恶意触发检测",
        "component": "select",
        "order": 10,
        "options": [
            {"label": "关闭", "value": "off"},
            {"label": "黑名单", "value": "blacklist"},
            {"label": "白名单", "value": "whitelist"},
        ],
    },
)

Config.add_plugin_config(
    "hook",
    "MALICIOUS_CHECK_PLUGINS",
    [],
    help="恶意触发检测插件列表，按模式作为黑名单或白名单使用，填插件模块名",
    default_value=[],
    type=list,
    ui={
        "label": "插件名单",
        "section": "恶意触发检测",
        "component": "tags",
        "order": 50,
        "visible_when": {
            "path": "MALICIOUS_CHECK_MODE",
            "operator": "neq",
            "value": "off",
        },
    },
)

Config.add_plugin_config(
    "hook",
    "IS_SEND_TIP_MESSAGE",
    True,
    help="是否发送阻断时提示消息",
    default_value=True,
    type=bool,
    ui={
        "label": "发送阻断提示",
        "section": "权限检测",
        "component": "switch",
        "order": 20,
    },
)

nonebot.load_plugins(str(Path(__file__).parent.resolve()))
