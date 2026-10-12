import re

from zhenxun.models.plugin_info import PluginInfo

from .config import SwitchEnum

_GROUP_WAKE_PATTERN = re.compile(r"^醒来$", re.IGNORECASE)
_GROUP_WAKE_CANONICAL_PATTERN = re.compile(r"^group-status\s+wake$", re.IGNORECASE)


def _is_group_wake_command(plugin: PluginInfo, text: str) -> bool:
    if "plugin_switch" not in (plugin.module or ""):
        return False
    normalized = re.sub(r"\s+", " ", (text or "").strip())
    if not normalized:
        return False
    if (
        _GROUP_WAKE_PATTERN.match(normalized) is not None
        or _GROUP_WAKE_CANONICAL_PATTERN.match(normalized) is not None
    ):
        return True
    # 兼容 to_me 前缀场景：如“真寻 醒来”
    tokens = normalized.split(" ")
    return len(tokens) == 2 and tokens[-1] == SwitchEnum.ENABLE
