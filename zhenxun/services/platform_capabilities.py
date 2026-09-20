"""Event-local capabilities; remote API entitlement is checked by the API call."""

from dataclasses import dataclass

from nonebot.adapters import Bot, Event
from nonebot.adapters.onebot.v11 import Bot as OneBotBot
from nonebot.adapters.qq import Bot as QQBot
from nonebot.adapters.qq.event import DirectMessageCreateEvent, InteractionCreateEvent


@dataclass(frozen=True)
class PlatformCapabilities:
    available: frozenset[str]
    authorization_unknown: frozenset[str] = frozenset()


def event_capabilities(bot: Bot, event: Event, session) -> PlatformCapabilities:
    available = {"uni_message"}
    remote = set()
    scene = session.scene
    if isinstance(event, DirectMessageCreateEvent):
        available.add("guild_dm")
    elif scene.is_channel:
        available.update({"guild", "channel"})
    elif scene.is_guild:
        available.add("guild")
    elif scene.is_group:
        available.add("group")
    elif scene.is_private:
        available.add("c2c")
    if event.get_type() == "message":
        available.add("passive_reply")
    if isinstance(event, InteractionCreateEvent) and event.type == 11:
        available.add("interaction_response")
        if event.event_id:
            available.add("passive_reply")
    if isinstance(bot, QQBot | OneBotBot) and available & {"group", "guild"}:
        available.add("members_query")
        # SDK support cannot establish server-side authorization for this Bot.
        remote.add("members_query")
    return PlatformCapabilities(frozenset(available), frozenset(remote))
