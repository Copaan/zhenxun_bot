from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, cast

import nonebot
from nonebot.adapters.onebot.v11 import Adapter as OneBotV11Adapter
from nonebot.adapters.onebot.v11 import Bot as OneBotV11Bot
from nonebot.adapters.onebot.v11 import Message
import pytest


@dataclass
class DummyAdapter:
    name: str
    calls: list[tuple[str, dict[str, Any]]]

    def get_name(self) -> str:
        return self.name

    async def _call_api(self, bot, api: str, **data: Any):
        self.calls.append((api, data))
        return {"message_id": len(self.calls), "api": api, "bot_id": bot.self_id}


@dataclass
class DummyBot:
    self_id: str
    adapter: DummyAdapter

    async def call_api(self, api: str, **data: Any):
        return await self.adapter._call_api(self, api, **data)


def _models():
    from zhenxun.models.ban_console import BanConsole
    from zhenxun.models.friend_user import FriendUser
    from zhenxun.models.group_console import GroupConsole
    from zhenxun.models.group_member_info import GroupInfoUser
    from zhenxun.models.level_user import LevelUser
    from zhenxun.models.user_console import UserConsole

    return BanConsole, FriendUser, GroupConsole, GroupInfoUser, LevelUser, UserConsole


@pytest.fixture(autouse=True)
async def _clean_scoped_identity_tables(app):
    del app
    BanConsole, FriendUser, GroupConsole, GroupInfoUser, LevelUser, UserConsole = (
        _models()
    )
    await GroupConsole.all().delete()
    await FriendUser.all().delete()
    await GroupInfoUser.all().delete()
    await LevelUser.all().delete()
    await BanConsole.all().delete()
    await UserConsole.all().delete()
    yield
    await GroupConsole.all().delete()
    await FriendUser.all().delete()
    await GroupInfoUser.all().delete()
    await LevelUser.all().delete()
    await BanConsole.all().delete()
    await UserConsole.all().delete()


def _make_onebot_bot(self_id: str = "onebot-client") -> OneBotV11Bot:
    driver = nonebot.get_driver()
    try:
        adapter = nonebot.get_adapter(OneBotV11Adapter)
    except Exception:
        driver.register_adapter(OneBotV11Adapter)
        adapter = nonebot.get_adapter(OneBotV11Adapter)
    return OneBotV11Bot(adapter, self_id)


def _make_official_bot(self_id: str = "qq-api") -> DummyBot:
    return DummyBot(self_id=self_id, adapter=DummyAdapter("qq", []))


@pytest.mark.asyncio
async def test_onebot_only_keeps_legacy_platform_and_scope():
    from zhenxun.utils.platform import PlatformUtils

    bot = _make_onebot_bot()

    assert PlatformUtils.get_platform(bot) == "qq"
    assert PlatformUtils.get_platform_scope(bot) == "qq_client"


@pytest.mark.asyncio
async def test_qq_official_only_uses_qq_api_scope_without_changing_legacy_platform():
    from zhenxun.utils.platform import PlatformUtils

    bot = _make_official_bot()

    assert PlatformUtils.get_platform_scope(bot) == "qq_api"
    assert PlatformUtils.is_qbot(cast(Any, bot)) is True


@pytest.mark.asyncio
async def test_same_user_group_collision_keeps_legacy_rows_unchanged():
    from tortoise.exceptions import IntegrityError

    BanConsole, FriendUser, GroupConsole, GroupInfoUser, LevelUser, _ = _models()

    user_id = "collision-user"
    group_id = "collision-group"

    await GroupConsole.create(group_id=group_id, group_name="OneBot Group")
    await FriendUser.create(user_id=user_id, user_name="onebot-friend")
    await GroupInfoUser.create(
        user_id=user_id,
        group_id=group_id,
        user_name="onebot-member",
    )
    await LevelUser.set_level(user_id, group_id, 5)
    await BanConsole.ban(user_id, group_id, 5, "legacy", 60)

    with pytest.raises(IntegrityError):
        await FriendUser.create(user_id=user_id, user_name="official-friend")
    with pytest.raises(IntegrityError):
        await GroupInfoUser.create(
            user_id=user_id,
            group_id=group_id,
            user_name="official-member",
        )

    onebot_group = await GroupConsole.get_group_db(group_id)
    assert onebot_group is not None
    assert onebot_group.group_name == "OneBot Group"
    assert await FriendUser.get_user_name(user_id) == "onebot-friend"
    assert await GroupInfoUser.get_all_uid(group_id) == {user_id}
    assert await LevelUser.get_user_level(user_id, group_id) == 5

    from zhenxun.services.cache.runtime_cache import BanMemoryCache

    await BanMemoryCache.refresh()
    assert await BanConsole.is_ban(user_id, group_id) is True


@pytest.mark.asyncio
async def test_context_fallback_prevents_official_writes_from_polluting_onebot():
    from zhenxun.builtin_plugins.hooks.auth.context import EventContext
    from zhenxun.builtin_plugins.hooks.auth_snapshot import (
        _build_runtime_group_snapshot,
    )
    from zhenxun.utils.utils import EntityIDs

    context = EventContext(
        bot_id="qq-api",
        platform="qq",
        platform_scope="qq_api",
        event_type="message",
        message_id="msg-1",
        entity=EntityIDs(
            user_id="ctx-user",
            group_id="ctx-group",
            channel_id=None,
        ),
    )

    snapshot = _build_runtime_group_snapshot(context)

    assert snapshot is not None
    assert snapshot.group_id == "ctx-group"
    assert snapshot.status is True
    assert snapshot.level == 5
    _, FriendUser, GroupConsole, GroupInfoUser, LevelUser, _ = _models()
    assert await FriendUser.all().count() == 0
    assert await GroupConsole.all().count() == 0
    assert await GroupInfoUser.all().count() == 0
    assert await LevelUser.all().count() == 0


@pytest.mark.asyncio
async def test_background_task_without_bot_id_prefers_unique_onebot(monkeypatch):
    from zhenxun.utils.platform import PlatformUtils

    onebot = _make_onebot_bot("onebot-client")
    official = _make_official_bot("qq-api")
    monkeypatch.setattr(
        nonebot,
        "get_bots",
        lambda: {onebot.self_id: onebot, official.self_id: official},
    )

    assert PlatformUtils.resolve_bot(log_cmd="test") is onebot
    assert PlatformUtils.resolve_bot(platform_scope="qq_client") is onebot
    assert PlatformUtils.resolve_bot(platform_scope="qq_api") is official


@pytest.mark.asyncio
async def test_onebot_send_queue_patches_only_onebot_adapter(monkeypatch):
    from zhenxun.services import send_queue

    adapter_class = send_queue.OneBotV11Adapter
    original_call_api = adapter_class._call_api
    await send_queue.stop_send_queue()
    adapter_class._call_api = original_call_api
    send_queue._PATCHED = False
    adapter = adapter_class(nonebot.get_driver())
    bot = send_queue.OneBotV11Bot(adapter, "onebot-client")
    calls: list[tuple[str, dict[str, Any]]] = []

    async def fake_original(adapter, bot, api: str, **data: Any):
        calls.append((api, data))
        return {"message_id": len(calls)}

    monkeypatch.setattr(send_queue, "_ORIG_CALL_API", fake_original)
    await send_queue.start_send_queue()
    try:
        results = await asyncio.gather(
            *(
                bot.call_api(
                    "send_msg",
                    message_type="group",
                    group_id=10000,
                    message=Message(f"msg-{idx}"),
                )
                for idx in range(30)
            )
        )
    finally:
        await send_queue.stop_send_queue()
        adapter_class._call_api = original_call_api
        send_queue._PATCHED = False

    assert len(results) == 30
    assert len(calls) == 30


@pytest.mark.asyncio
async def test_send_queue_start_repairs_partial_worker_set():
    from zhenxun.services import send_queue

    await send_queue.stop_send_queue()
    await send_queue.start_send_queue()
    exited = send_queue._WORKER_TASKS.pop()
    exited.cancel()
    await asyncio.gather(exited, return_exceptions=True)

    await send_queue.start_send_queue()

    assert send_queue.send_queue_healthy()
    assert len(send_queue._WORKER_TASKS) == send_queue._WORKERS
    await send_queue.stop_send_queue()


@pytest.mark.asyncio
async def test_qq_official_send_does_not_enter_onebot_queue():
    from zhenxun.services.send_queue import patch_send_queue, unpatch_send_queue

    official = _make_official_bot()
    original = OneBotV11Adapter._call_api
    patch_send_queue()
    try:
        assert OneBotV11Adapter._call_api is not original
        result = await official.call_api("send_message", content="hello")
    finally:
        unpatch_send_queue()

    assert result["api"] == "send_message"
    assert official.adapter.calls == [("send_message", {"content": "hello"})]


@pytest.mark.asyncio
async def test_legacy_unique_conflicts_do_not_overwrite_rows():
    from tortoise.exceptions import IntegrityError

    _, FriendUser, _, _, _, UserConsole = _models()

    await FriendUser.create(user_id="unique-user", user_name="legacy")
    with pytest.raises(IntegrityError):
        await FriendUser.create(user_id="unique-user", user_name="official")

    assert await FriendUser.get_user_name("unique-user") == "legacy"

    try:
        await UserConsole.get_or_create_user("unique-user", platform="qq")
        await UserConsole.get_or_create_user("unique-user", platform="qq_api")
    except IntegrityError as exc:
        raise AssertionError("UserConsole should not leak unique conflicts") from exc

    assert await UserConsole.get_or_none(user_id="unique-user")


@pytest.mark.asyncio
async def test_bot_console_get_or_create_is_connect_hook_race_safe():
    from zhenxun.models.bot_console import BotConsole

    bot_id = "race-safe-bot"
    await BotConsole.filter(bot_id=bot_id).delete()

    try:
        results = await asyncio.gather(
            *(BotConsole.get_or_create(bot_id=bot_id) for _ in range(20))
        )

        assert await BotConsole.filter(bot_id=bot_id).count() == 1
        assert {item[0].bot_id for item in results} == {bot_id}
    finally:
        await BotConsole.filter(bot_id=bot_id).delete()
