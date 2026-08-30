from nonebot import on_message
from nonebot.plugin import PluginMetadata
from nonebot_plugin_alconna import UniMsg
from nonebot_plugin_uninfo import Uninfo

from zhenxun.configs.config import Config
from zhenxun.configs.utils import PluginExtraData, RegisterConfig
from zhenxun.models.chat_history import ChatHistory
from zhenxun.services.db_context import with_db_timeout
from zhenxun.services.log import logger
from zhenxun.services.low_priority_writer import (
    LowPriorityWriterConfig,
    append_low_priority_record,
    register_low_priority_writer,
)
from zhenxun.services.message_load import is_overloaded
from zhenxun.utils.enum import PluginType
from zhenxun.utils.utils import get_entity_ids

__plugin_meta__ = PluginMetadata(
    name="消息存储",
    description="消息存储，被动存储群消息",
    usage="",
    extra=PluginExtraData(
        author="HibiKier",
        version="0.1",
        plugin_type=PluginType.HIDDEN,
        supported_platform_scopes={"qq_client"},
        configs=[
            RegisterConfig(
                module="chat_history",
                key="FLAG",
                value=True,
                help="是否开启消息自从存储",
                default_value=True,
                type=bool,
            )
        ],
    ).to_dict(),
)


def rule(message: UniMsg) -> bool:
    return bool(Config.get_config("chat_history", "FLAG") and message)


chat_history = on_message(rule=rule, priority=1, block=False)

_WRITER_NAME = "chat_history"
_FLUSH_BATCH_SIZE = 200
_FLUSH_MAX_PER_TICK = 1000
_FLUSH_DB_TIMEOUT = 5.0
_STRING_FIELDS = ("user_id", "group_id", "text", "plain_text", "bot_id", "platform")


def _strip_nul(value: str) -> str:
    return value.replace("\x00", "")


def _sanitize_chat_history(record: ChatHistory) -> int:
    changed = 0
    for field in _STRING_FIELDS:
        value = getattr(record, field, None)
        if isinstance(value, str) and "\x00" in value:
            setattr(record, field, _strip_nul(value))
            changed += 1
    return changed


async def _write_chat_history_batch(batch: list[ChatHistory], reason: str) -> None:
    sanitized = sum(_sanitize_chat_history(record) for record in batch)
    if sanitized:
        logger.warning(
            f"聊天历史写库前清理了 {sanitized} 个含 NUL 的字符串字段",
            "chat_history",
        )
    await with_db_timeout(
        ChatHistory.bulk_create(batch, _FLUSH_BATCH_SIZE),
        timeout=_FLUSH_DB_TIMEOUT,
        operation=f"ChatHistory.bulk_create[{len(batch)}]",
        source=f"chat_history:{reason}",
    )


register_low_priority_writer(
    LowPriorityWriterConfig(
        name=_WRITER_NAME,
        write_batch=_write_chat_history_batch,
        batch_size=_FLUSH_BATCH_SIZE,
        trigger_size=_FLUSH_BATCH_SIZE,
        max_retain=5000,
        flush_interval_seconds=60.0,
        max_items_per_cycle=_FLUSH_MAX_PER_TICK,
        backoff_base_seconds=30.0,
        backoff_max_seconds=600.0,
        log_command="chat_history",
    )
)


@chat_history.handle()
async def _(message: UniMsg, session: Uninfo):
    entity = get_entity_ids(session)
    if is_overloaded():
        return
    try:
        from zhenxun.adapters.qq_official.context import get_current_official_context

        official_context = get_current_official_context()
        record = ChatHistory(
            user_id=entity.user_id,
            group_id=entity.group_id,
            text=str(message),
            plain_text=message.extract_plain_text(),
            bot_id=(
                official_context.storage_bot_id
                if official_context is not None
                else session.self_id
            ),
            platform=session.platform,
        )
        sanitized = _sanitize_chat_history(record)
        if sanitized:
            logger.warning(
                f"聊天历史入队前清理了 {sanitized} 个含 NUL 的字符串字段",
                "chat_history",
            )
        await append_low_priority_record(_WRITER_NAME, record)
    except Exception as e:
        logger.warning("存储聊天记录失败", "chat_history", e=e)
