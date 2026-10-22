from collections.abc import Callable
import random
import re
from typing import overload

from nonebot.adapters import Bot
from nonebot_plugin_uninfo import Session, SupportScope, Uninfo, get_interface

from zhenxun.configs.config import BotConfig
from zhenxun.configs.path_config import THEMES_PATH
from zhenxun.services.cache.runtime_cache import (
    BanMemoryCache,
    BotMemoryCache,
    GroupMemoryCache,
    TaskInfoMemoryCache,
)
from zhenxun.services.log import logger


class CommonUtils:
    @classmethod
    async def task_is_block(
        cls, session: Uninfo | Bot, module: str, group_id: str | None = None
    ) -> bool:
        """判断被动技能是否被阻断。

        被动全局状态、账号与群聊/私聊禁用由 plugin_policy_service.availability
        统一判断，另外检查 bot 休眠、群权限与群 ban 状态。

        参数:
            module: 被动技能模块名
            group_id: 群组id

        返回:
            bool: True 表示被动技能应被阻断，False 表示允许继续执行
        """
        if isinstance(session, Bot):
            if interface := get_interface(session):
                info = interface.basic_info()
                if info["scope"] == SupportScope.qq_api:
                    logger.debug("q官bot放弃所有被动技能发言...")
                    return True
        if isinstance(session, Session) and session.scope == SupportScope.qq_api:
            logger.debug("q官bot放弃所有被动技能发言...")
            return True
        if not group_id and isinstance(session, Session):
            group_id = session.group.id if session.group else None
        from zhenxun.services.bot_group_policy import bot_group_policy_service
        from zhenxun.services.platform_identity import (
            is_namespaced_scope,
            scoped_group_id,
        )
        from zhenxun.services.plugin_policy import plugin_policy_service
        from zhenxun.utils.platform import PlatformUtils

        scope = PlatformUtils.get_platform_scope(session)
        bot_id = PlatformUtils.storage_bot_id(scope, str(session.self_id))
        channel_id = getattr(getattr(session, "channel", None), "id", None)
        stored_group_id = scoped_group_id(group_id, scope)
        group = (
            GroupMemoryCache.get_if_ready(
                stored_group_id, None if is_namespaced_scope(scope) else channel_id
            )
            if group_id
            else None
        )
        await bot_group_policy_service.ensure_fresh(bot_id, scope, group_id, channel_id)
        bot_snapshot = await BotMemoryCache.get(bot_id)
        if bot_snapshot and not bot_snapshot.status:
            """bot是否休眠"""
            return True
        profile = await TaskInfoMemoryCache.get(module)
        if profile is not None and plugin_policy_service.availability(
            profile,
            bot_snapshot,
            group,
            bot_id=bot_id,
            platform_scope=scope,
            group_id=group_id,
            channel_id=channel_id,
            task=True,
        ):
            """被动全局状态、账号与群聊/私聊禁用"""
            return True
        if group_id:
            if group and group.level < 0:
                """群组权限是否小于0"""
                return True
            if BanMemoryCache.is_banned(None, stored_group_id):
                """群组是否被ban"""
                return True
        return False

    @staticmethod
    def format(name: str) -> str:
        return f"<{name},"

    @overload
    @classmethod
    def convert_module_format(cls, data: str) -> list[str]: ...

    @overload
    @classmethod
    def convert_module_format(cls, data: list[str]) -> str: ...

    @classmethod
    def convert_module_format(cls, data: str | list[str]) -> str | list[str]:
        """
        在 `<aaa,<bbb,<ccc,` 和 `["aaa", "bbb", "ccc"]` 之间进行相互转换。

        参数:
            data (str | list[str]): 输入数据，可能是格式化字符串或字符串列表。

        返回:
            str | list[str]: 根据输入类型返回转换后的数据。
        """
        if isinstance(data, str):
            return [item.strip(",") for item in data.split("<") if item]
        elif isinstance(data, list):
            return "".join(cls.format(item) for item in data)

    @staticmethod
    def get_random_asset_factory(sub_path: str) -> Callable[[], str | None]:
        """
        创建一个从指定 assets 子目录随机选取资源的工厂函数。
        用于 Pydantic 模型的 default_factory。

        参数:
            sub_path: 相对于 themes/default/assets/ 的子路径，例如 "ui/zhenxun/down"
        """

        def _factory() -> str | None:
            target_dir = THEMES_PATH / "default" / "assets" / sub_path
            if not target_dir.exists():
                return None

            images = [
                f.name
                for f in target_dir.iterdir()
                if f.is_file()
                and f.suffix.lower() in [".png", ".jpg", ".jpeg", ".webp"]
            ]
            return f"{sub_path}/{random.choice(images)}" if images else None

        return _factory


class SqlUtils:
    @classmethod
    def random(cls, query, limit: int = 1) -> str:
        db_class_name = BotConfig.get_sql_type()
        if "postgres" in db_class_name or "sqlite" in db_class_name:
            query = f"{query.sql()} ORDER BY RANDOM() LIMIT {limit};"
        elif "mysql" in db_class_name:
            query = f"{query.sql()} ORDER BY RAND() LIMIT {limit};"
        else:
            logger.warning(
                f"Unsupported database type: {db_class_name}", query.__module__
            )
        return query

    @classmethod
    def local_date_sql(cls, column: str, sql_type: str | None = None) -> str:
        """按 ORM 时区（Asia/Shanghai）取日期的 SQL 表达式

        参数:
            column: 时间列名
            sql_type: 数据库类型，默认读取当前配置

        返回:
            str: SQL 表达式
        """
        sql_type = sql_type or BotConfig.get_sql_type()
        if "sqlite" in sql_type:
            # SQLite 存的是带偏移的本地时间字符串，DATE() 会先换算成 UTC
            return f"DATE(SUBSTR({column}, 1, 10))"
        if "postgres" in sql_type:
            return f"DATE({column} AT TIME ZONE 'Asia/Shanghai')"
        return f"DATE({column})"

    @classmethod
    def add_column(
        cls,
        table_name: str,
        column_name: str,
        column_type: str,
        default: str | None = None,
        not_null: bool = False,
    ) -> str:
        sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
        if default:
            sql += f" DEFAULT {default}"
        if not_null:
            sql += " NOT NULL"
        return sql


def format_usage_for_markdown(text: str) -> str:
    """
    智能地将Python多行字符串转换为适合Markdown渲染的格式。
    - 在列表、标题等块级元素前自动插入换行，确保正确解析。
    - 将段落内的单个换行符替换为Markdown的硬换行（行尾加两个空格）。
    - 保留两个或更多的连续换行符，使其成为Markdown的段落分隔。
    """
    if not text:
        return ""

    text = re.sub(r"([^\n])\n(\s*[-*] |\s*#+\s|\s*>)", r"\1\n\n\2", text)

    text = re.sub(r"(?<!\n)\n(?!\n)", "  \n", text)

    return text
