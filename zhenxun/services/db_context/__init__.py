import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Literal
from urllib.parse import urlparse

import aiofiles
import nonebot
from nonebot.utils import is_coroutine_callable
from tortoise import Tortoise
from tortoise.connection import connections
from tortoise.exceptions import ConfigurationError
from tortoise.transactions import in_transaction

from zhenxun.configs.config import BotConfig
from zhenxun.configs.database import is_sqlite_memory_url, sqlite_path_from_url
from zhenxun.services.log import logger
from zhenxun.utils.manager.priority_manager import PriorityLifecycle

from . import watchdog as _watchdog  # noqa: F401
from .base_model import Model
from .config import (
    DB_TIMEOUT_SECONDS,
    MYSQL_CONFIG,
    POSTGRESQL_CONFIG,
    SLOW_QUERY_THRESHOLD,
    SQLITE_CONFIG,
    db_model,
    prompt,
)
from .exceptions import DbConnectError, DbUrlIsNode
from .schema_guard import repair_safe_schema_drift
from .schema_ops import SchemaOpRisk, normalize_schema_ops
from .script_compat import script_action
from .utils import with_db_timeout

Dialect = Literal["sqlite", "postgres", "mysql", "unknown"]


@dataclass(frozen=True, slots=True)
class MigrationStatement:
    owner: str
    sql: str


MODELS = db_model.models
SCRIPT_METHOD = db_model.script_method

__all__ = [
    "DB_TIMEOUT_SECONDS",
    "MODELS",
    "SCRIPT_METHOD",
    "SLOW_QUERY_THRESHOLD",
    "DbConnectError",
    "DbUrlIsNode",
    "Model",
    "database_ready",
    "disconnect",
    "init",
    "with_db_timeout",
]

driver = nonebot.get_driver()

_SCRIPT_HASH_DIR = Path() / "data" / ".db_script_hashes"
_TRUE_VALUES = {"1", "true", "yes", "on"}
_database_ready = False


def database_ready() -> bool:
    """Only admit connection-hook writes after schema preparation has succeeded."""
    return _database_ready


def _connection_dialect() -> Dialect:
    try:
        connection = Tortoise.get_connection("default")
        capabilities = getattr(connection, "capabilities", None)
        raw = str(getattr(capabilities, "dialect", "") or "").lower()
        if raw.startswith("sqlite"):
            return "sqlite"
        if raw.startswith("postgres"):
            return "postgres"
        if raw.startswith("mysql"):
            return "mysql"
    except Exception:
        pass
    return "unknown"


def _allow_guarded_schema_ops() -> bool:
    """Whether startup may run guarded SchemaOp migrations.

    Safe SchemaOps are limited to non-destructive changes such as adding nullable
    columns and non-unique indexes. Guarded operations may rename, drop, or alter
    columns, so keep them opt-in to avoid damaging existing databases during
    normal startup.
    """
    return os.getenv("DB_SCHEMA_RUN_GUARDED_OPS", "").strip().lower() in _TRUE_VALUES


def _extract_alter_table_name(sql: str) -> str | None:
    match = re.match(r"ALTER\s+TABLE\s+[`\"]?(\w+)[`\"]?", sql, re.IGNORECASE)
    return match.group(1) if match else None


def _extract_create_index_table_name(sql: str) -> str | None:
    match = re.search(r"\bON\s+[`\"]?(\w+)[`\"]?\s*\(", sql, re.IGNORECASE)
    return match.group(1) if match else None


def _extract_statement_table_name(sql: str) -> str | None:
    """Extract the target table for statements that need existence checks."""
    patterns = (
        r"^ALTER\s+TABLE\s+[`\"]?(\w+)[`\"]?",
        r"^UPDATE\s+[`\"]?(\w+)[`\"]?",
        r"^DELETE\s+FROM\s+[`\"]?(\w+)[`\"]?",
        r"^INSERT\s+INTO\s+[`\"]?(\w+)[`\"]?",
    )
    for pattern in patterns:
        match = re.match(pattern, sql.strip(), re.IGNORECASE)
        if match:
            return match.group(1)
    return _extract_create_index_table_name(sql)


def _db_script_hash_file(script_fingerprint: str) -> Path:
    parsed = urlparse(BotConfig.db_url or "")
    dialect = parsed.scheme or "unknown"
    if dialect == "sqlite":
        db_identity = (
            ":memory:"
            if is_sqlite_memory_url(BotConfig.db_url)
            else str(sqlite_path_from_url(BotConfig.db_url))
        )
    else:
        db_identity = f"{parsed.hostname or ''}:{parsed.port or ''}{parsed.path}"
    db_hash = hashlib.md5(
        json.dumps(
            {
                "dialect": dialect,
                "db": db_identity,
                "script": script_fingerprint,
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode()
    ).hexdigest()
    return _SCRIPT_HASH_DIR / f"{db_hash}.json"


def _migration_lock_path() -> Path:
    identity = hashlib.md5((BotConfig.db_url or "").encode()).hexdigest()
    return _SCRIPT_HASH_DIR / f"{identity}.lock"


def _acquire_file_lock(handle) -> None:
    handle.seek(0)
    handle.write("0")
    handle.flush()
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)


def _release_file_lock(handle) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@asynccontextmanager
async def _schema_migration_lock():
    _SCRIPT_HASH_DIR.mkdir(parents=True, exist_ok=True)
    handle = await asyncio.to_thread(_migration_lock_path().open, "a+")
    try:
        await asyncio.to_thread(_acquire_file_lock, handle)
        yield
    finally:
        try:
            await asyncio.to_thread(_release_file_lock, handle)
        finally:
            handle.close()


async def _run_script_migrations(
    statements: list[MigrationStatement | str], fingerprint: str
) -> None:
    """Run legacy scripts once; deferred declarations are not completed DDL."""
    normalized = [
        item
        if isinstance(item, MigrationStatement)
        else MigrationStatement("unknown", item)
        for item in statements
    ]
    script_hash_file = _db_script_hash_file(fingerprint)
    async with _schema_migration_lock():
        # Read both fingerprint formats. Adding diagnostics must not replay DML.
        aliases = {
            fingerprint,
            hashlib.md5(
                json.dumps(
                    sorted(item.sql for item in normalized), ensure_ascii=False
                ).encode()
            ).hexdigest(),
            hashlib.md5(
                json.dumps(
                    sorted((item.owner, item.sql) for item in normalized),
                    ensure_ascii=False,
                ).encode()
            ).hexdigest(),
        }
        previous = {}
        for candidate in aliases:
            try:
                saved = json.loads(
                    _db_script_hash_file(candidate).read_text(encoding="utf-8")
                )
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(saved, dict) and saved.get("script_fingerprint") == candidate:
                if saved.get("status") in {None, "completed", "legacy_deferred"}:
                    logger.debug("迁移脚本无变化，跳过执行")
                    return
                # Ordinals are meaningful only for the exact ordered input.
                if saved.get("status") == "pending_tables":
                    if saved.get("statements") != [item.sql for item in normalized]:
                        raise RuntimeError(
                            "待完成迁移的语句顺序已变化，拒绝重放已执行数据脚本"
                        )
                    previous = saved

        dialect = _connection_dialect()
        attempted: list[MigrationStatement] = []
        completed = set(previous.get("completed", []))
        deferred_indexes = set(previous.get("deferred_indexes", []))
        deferred = list(previous.get("deferred", []))
        pending = []
        current: MigrationStatement | None = None
        current_index = -1
        try:
            async with in_transaction(connection_name="default") as transaction:
                for index, statement in enumerate(normalized):
                    current = statement
                    current_index = index
                    sql = statement.sql
                    if index in completed or index in deferred_indexes:
                        continue
                    action = await asyncio.wait_for(
                        script_action(transaction, sql, dialect), DB_TIMEOUT_SECONDS
                    )
                    if action == "missing_table":
                        pending.append(index)
                        continue
                    if action == "legacy_deferred":
                        deferred.append({"owner": statement.owner, "sql": sql})
                        logger.warning(
                            "SQLite 保留旧插件兼容声明（未执行类型转换）: "
                            f"{statement.owner}: {sql}"
                        )
                        deferred_indexes.add(index)
                        continue
                    elif action == "execute":
                        attempted.append(statement)
                        logger.debug(f"执行迁移SQL: {statement.owner}: {sql}")
                        await asyncio.wait_for(
                            transaction.execute_query_dict(sql),
                            timeout=DB_TIMEOUT_SECONDS,
                        )
                    completed.add(index)
        except Exception as error:
            remaining = (
                normalized[current_index + 1 :] if current_index >= 0 else normalized
            )
            current_text = f"{current.owner}: {current.sql}" if current else "<unknown>"
            logger.debug(
                f"迁移失败明细: dialect={dialect}; "
                f"attempted={attempted}; pending={remaining}"
            )
            rollback = (
                "DDL may be committed"
                if dialect == "mysql"
                else "transaction rolled back"
            )
            raise RuntimeError(
                "数据库迁移未完成，未写入脚本指纹: "
                f"dialect={dialect}; current={current_text}; error={error}; "
                f"attempted_count={len(attempted)}; pending_count={len(remaining)}; "
                f"rollback={rollback}"
            ) from error

        payload = json.dumps(
            {
                "dialect": urlparse(BotConfig.db_url or "").scheme,
                "db_url_hash": hashlib.md5(
                    (BotConfig.db_url or "").encode()
                ).hexdigest(),
                "script_fingerprint": fingerprint,
                "status": "pending_tables"
                if pending
                else "legacy_deferred"
                if deferred
                else "completed",
                "statements": [item.sql for item in normalized],
                "completed": sorted(completed),
                "deferred_indexes": sorted(deferred_indexes),
                "deferred": deferred,
                "pending": pending,
            },
            ensure_ascii=False,
            indent=2,
        )
        temporary = script_hash_file.with_suffix(
            f".tmp.{os.getpid()}.{os.urandom(4).hex()}"
        )
        temporary.write_text(payload, encoding="utf-8")
        os.replace(temporary, script_hash_file)
        logger.debug("SCRIPT_METHOD方法执行完毕!")


def get_config() -> dict:
    """获取数据库配置"""
    if not BotConfig.db_url:
        raise DbUrlIsNode("数据库Url连接字符串为空，请检查配置文件（.env.dev）")
    parsed = urlparse(BotConfig.db_url)

    config = {
        "connections": {"default": BotConfig.db_url},
        "apps": {
            "models": {
                "models": db_model.models,
                "default_connection": "default",
            }
        },
        "timezone": "Asia/Shanghai",
    }

    if parsed.scheme.startswith("postgres"):
        config["connections"]["default"] = {
            "engine": "tortoise.backends.asyncpg",
            "credentials": {
                "host": parsed.hostname,
                "port": parsed.port or 5432,
                "user": parsed.username,
                "password": parsed.password,
                "database": parsed.path[1:],
                **POSTGRESQL_CONFIG,
            },
        }
    elif parsed.scheme == "mysql":
        config["connections"]["default"] = {
            "engine": "tortoise.backends.mysql",
            "credentials": {
                "host": parsed.hostname,
                "port": parsed.port or 3306,
                "user": parsed.username,
                "password": parsed.password,
                "database": parsed.path[1:],
                **MYSQL_CONFIG,
            },
        }
    elif parsed.scheme == "sqlite":
        if is_sqlite_memory_url(BotConfig.db_url):
            sqlite_file_path = ":memory:"
        else:
            sqlite_path = sqlite_path_from_url(BotConfig.db_url)
            sqlite_path.parent.mkdir(parents=True, exist_ok=True)
            sqlite_file_path = str(sqlite_path)
        config["connections"]["default"] = {
            "engine": "tortoise.backends.sqlite",
            "credentials": {
                "file_path": sqlite_file_path,
                **SQLITE_CONFIG,
            },
        }
    return config


@PriorityLifecycle.on_startup(
    priority=1,
    stage="management",
    timeout=60,
    component_id="management:database",
    scope="worker",
    restart_policy="worker",
    config_keys=("DB_URL", "DATABASE_MODELS", "DATABASE_SCHEMA"),
)
async def init():
    global MODELS, SCRIPT_METHOD, _database_ready

    _database_ready = False

    env_example_file = Path() / ".env.example"
    env_dev_file = Path() / ".env.dev"
    if not env_dev_file.exists():
        async with aiofiles.open(env_example_file, encoding="utf-8") as f:
            env_text = await f.read()
        async with aiofiles.open(env_dev_file, "w", encoding="utf-8") as f:
            await f.write(env_text)
        logger.info("已生成 .env.dev 文件，请根据 .env.example 文件配置进行配置")

    MODELS = db_model.models
    SCRIPT_METHOD = db_model.script_method
    if not BotConfig.db_url:
        error = prompt
        raise DbUrlIsNode("\n" + error.strip())
    try:
        await Tortoise.init(
            config=get_config(),
        )
        from .timing import instrument_client

        instrument_client(Tortoise.get_connection("default"))
        migration_statements: list[MigrationStatement] = []
        if db_model.script_method:
            logger.debug(
                "即将运行SCRIPT_METHOD方法, 合计 "
                f"<u><y>{len(db_model.script_method)}</y></u> 个..."
            )
            allow_guarded_ops = _allow_guarded_schema_ops()
            for module, func in db_model.script_method:
                try:
                    items = await func() if is_coroutine_callable(func) else func()
                    if not items:
                        continue
                    for item in items:
                        if not isinstance(item, str):
                            if item.risk == SchemaOpRisk.MANUAL:
                                logger.debug(f"{module} 跳过手动迁移动作: {item}")
                                continue
                            if (
                                item.risk == SchemaOpRisk.GUARDED
                                and not allow_guarded_ops
                            ):
                                logger.debug(f"{module} 跳过受保护迁移动作: {item}")
                                continue
                            if item.risk != SchemaOpRisk.SAFE and not allow_guarded_ops:
                                logger.debug(f"{module} 跳过未知风险迁移动作: {item}")
                                continue
                        owner = f"{module}:{getattr(func, '__qualname__', 'script')}"
                        migration_statements.extend(
                            MigrationStatement(owner, sql)
                            for sql in normalize_schema_ops(
                                [item], _connection_dialect()
                            )
                        )
                except Exception as e:
                    logger.debug(f"{module} 执行SCRIPT_METHOD方法出错...", e=e)
            if migration_statements:
                fingerprint = hashlib.md5(
                    json.dumps(
                        sorted(item.sql for item in migration_statements),
                        ensure_ascii=False,
                    ).encode()
                ).hexdigest()
                await _run_script_migrations(migration_statements, fingerprint)
        # Tortoise may emit column comments/index SQL during generate_schemas().
        # On existing databases with newly added nullable fields, PostgreSQL can
        # fail before the post-generate SchemaGuard gets a chance to repair drift.
        await repair_safe_schema_drift()
        logger.debug("开始生成数据库表结构...")
        await Tortoise.generate_schemas()
        if migration_statements:
            await _run_script_migrations(migration_statements, fingerprint)
        logger.debug("数据库表结构生成完毕!")
        from zhenxun.models.chat_history import ensure_chat_history_nullable_columns
        from zhenxun.models.group_plugin_setting import (
            ensure_group_plugin_scope_constraint,
        )

        await ensure_chat_history_nullable_columns()
        async with _schema_migration_lock():
            await ensure_group_plugin_scope_constraint()
        await repair_safe_schema_drift()
        _database_ready = True
        logger.info("Database loaded successfully!")
    except Exception as e:
        raise DbConnectError(f"数据库连接错误... e:{e}") from e


@PriorityLifecycle.on_shutdown(priority=100, component_id="management:database")
async def disconnect():
    global _database_ready

    _database_ready = False
    try:
        await connections.close_all()
    except ConfigurationError:
        logger.debug("数据库连接未初始化，跳过关闭")
    except Exception as e:
        logger.error(f"关闭数据库连接时发生意外错误: {e}")
