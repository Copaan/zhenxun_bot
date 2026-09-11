import asyncio
from collections.abc import Iterable
import contextlib
from typing import Any, ClassVar
from typing_extensions import Self

from tortoise.backends.base.client import BaseDBAsyncClient
from tortoise.exceptions import (
    IntegrityError,
    MultipleObjectsReturned,
)
from tortoise.manager import Manager
from tortoise.models import Model as TortoiseModel
from tortoise.queryset import QuerySet
from tortoise.transactions import in_transaction

from zhenxun.services.cache import CacheRoot
from zhenxun.services.cache.write import WriteQuery, notify_bulk_write, write_boundary
from zhenxun.services.log import logger
from zhenxun.services.platform_identity import guard_legacy_identity_write
from zhenxun.utils.enum import DbLockType

from .config import LOG_COMMAND, db_model
from .utils import with_db_timeout


class _PlatformGuardedQuerySet(QuerySet):
    def _guard_platform_write(self) -> None:
        guard_legacy_identity_write(self.model._meta.db_table)

    def update(self, **kwargs: Any):
        self._guard_platform_write()
        return WriteQuery(super().update(**kwargs), self.model)

    def delete(self):
        self._guard_platform_write()
        return WriteQuery(super().delete(), self.model)

    def bulk_create(
        self,
        objects,
        batch_size=None,
        ignore_conflicts=False,
        update_fields=None,
        on_conflict=None,
    ):
        self._guard_platform_write()
        return WriteQuery(
            super().bulk_create(
                objects,
                batch_size,
                ignore_conflicts,
                update_fields,
                on_conflict,
            ),
            self.model,
        )

    def bulk_update(self, objects, fields, batch_size=None):
        self._guard_platform_write()
        return WriteQuery(super().bulk_update(objects, fields, batch_size), self.model)

    def raw(self, sql: str):
        self._guard_platform_write()
        return super().raw(sql)


class _PlatformGuardedManager(Manager):
    def get_queryset(self) -> _PlatformGuardedQuerySet:
        return _PlatformGuardedQuerySet(self._model)


class Model(TortoiseModel):
    """
    增强的ORM基类，解决锁嵌套问题
    """

    sem_data: ClassVar[dict[type, dict[DbLockType, asyncio.Semaphore]]] = {}
    _current_locks: ClassVar[
        dict[tuple[type, asyncio.Task], tuple[DbLockType, ...]]
    ] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        cls._meta.manager = _PlatformGuardedManager(cls)

        for name in ("create", "save", "delete", "get_or_create", "update_or_create"):
            descriptor = next(
                base.__dict__[name] for base in cls.__mro__ if name in base.__dict__
            )
            if isinstance(descriptor, classmethod):
                setattr(cls, name, classmethod(write_boundary(descriptor.__func__)))
            else:
                setattr(cls, name, write_boundary(descriptor))

        is_abstract = (
            getattr(cls.Meta, "abstract", False) if hasattr(cls, "Meta") else False
        )
        if not is_abstract and cls.__module__ not in db_model.models:
            db_model.models.append(cls.__module__)

        if func := getattr(cls, "_run_script", None):
            db_model.script_method.append((cls.__module__, func))

    @classmethod
    def get_cache_type(cls) -> str | None:
        """获取缓存类型"""
        return getattr(cls, "cache_type", None)

    @classmethod
    def get_cache_key_field(cls) -> str | tuple[str]:
        """获取缓存键字段"""
        return getattr(cls, "cache_key_field", "id")

    @classmethod
    def get_cache_key(cls, instance) -> str | None:
        """获取缓存键

        参数:
            instance: 模型实例

        返回:
            str | None: 缓存键，如果无法获取则返回None
        """
        from zhenxun.services.cache.config import COMPOSITE_KEY_SEPARATOR

        key_field = cls.get_cache_key_field()

        if isinstance(key_field, tuple):
            # 多字段主键
            key_parts = []
            for field in key_field:
                if hasattr(instance, field):
                    value = getattr(instance, field, None)
                    key_parts.append(value if value is not None else "")
                else:
                    # 如果缺少任何必要的字段，返回None
                    key_parts.append("")

            # 如果没有有效参数，返回None
            return COMPOSITE_KEY_SEPARATOR.join(key_parts) if key_parts else None
        elif hasattr(instance, key_field):
            value = getattr(instance, key_field, None)
            return str(value) if value is not None else None

        return None

    @classmethod
    def get_semaphore(cls, lock_type: DbLockType):
        enable_lock = getattr(cls, "enable_lock", None)
        if not enable_lock or lock_type not in enable_lock:
            return None

        if cls not in cls.sem_data:
            cls.sem_data[cls] = {}
        if lock_type not in cls.sem_data[cls]:
            cls.sem_data[cls][lock_type] = asyncio.Semaphore(1)
        return cls.sem_data[cls][lock_type]

    @classmethod
    def _require_lock(cls, lock_type: DbLockType) -> bool:
        """检查是否需要真正加锁"""
        lock_key = (cls, asyncio.current_task())
        return lock_type not in cls._current_locks.get(lock_key, ())

    @classmethod
    @contextlib.asynccontextmanager
    async def _lock_context(cls, lock_type: DbLockType):
        """带重入检查的锁上下文"""
        lock_key = (cls, asyncio.current_task())
        need_lock = cls._require_lock(lock_type)

        if need_lock and (sem := cls.get_semaphore(lock_type)):
            async with sem:
                previous = cls._current_locks.get(lock_key, ())
                cls._current_locks[lock_key] = (*previous, lock_type)
                try:
                    yield
                finally:
                    if previous:
                        cls._current_locks[lock_key] = previous
                    else:
                        cls._current_locks.pop(lock_key, None)
        else:
            yield

    @classmethod
    def _guard_platform_write(cls) -> None:
        guard_legacy_identity_write(cls._meta.db_table)

    @classmethod
    async def create(
        cls, using_db: BaseDBAsyncClient | None = None, **kwargs: Any
    ) -> Self:
        """创建数据（使用CREATE锁）"""
        cls._guard_platform_write()
        async with cls._lock_context(DbLockType.CREATE):
            # 直接调用父类的_create方法避免触发save的锁
            result = await super().create(using_db=using_db, **kwargs)
            if cache_type := cls.get_cache_type():
                await CacheRoot.invalidate_cache(cache_type, cls.get_cache_key(result))
            return result

    @classmethod
    async def get_or_create(
        cls,
        defaults: dict | None = None,
        using_db: BaseDBAsyncClient | None = None,
        **kwargs: Any,
    ) -> tuple[Self, bool]:
        """获取或创建数据（无锁版本，依赖数据库约束）"""
        cls._guard_platform_write()
        from uuid import uuid4

        db = using_db or cls._choose_db(True)
        existing = await cls.filter(**kwargs).using_db(db).get_or_none()
        if existing is not None:
            return existing, False
        active = getattr(db, "_finalized", None) is False
        context = (
            contextlib.nullcontext(db) if active else in_transaction(db.connection_name)
        )
        async with context as connection:
            savepoint = "zx_create_" + uuid4().hex
            await connection.execute_query(f"SAVEPOINT {savepoint}")
            try:
                result = (
                    await cls.create(
                        using_db=connection, **{**(defaults or {}), **kwargs}
                    ),
                    True,
                )
            except IntegrityError:
                await connection.execute_query(f"ROLLBACK TO SAVEPOINT {savepoint}")
                obj = (
                    await cls.filter(**kwargs)
                    .using_db(connection)
                    .select_for_update()
                    .get_or_none()
                )
                if obj is None:
                    raise
                result = obj, False
            except BaseException:
                with contextlib.suppress(Exception):
                    await connection.execute_query(f"ROLLBACK TO SAVEPOINT {savepoint}")
                raise
            finally:
                # Release errors after a successful write must remain visible;
                # don't attempt SQL on an already disconnected/aborted transaction.
                if "result" in locals():
                    await connection.execute_query(f"RELEASE SAVEPOINT {savepoint}")

        if result[1] and (cache_type := cls.get_cache_type()):
            await CacheRoot.invalidate_cache(cache_type, cls.get_cache_key(result[0]))
        return result

    @classmethod
    async def update_or_create(
        cls,
        defaults: dict | None = None,
        using_db: BaseDBAsyncClient | None = None,
        **kwargs: Any,
    ) -> tuple[Self, bool]:
        """更新或创建数据（使用UPSERT锁）"""
        cls._guard_platform_write()
        async with cls._lock_context(DbLockType.UPSERT):
            db = using_db or cls._choose_db(True)
            active = getattr(db, "_finalized", None) is False
            context = (
                contextlib.nullcontext(db)
                if active
                else in_transaction(db.connection_name)
            )
            async with context as connection:
                obj, created = await cls.get_or_create(
                    defaults=defaults, using_db=connection, **kwargs
                )
                if not created:
                    obj = (
                        await cls.filter(pk=obj.pk)
                        .using_db(connection)
                        .select_for_update()
                        .get()
                    )
                    obj.update_from_dict(defaults or {})
                    if defaults:
                        await obj.save(
                            using_db=connection, update_fields=list(defaults)
                        )
                return obj, created

    async def save(
        self,
        using_db: BaseDBAsyncClient | None = None,
        update_fields: Iterable[str] | None = None,
        force_create: bool = False,
        force_update: bool = False,
    ):
        """保存数据（根据操作类型自动选择锁）"""
        self._guard_platform_write()
        lock_type = (
            DbLockType.CREATE
            if getattr(self, "id", None) is None
            else DbLockType.UPDATE
        )
        async with self._lock_context(lock_type):
            await super().save(
                using_db=using_db,
                update_fields=update_fields,
                force_create=force_create,
                force_update=force_update,
            )
            if self._meta.db_table in {"group_info_users", "group_plugin_settings"}:
                await notify_bulk_write(type(self))
            if cache_type := getattr(self, "cache_type", None):
                await CacheRoot.invalidate_cache(
                    cache_type, self.__class__.get_cache_key(self)
                )

    async def delete(self, using_db: BaseDBAsyncClient | None = None):
        self._guard_platform_write()
        cache_type = getattr(self, "cache_type", None)
        key = self.__class__.get_cache_key(self) if cache_type else None
        # 执行删除操作
        await super().delete(using_db=using_db)
        if self._meta.db_table in {"group_info_users", "group_plugin_settings"}:
            await notify_bulk_write(type(self))

        # 清除缓存
        if cache_type:
            await CacheRoot.invalidate_cache(cache_type, key)

    @classmethod
    def bulk_create(cls, objects, *args, **kwargs):
        cls._guard_platform_write()
        return super().bulk_create(objects, *args, **kwargs)

    @classmethod
    def bulk_update(cls, objects, fields, *args, **kwargs):
        cls._guard_platform_write()
        return super().bulk_update(objects, fields, *args, **kwargs)

    @classmethod
    async def safe_get_or_none(
        cls,
        *args,
        using_db: BaseDBAsyncClient | None = None,
        clean_duplicates: bool = True,
        **kwargs: Any,
    ) -> Self | None:
        """安全地获取一条记录或None，处理存在多个记录时返回最新的那个
        注意，默认会删除重复的记录，仅保留最新的

        参数:
            *args: 查询参数
            using_db: 数据库连接
            clean_duplicates: 是否删除重复的记录，仅保留最新的
            **kwargs: 查询参数

        返回:
            Self | None: 查询结果，如果不存在返回None
        """
        try:
            # 先尝试使用 get_or_none 获取单个记录
            try:
                return await with_db_timeout(
                    cls.get_or_none(*args, using_db=using_db, **kwargs),
                    operation=f"{cls.__name__}.get_or_none",
                    source="DataBaseModel",
                )
            except MultipleObjectsReturned:
                # 如果出现多个记录的情况，进行特殊处理
                logger.warning(
                    f"{cls.__name__} safe_get_or_none 发现多个记录: {kwargs}",
                    LOG_COMMAND,
                )

                # 查询所有匹配记录
                records = await with_db_timeout(
                    cls.filter(*args, **kwargs).all(),
                    operation=f"{cls.__name__}.filter.all",
                    source="DataBaseModel",
                )

                if not records:
                    return None

                # 如果需要清理重复记录
                if clean_duplicates and hasattr(records[0], "id"):
                    # 按 id 排序
                    records = sorted(
                        records, key=lambda x: getattr(x, "id", 0), reverse=True
                    )
                    for record in records[1:]:
                        try:
                            await with_db_timeout(
                                record.delete(),
                                operation=f"{cls.__name__}.delete_duplicate",
                                source="DataBaseModel",
                            )
                            logger.info(
                                f"{cls.__name__} 删除重复记录:"
                                f" id={getattr(record, 'id', None)}",
                                LOG_COMMAND,
                            )
                        except Exception as del_e:
                            logger.error(f"删除重复记录失败: {del_e}")
                    return records[0]
                # 如果不需要清理或没有 id 字段，则返回最新的记录
                if hasattr(cls, "id"):
                    return await with_db_timeout(
                        cls.filter(*args, **kwargs).order_by("-id").first(),
                        operation=f"{cls.__name__}.filter.order_by.first",
                        source="DataBaseModel",
                    )
                # 如果没有 id 字段，则返回第一个记录
                return await with_db_timeout(
                    cls.filter(*args, **kwargs).first(),
                    operation=f"{cls.__name__}.filter.first",
                    source="DataBaseModel",
                )
        except asyncio.TimeoutError:
            logger.error(
                f"数据库操作超时: {cls.__name__}.safe_get_or_none", LOG_COMMAND
            )
            return None
        except Exception as e:
            # 其他类型的错误则继续抛出
            logger.error(
                f"数据库操作异常: {cls.__name__}.safe_get_or_none, {e!s}", LOG_COMMAND
            )
            raise
