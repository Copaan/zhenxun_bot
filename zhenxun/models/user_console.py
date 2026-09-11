import asyncio
from dataclasses import dataclass
from typing import ClassVar
from uuid import uuid4

from tortoise import fields
from tortoise.exceptions import IntegrityError
from tortoise.expressions import F

from zhenxun.models.goods_info import GoodsInfo
from zhenxun.services.asset_transaction import (
    account_write,
    asset_call,
    asset_transaction,
    require_positive_amount,
)
from zhenxun.services.buffered_writers import append_user_gold_log
from zhenxun.services.db_context import Model
from zhenxun.utils.enum import CacheType, GoldHandle
from zhenxun.utils.exception import GoodsNotFound, InsufficientGold


@dataclass(slots=True)
class GoldReservation:
    user_id: str
    gold: int
    handle: GoldHandle
    plugin_module: str
    platform: str | None = None
    operation_id: str = ""
    committed: bool = False
    released: bool = False

    @asset_call
    async def _finish(self, action: str) -> None:
        from zhenxun.models.asset_operation import AssetOperation

        async with asset_transaction(self.user_id, self.platform):
            record = await AssetOperation.get(id=self.operation_id)
            if (
                record.kind != "fee"
                or record.user_id != self.user_id
                or record.state not in {"reserved", "committed", "released"}
            ):
                raise RuntimeError("fee_reservation_receipt_invalid")
            if record.state == "reserved":
                amount = record.payload["gold"]
                require_positive_amount(amount)
                if action == "committed":
                    await append_user_gold_log(
                        user_id=self.user_id,
                        gold=amount,
                        handle=GoldHandle(record.payload["handle"]),
                        source=record.payload["plugin_module"],
                    )
                else:
                    updated = await UserConsole.filter(
                        user_id=self.user_id, gold__lte=2**31 - 1 - amount
                    ).update(gold=F("gold") + amount)
                    if not updated:
                        raise ValueError("fee_refund_balance_out_of_range")
                    await UserConsole.invalidate_user_cache(self.user_id)
                record.state = action
                await record.save(update_fields=["state", "update_time"])
            state = record.state
        # Publish local state only after the transaction is confirmed.
        self.committed = state == "committed"
        self.released = state == "released"

    async def commit(self) -> None:
        await self._finish("committed")

    async def release(self) -> None:
        await self._finish("released")


class UserConsole(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    user_id = fields.CharField(255, unique=True, description="用户id")
    """用户id"""
    uid = fields.IntField(description="UID", unique=True)
    """UID"""
    gold = fields.IntField(default=100, description="金币数量")
    """金币数量"""
    sign = fields.ReverseRelation["SignUser"]  # type: ignore
    """好感度"""
    props: dict[str, int] = fields.JSONField(default={})  # type: ignore
    """道具"""
    platform = fields.CharField(255, null=True, description="平台")
    """平台"""
    create_time = fields.DatetimeField(auto_now_add=True, description="创建时间")
    """创建时间"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "user_console"
        table_description = "用户数据表"
        indexes = [("user_id",), ("uid",)]  # noqa: RUF012

    cache_type = CacheType.USERS
    """缓存类型"""
    cache_key_field = "user_id"
    """缓存键字段"""

    _uid_counter: ClassVar[int | None] = None
    _uid_lock: ClassVar[asyncio.Lock] = asyncio.Lock()

    @classmethod
    async def get_or_create_user(
        cls, user_id: str, platform: str | None = None
    ) -> tuple["UserConsole", bool]:
        existing = await cls.get_or_none(user_id=user_id)
        if existing is not None:
            return existing, False
        for attempt in range(2):
            try:
                return await cls.get_or_create(
                    user_id=user_id,
                    defaults={"platform": platform, "uid": await cls.get_new_uid()},
                )
            except IntegrityError:
                async with cls._uid_lock:
                    cls._uid_counter = None
                if attempt >= 1:
                    raise
        return await cls.get_or_create(
            user_id=user_id,
            defaults={"platform": platform, "uid": await cls.get_new_uid()},
        )

    @classmethod
    async def get_user(cls, user_id: str, platform: str | None = None) -> "UserConsole":
        """获取用户

        参数:
            user_id: 用户id
            platform: 平台.

        返回:
            UserConsole: UserConsole
        """
        user, _ = await cls.get_or_create_user(user_id=user_id, platform=platform)
        return user

    @classmethod
    async def _get_user_for_write(
        cls, user_id: str, platform: str | None = None
    ) -> "UserConsole":
        """获取写入用用户；已有用户不走 get_or_create，避免重复清理缓存。"""
        user = await cls.get_or_none(user_id=user_id)
        if user is not None:
            return user
        user, _ = await cls.get_or_create_user(user_id=user_id, platform=platform)
        return user

    @classmethod
    async def get_new_uid(cls) -> int:
        """获取最新uid

        返回:
            int: 最新uid
        """
        async with cls._uid_lock:
            if cls._uid_counter is None:
                user = await cls.annotate().order_by("-uid").first()
                cls._uid_counter = user.uid if user else 0
            cls._uid_counter += 1
            return cls._uid_counter

    @classmethod
    @account_write
    async def set_gold(cls, user_id: str, gold: int, platform: str | None = None):
        if (
            isinstance(gold, bool)
            or not isinstance(gold, int)
            or not 0 <= gold <= 2**31 - 1
        ):
            raise ValueError("gold_out_of_range")
        user = await cls._get_user_for_write(user_id, platform)
        delta = gold - user.gold
        await cls.filter(user_id=user_id).update(gold=gold)
        if delta:
            await append_user_gold_log(
                user_id,
                abs(delta),
                GoldHandle.GET if delta > 0 else GoldHandle.PLUGIN,
                "superuser_set",
            )
        await cls.invalidate_user_cache(user_id)

    @classmethod
    @account_write
    async def add_gold(
        cls, user_id: str, gold: int, source: str, platform: str | None = None
    ):
        """添加金币

        参数:
            user_id: 用户id
            gold: 金币
            source: 来源
            platform: 平台.
        """
        require_positive_amount(gold)
        await cls._get_user_for_write(user_id=user_id, platform=platform)
        # 原子自增,避免并发 read-modify-write 丢币(A2);filter().update()
        # 不触发基类 save() 的缓存失效,需手动失效。
        updated = await cls.filter(user_id=user_id, gold__lte=2**31 - 1 - gold).update(
            gold=F("gold") + gold
        )
        if not updated:
            raise ValueError("gold_out_of_range")
        await cls.invalidate_user_cache(user_id)
        await append_user_gold_log(
            user_id=user_id, gold=gold, handle=GoldHandle.GET, source=source
        )

    @classmethod
    @account_write
    async def reduce_gold(
        cls,
        user_id: str,
        gold: int,
        handle: GoldHandle,
        plugin_module: str,
        platform: str | None = None,
    ):
        """消耗金币

        参数:
            user_id: 用户id
            gold: 金币
            handle: 金币处理
            plugin_name: 插件模块
            platform: 平台.

        异常:
            InsufficientGold: 金币不足
        """
        require_positive_amount(gold)
        user = await cls._get_user_for_write(user_id=user_id, platform=platform)
        if user.gold < gold:
            raise InsufficientGold()
        # 原子扣减 + gold__gte 守卫,防并发超扣(A2);未命中说明余额已被
        # 其他协程扣走,按金币不足处理。
        updated = await cls.filter(user_id=user_id, gold__gte=gold).update(
            gold=F("gold") - gold
        )
        if not updated:
            raise InsufficientGold()
        await cls.invalidate_user_cache(user_id)
        await append_user_gold_log(
            user_id=user_id, gold=gold, handle=handle, source=plugin_module
        )

    @classmethod
    @asset_call
    async def reserve_gold(
        cls,
        user_id: str,
        gold: int,
        handle: GoldHandle,
        plugin_module: str,
        platform: str | None = None,
    ) -> GoldReservation:
        """预扣金币；插件最终未执行时可 release 补偿。"""
        from zhenxun.models.asset_operation import AssetOperation
        from zhenxun.services.message_execution import current_execution, operation_key

        require_positive_amount(gold)
        operation_id = operation_key(f"fee:{plugin_module}", user_id) or uuid4().hex
        async with asset_transaction(user_id, platform) as user:
            existing = await AssetOperation.get_or_none(id=operation_id)
            if existing:
                if {
                    key: existing.payload.get(key)
                    for key in ("gold", "handle", "plugin_module", "platform")
                } != {
                    "gold": gold,
                    "handle": handle.value,
                    "plugin_module": plugin_module,
                    "platform": platform,
                }:
                    raise RuntimeError("fee_reservation_input_conflict")
                return GoldReservation(
                    user_id=user_id,
                    gold=gold,
                    handle=handle,
                    plugin_module=plugin_module,
                    platform=platform,
                    operation_id=operation_id,
                    committed=existing.state == "committed",
                    released=existing.state == "released",
                )
            if user.gold < gold:
                raise InsufficientGold()
            updated = await cls.filter(user_id=user_id, gold__gte=gold).update(
                gold=F("gold") - gold
            )
            if not updated:
                raise InsufficientGold()
            await AssetOperation.create(
                id=operation_id,
                user_id=user_id,
                kind="fee",
                state="reserved",
                event_id=current_execution.get().identity
                if current_execution.get()
                else None,
                payload={
                    "event": current_execution.get().identity
                    if current_execution.get()
                    else None,
                    "gold": gold,
                    "handle": handle.value,
                    "plugin_module": plugin_module,
                    "platform": platform,
                },
            )
            await cls.invalidate_user_cache(user_id)
        return GoldReservation(
            user_id=user_id,
            gold=gold,
            handle=handle,
            plugin_module=plugin_module,
            platform=platform,
            operation_id=operation_id,
        )

    @classmethod
    async def invalidate_user_cache(cls, user_id: str) -> None:
        from zhenxun.services.cache import CacheRoot

        await CacheRoot.invalidate_cache(CacheType.USERS, user_id)

    @classmethod
    @account_write
    async def add_props(
        cls, user_id: str, goods_uuid: str, num: int = 1, platform: str | None = None
    ):
        """添加道具

        参数:
            user_id: 用户id
            goods_uuid: 道具uuid
            num: 道具数量.
            platform: 平台.
        """
        require_positive_amount(num)
        user = await cls._get_user_for_write(user_id=user_id, platform=platform)
        if goods_uuid not in user.props:
            user.props[goods_uuid] = 0
        if (
            not isinstance(user.props[goods_uuid], int)
            or not 0 <= user.props[goods_uuid] <= 2**31 - 1 - num
        ):
            raise ValueError("props_quantity_out_of_range")
        user.props[goods_uuid] += num
        await user.save(update_fields=["props"])

    @classmethod
    async def add_props_by_name(
        cls, user_id: str, name: str, num: int = 1, platform: str | None = None
    ):
        """根据名称添加道具

        参数:
            user_id: 用户id
            name: 道具名称
            num: 道具数量.
            platform: 平台.
        """
        if goods := await GoodsInfo.get_or_none(goods_name=name):
            return await cls.add_props(user_id, goods.uuid, num, platform)
        raise GoodsNotFound("未找到商品...")

    @classmethod
    @account_write
    async def use_props(
        cls, user_id: str, goods_uuid: str, num: int = 1, platform: str | None = None
    ):
        """添加道具

        参数:
            user_id: 用户id
            goods_uuid: 道具uuid
            num: 道具数量.
            platform: 平台.
        """
        require_positive_amount(num)
        user = await cls._get_user_for_write(user_id=user_id, platform=platform)

        if goods_uuid not in user.props or user.props[goods_uuid] < num:
            raise GoodsNotFound("未找到商品或道具数量不足...")
        user.props[goods_uuid] -= num
        if user.props[goods_uuid] <= 0:
            del user.props[goods_uuid]
        await user.save(update_fields=["props"])

    @classmethod
    async def use_props_by_name(
        cls, user_id: str, name: str, num: int = 1, platform: str | None = None
    ):
        """根据名称添加道具

        参数:
            user_id: 用户id
            name: 道具名称
            num: 道具数量.
            platform: 平台.
        """
        if goods := await GoodsInfo.get_or_none(goods_name=name):
            return await cls.use_props(user_id, goods.uuid, num, platform)
        raise GoodsNotFound("未找到商品...")

    @classmethod
    async def _run_script(cls):
        return []
