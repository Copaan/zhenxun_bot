import asyncio
from datetime import timedelta
import random

from nonebot_plugin_uninfo import Uninfo
from tortoise.expressions import RawSQL
from tortoise.functions import Count, Sum
from tortoise.timezone import localtime

from zhenxun.configs.config import Config
from zhenxun.models.mahiro_bank import MahiroBank
from zhenxun.models.mahiro_bank_log import MahiroBankLog
from zhenxun.models.sign_user import SignUser
from zhenxun.models.user_console import UserConsole
from zhenxun.services import avatar_service
from zhenxun.services.asset_transaction import (
    account_write,
    asset_call,
    asset_transaction,
)
from zhenxun.utils.enum import BankHandleType, GoldHandle
from zhenxun.utils.platform import PlatformUtils

base_config = Config.get("mahiro_bank")


class BankManager:
    @classmethod
    async def random_event(cls, impression: float):
        """随机事件"""
        impression_event = base_config.get("impression_event")
        impression_event_prop = base_config.get("impression_event_prop")
        impression_event_range = base_config.get("impression_event_range")
        if impression >= impression_event and random.random() < impression_event_prop:
            """触发好感度事件"""
            return random.uniform(impression_event_range[0], impression_event_range[1])
        return None

    @classmethod
    async def deposit_check(cls, user_id: str, amount: int) -> str | None:
        """检查存款是否合法

        参数:
            user_id: 用户id
            amount: 存款金额

        返回:
            str | None: 存款信息
        """
        if amount <= 0:
            return "存款数量必须大于 0 啊笨蛋！"
        user = await UserConsole.get_user(user_id)
        sign_user = await SignUser.get_user(user_id)
        bank_user = await cls.get_user(user_id)
        sign_max_deposit: int = base_config.get("sign_max_deposit")
        max_deposit = max(int(float(sign_user.impression) * sign_max_deposit), 100)
        if user.gold < amount:
            return f"金币数量不足，当前你的金币为：{user.gold}."
        if bank_user.amount + amount > max_deposit:
            return (
                f"存款超过上限，存款上限为：{max_deposit}，"
                f"当前你的还可以存款金额：{max_deposit - bank_user.amount}。"
            )
        max_daily_deposit_count: int = base_config.get("max_daily_deposit_count")
        today_deposit_count = len(await cls.get_user_deposit(user_id))
        if today_deposit_count >= max_daily_deposit_count:
            return f"存款次数超过上限，每日存款次数上限为：{max_daily_deposit_count}。"
        return None

    @classmethod
    async def withdraw_check(cls, user_id: str, amount: int) -> str | None:
        """检查取款是否合法

        参数:
            user_id: 用户id
            amount: 取款金额

        返回:
            str | None: 取款信息
        """
        if amount <= 0:
            return "取款数量必须大于 0 啊笨蛋！"
        user = await cls.get_user(user_id)
        data_list = await cls.get_user_deposit(user_id)
        lock_amount = sum(data.amount for data in data_list)
        if user.amount - lock_amount < amount:
            return (
                "取款金额不足，当前你的存款为："
                f"{user.amount}（{lock_amount}已被锁定）！"
            )
        return None

    @classmethod
    async def get_user_deposit(
        cls, user_id: str, is_completed: bool = False
    ) -> list[MahiroBankLog]:
        """获取用户今日存款次数

        参数:
            user_id: 用户id

        返回:
            list[MahiroBankLog]: 存款列表
        """
        return await MahiroBankLog.filter(
            user_id=user_id,
            handle_type=BankHandleType.DEPOSIT,
            is_completed=is_completed,
        )

    @classmethod
    async def get_user(cls, user_id: str) -> MahiroBank:
        """查询余额

        参数:
            user_id: 用户id

        返回:
            MahiroBank
        """
        return await MahiroBank.get_account(user_id)

    @classmethod
    async def get_user_data(
        cls,
        user_id: str,
        data_type: BankHandleType,
        is_completed: bool = False,
        count: int = 5,
    ) -> list[MahiroBankLog]:
        return (
            await MahiroBankLog.filter(
                user_id=user_id, handle_type=data_type, is_completed=is_completed
            )
            .order_by("-id")
            .limit(count)
            .all()
        )

    @classmethod
    async def complete_projected_revenue(cls, user_id: str) -> int:
        """预计收益

        参数:
            user_id: 用户id

        返回:
            int: 预计收益金额
        """
        deposit_list = await cls.get_user_deposit(user_id)
        if not deposit_list:
            return 0
        return int(
            sum(
                deposit.rate * deposit.amount * deposit.effective_hour
                for deposit in deposit_list
            )
        )

    @classmethod
    async def get_user_info_data(cls, session: Uninfo, uname: str) -> dict:
        """获取用户数据（返回字典）

        参数:
            session: Uninfo
            uname: 用户id

        返回:
            dict: 用户银行数据字典
        """
        user_id = session.user.id
        user = await cls.get_user(user_id=user_id)
        (
            rank,
            deposit_count,
            user_today_deposit,
            projected_revenue,
            sum_data,
        ) = await asyncio.gather(
            *[
                MahiroBank.filter(amount__gt=user.amount).count(),
                MahiroBankLog.filter(user_id=user_id).count(),
                cls.get_user_deposit(user_id),
                cls.complete_projected_revenue(user_id),
                MahiroBankLog.filter(
                    user_id=user_id, handle_type=BankHandleType.INTEREST
                )
                .annotate(sum=Sum("amount"))
                .values("sum"),
            ]
        )
        now = localtime()
        end_time = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        today_deposit_amount = sum(deposit.amount for deposit in user_today_deposit)
        deposit_list = [
            {
                "id": deposit.id,
                "date": str(now.date()),
                "start_time": str(deposit.create_time).split(".")[0],
                "end_time": str(end_time.replace(microsecond=0)),
                "amount": deposit.amount,
                "rate": f"{deposit.rate * 100:.2f}",
                "projected_revenue": int(
                    deposit.amount * deposit.rate * deposit.effective_hour
                )
                or 1,
            }
            for deposit in user_today_deposit
        ]
        platform = PlatformUtils.get_platform(session)
        avatar_path = await avatar_service.get_avatar_path(platform, user_id)
        avatar_url = avatar_path.as_uri() if avatar_path else ""
        return {
            "name": uname,
            "rank": rank + 1,
            "avatar_url": avatar_url or "",
            "amount": user.amount,
            "deposit_count": deposit_count,
            "today_deposit_count": len(user_today_deposit),
            "cumulative_gain": sum_data[0]["sum"] or 0,
            "projected_revenue": projected_revenue,
            "today_deposit_amount": today_deposit_amount,
            "deposit_list": deposit_list,
            "create_time": str(now.replace(microsecond=0)),
        }

    @classmethod
    async def get_bank_info_data(cls) -> dict:
        """获取银行总览数据（返回字典）

        返回:
            dict: 银行总览数据字典
        """
        now = localtime()
        now_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        (
            bank_data,
            today_count,
            interest_amount,
            active_user_count,
            date_data,
        ) = await asyncio.gather(
            *[
                MahiroBank.annotate(
                    amount_sum=Sum("amount"), user_count=Count("id")
                ).values("amount_sum", "user_count"),
                MahiroBankLog.filter(
                    create_time__gt=now_start, handle_type=BankHandleType.DEPOSIT
                ).count(),
                MahiroBankLog.filter(handle_type=BankHandleType.INTEREST)
                .annotate(amount_sum=Sum("amount"))
                .values("amount_sum"),
                MahiroBankLog.filter(
                    create_time__gte=now_start - timedelta(days=7),
                    handle_type=BankHandleType.DEPOSIT,
                )
                .annotate(count=Count("user_id", distinct=True))
                .values("count"),
                MahiroBankLog.filter(
                    create_time__gte=now_start - timedelta(days=7),
                    handle_type=BankHandleType.DEPOSIT,
                )
                .annotate(date=RawSQL("DATE(create_time)"), total_amount=Sum("amount"))
                .group_by("date")
                .values("date", "total_amount"),
            ]
        )
        date2cnt = {str(date["date"]): date["total_amount"] for date in date_data}
        date = now.date()
        e_date, e_amount = [], []
        for _ in range(7):
            if str(date) in date2cnt:
                e_amount.append(date2cnt[str(date)])
            else:
                e_amount.append(0)
            e_date.append(str(date)[5:])
            date -= timedelta(days=1)
        e_date.reverse()
        e_amount.reverse()
        date = 1
        lasted_log = await MahiroBankLog.annotate().order_by("create_time").first()
        if lasted_log:
            date = now.date() - lasted_log.create_time.date()
            date = (date.days or 1) + 1
        return {
            "amount_sum": bank_data[0]["amount_sum"] or 0,
            "user_count": bank_data[0]["user_count"] or 0,
            "today_count": today_count,
            "day_amount": int((bank_data[0]["amount_sum"] or 0) / date),
            "interest_amount": interest_amount[0]["amount_sum"] or 0,
            "active_user_count": active_user_count[0]["count"] or 0,
            "e_data": e_date,
            "e_amount": e_amount,
            "create_time": str(now.replace(microsecond=0)),
        }

    @classmethod
    @account_write
    async def deposit(
        cls, user_id: str, amount: int
    ) -> tuple[MahiroBank, float, float | None]:
        """存款

        参数:
            user_id: 用户id
            amount: 存款数量

        返回:
            tuple[MahiroBank, float, float]: MahiroBank，利率，增加的利率
        """
        if error := await cls.deposit_check(user_id, amount):
            raise ValueError(error)
        rate_range = base_config.get("rate_range")
        rate = random.uniform(rate_range[0], rate_range[1])
        sign_user = await SignUser.get_user(user_id)
        random_add_rate = await cls.random_event(float(sign_user.impression))
        if random_add_rate:
            rate += random_add_rate
        await UserConsole.reduce_gold(user_id, amount, GoldHandle.PLUGIN, "bank")
        return await MahiroBank.deposit(user_id, amount, rate), rate, random_add_rate

    @classmethod
    @account_write
    async def withdraw(cls, user_id: str, amount: int) -> MahiroBank:
        """取款

        参数:
            user_id: 用户id
            amount: 取款数量

        返回:
            MahiroBank
        """
        if error := await cls.withdraw_check(user_id, amount):
            raise ValueError(error)
        await UserConsole.add_gold(user_id, amount, "bank")
        return await MahiroBank.withdraw(user_id, amount)

    @classmethod
    @account_write
    async def loan(cls, user_id: str, amount: int) -> tuple[MahiroBank, float | None]:
        """贷款

        参数:
            user_id: 用户id
            amount: 贷款数量

        返回:
            tuple[MahiroBank, float]: MahiroBank，贷款利率
        """
        rate_range = base_config.get("rate_range")
        rate = random.uniform(rate_range[0], rate_range[1])
        sign_user = await SignUser.get_user(user_id)
        user = await MahiroBank.get_account(user_id)
        if user.loan_amount + amount > sign_user.impression * 150:
            raise ValueError("贷款数量超过最大限制，请签到提升好感度获取更多额度吧...")
        random_reduce_rate = await cls.random_event(float(sign_user.impression))
        if random_reduce_rate:
            rate -= random_reduce_rate
        await UserConsole.add_gold(user_id, amount, "bank")
        return await MahiroBank.loan(user_id, amount, rate), random_reduce_rate

    @classmethod
    @account_write
    async def repayment(cls, user_id: str, amount: int) -> MahiroBank:
        """还款

        参数:
            user_id: 用户id
            amount: 还款数量

        返回:
            MahiroBank
        """
        await UserConsole.reduce_gold(user_id, amount, GoldHandle.PLUGIN, "bank")
        return await MahiroBank.repayment(user_id, amount)

    @classmethod
    async def settlement(cls):
        """结算每日利率"""
        period = localtime().date().isoformat()
        user_ids = await MahiroBank.filter(amount__gt=0).values_list(
            "user_id", flat=True
        )
        pending_ids = await MahiroBankLog.filter(
            is_completed=False, handle_type=BankHandleType.DEPOSIT
        ).values_list("user_id", flat=True)
        for user_id in sorted(set(user_ids) | set(pending_ids)):
            await cls._settle_account(user_id, period)

    @classmethod
    @asset_call
    async def _settle_account(cls, user_id, period):
        from hashlib import sha256

        from zhenxun.models.asset_operation import AssetOperation

        async with asset_transaction(user_id):
            key = sha256(f"bank-interest:{user_id}:{period}".encode()).hexdigest()
            if await AssetOperation.filter(id=key).exists():
                return
            bank = await MahiroBank.get_account(user_id)
            logs = await MahiroBankLog.filter(
                user_id=user_id,
                is_completed=False,
                handle_type=BankHandleType.DEPOSIT,
            ).all()
            payments = []
            amount = bank.amount - sum(log.amount for log in logs)
            if bank.amount > 0 and amount:
                payments.append((int(amount * bank.rate), bank.rate))
            for log in logs:
                payments.append(
                    (int(log.amount * log.rate * log.effective_hour) or 1, log.rate)
                )
                log.is_completed = True
                await log.save(update_fields=["is_completed"])
            total = sum(gold for gold, _ in payments)
            if total < 0:
                raise RuntimeError("bank_settlement_negative_interest")
            if total:
                await UserConsole.add_gold(user_id, total, "bank_interest")
            for gold, rate in payments:
                await MahiroBankLog.create(
                    user_id=user_id,
                    amount=gold,
                    rate=rate,
                    handle_type=BankHandleType.INTEREST,
                    is_completed=True,
                )
            await AssetOperation.create(
                id=key,
                user_id=user_id,
                kind="bank_interest",
                state="committed",
                payload={"period": period, "gold": total},
            )
