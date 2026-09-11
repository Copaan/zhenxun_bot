from datetime import datetime
import math
from typing_extensions import Self

from tortoise import fields

from zhenxun.services.asset_transaction import (
    account_write,
    asset_call,
    require_positive_amount,
)
from zhenxun.services.db_context import Model

from .mahiro_bank_log import BankHandleType, MahiroBankLog


class MahiroBank(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    user_id = fields.CharField(255, unique=True, description="用户id")
    """用户id"""
    amount = fields.BigIntField(default=0, description="存款")
    """用户存款"""
    rate = fields.FloatField(default=0.0005, description="小时利率")
    """小时利率"""
    loan_amount = fields.BigIntField(default=0, description="贷款")
    """用户贷款"""
    loan_rate = fields.FloatField(default=0.0005, description="贷款利率")
    """贷款利率"""
    update_time = fields.DatetimeField(auto_now=True)
    """修改时间"""
    create_time = fields.DatetimeField(auto_now_add=True)
    """创建时间"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "mahiro_bank"
        table_description = "小真寻银行"

    @classmethod
    async def _run_script(cls):
        from zhenxun.services.db_context.schema_ops import CreateIndex

        return [
            CreateIndex(
                "mahiro_bank", ["user_id"], name="zx_bank_account_unique", unique=True
            )
        ]

    @classmethod
    @asset_call
    async def get_account(cls, user_id: str) -> Self:
        from zhenxun.services.asset_transaction import asset_transaction

        async with asset_transaction(user_id):
            return await cls._get_account_locked(user_id)

    @classmethod
    async def _get_account_locked(cls, user_id: str) -> Self:
        rows = await cls.filter(user_id=user_id).limit(2)
        if len(rows) > 1:
            from hashlib import sha256
            from pathlib import Path

            from zhenxun.utils.atomic_json import write_json_locked

            identity = sha256(user_id.encode()).hexdigest()
            Path("data/runtime/asset-audit").mkdir(parents=True, exist_ok=True)
            write_json_locked(
                Path("data/runtime/asset-audit") / f"bank-{identity}.json",
                {
                    "reason": "duplicate_bank_account",
                    "account_digest": identity,
                    "row_ids": [row.id for row in rows],
                    "action": "blocked_without_changes",
                },
            )
            raise RuntimeError(
                "银行账户存在历史重复记录，已阻断操作；请核验资产审计报告。"
            )
        if rows:
            return rows[0]
        return await cls.create(user_id=user_id)

    @classmethod
    @account_write
    async def deposit(cls, user_id: str, amount: int, rate: float) -> Self:
        """存款

        参数:
            user_id: 用户id
            amount: 金币数量
            rate: 小时利率

        返回:
            Self: MahiroBank
        """
        require_positive_amount(amount)
        if not math.isfinite(rate):
            raise ValueError("bank_rate_not_finite")
        effective_hour = int(24 - datetime.now().hour)
        user = await cls.get_account(user_id)
        if not 0 <= user.amount <= 2**63 - 1 - amount:
            raise ValueError("bank_amount_out_of_range")
        user.amount += amount
        user.rate = rate
        await user.save(update_fields=["amount", "rate"])
        await MahiroBankLog.create(
            user_id=user_id,
            amount=amount,
            rate=rate,
            effective_hour=effective_hour,
            handle_type=BankHandleType.DEPOSIT,
        )
        return user

    @classmethod
    @account_write
    async def withdraw(cls, user_id: str, amount: int) -> Self:
        """取款

        参数:
            user_id: 用户id
            amount: 金币数量

        返回:
            Self: MahiroBank
        """
        require_positive_amount(amount)
        if amount <= 0:
            raise ValueError("取款金额必须大于0")
        user = await cls.get_account(user_id)
        if user.amount < amount:
            raise ValueError("取款金额不能大于存款金额")
        user.amount -= amount
        await user.save(update_fields=["amount"])
        await MahiroBankLog.create(
            user_id=user_id, amount=amount, handle_type=BankHandleType.WITHDRAW
        )
        return user

    @classmethod
    @account_write
    async def loan(cls, user_id: str, amount: int, rate: float) -> Self:
        """贷款

        参数:
            user_id: 用户id
            amount: 贷款金额
            rate: 贷款利率

        返回:
            Self: MahiroBank
        """
        require_positive_amount(amount)
        if not math.isfinite(rate):
            raise ValueError("bank_rate_not_finite")
        user = await cls.get_account(user_id)
        if not 0 <= user.loan_amount <= 2**63 - 1 - amount:
            raise ValueError("bank_loan_out_of_range")
        user.loan_amount += amount
        user.loan_rate = rate
        await user.save(update_fields=["loan_amount", "loan_rate"])
        await MahiroBankLog.create(
            user_id=user_id, amount=amount, rate=rate, handle_type=BankHandleType.LOAN
        )
        return user

    @classmethod
    @account_write
    async def repayment(cls, user_id: str, amount: int) -> Self:
        """还款

        参数:
            user_id: 用户id
            amount: 还款金额

        返回:
            Self: MahiroBank
        """
        require_positive_amount(amount)
        if amount <= 0:
            raise ValueError("还款金额必须大于0")
        user = await cls.get_account(user_id)
        if user.loan_amount < amount:
            raise ValueError("还款金额不能大于贷款金额")
        user.loan_amount -= amount
        await user.save(update_fields=["loan_amount"])
        await MahiroBankLog.create(
            user_id=user_id, amount=amount, handle_type=BankHandleType.REPAYMENT
        )
        return user
