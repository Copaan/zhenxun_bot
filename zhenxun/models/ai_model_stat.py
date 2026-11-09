from datetime import datetime, timedelta, timezone
import sqlite3
from typing import ClassVar

from tortoise import fields
from tortoise.expressions import F
from tortoise.timezone import localtime
from tortoise.transactions import in_transaction

from zhenxun.services.db_context import Model

COUNTERS = ("calls", "failures", "latency", "prompt_tokens", "completion_tokens")
"""按小时累计的计数字段"""


class ModelStatWriteError(RuntimeError):
    """模型统计批次的写入结果，rolled_back 表示可以安全重试。"""

    def __init__(self, outcome: str, error: BaseException):
        super().__init__(f"{outcome}: {type(error).__name__}: {error}")
        self.outcome = outcome


class AIModelStat(Model):
    id = fields.BigIntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    hour = fields.DatetimeField(description="统计小时")
    """统计小时（UTC 整点）"""
    model = fields.CharField(255, description="模型线路")
    """模型线路，提供商/模型"""
    task = fields.CharField(32, description="任务类型")
    """任务类型：chat、embedding、tts、image 等"""
    calls = fields.IntField(default=0, description="调用次数")
    """调用次数"""
    failures = fields.IntField(default=0, description="失败次数")
    """失败次数"""
    latency = fields.BigIntField(default=0, description="总耗时（毫秒）")
    """总耗时（毫秒）"""
    prompt_tokens = fields.BigIntField(default=0, description="输入 token")
    """输入 token"""
    completion_tokens = fields.BigIntField(default=0, description="输出 token")
    """输出 token"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "ai_model_stat"
        table_description = "AI 模型调用统计（按小时）"
        unique_together: ClassVar = [("hour", "model", "task")]

    @classmethod
    async def add(cls, rows: dict[tuple[datetime, str, str], list[int]]) -> None:
        """在独立事务内整批累加，回滚确认与提交结果不明分别报告

        参数:
            rows: (小时, 模型线路, 任务类型) -> 按 COUNTERS 顺序的增量
        """
        if not rows:
            return
        transaction = in_transaction()
        try:
            connection = await transaction.__aenter__()
        except BaseException as error:
            raise ModelStatWriteError("rolled_back", error) from error
        try:
            if (
                connection.capabilities.dialect == "sqlite"
                and sqlite3.sqlite_version_info >= (3, 24, 0)
            ):
                columns = ("hour", "model", "task", *COUNTERS)
                executor = connection.executor_class(model=cls, db=connection)
                fields_sql = ",".join(f'"{key}"' for key in columns)
                values_sql = ",".join("?" for _ in columns)
                updates_sql = ",".join(
                    f'"{key}"="{key}"+excluded."{key}"' for key in COUNTERS
                )
                parameters = [
                    [
                        executor.column_map[key](value, cls)
                        for key, value in zip(
                            columns, (localtime(hour), model, task, *values)
                        )
                    ]
                    for (hour, model, task), values in sorted(rows.items())
                ]
                await connection.execute_many(
                    f'INSERT INTO "{cls._meta.db_table}" ({fields_sql}) '
                    f"VALUES ({values_sql}) "
                    f'ON CONFLICT ("hour","model","task") '
                    f"DO UPDATE SET {updates_sql}",
                    parameters,
                )
            else:
                for (hour, model, task), values in sorted(rows.items()):
                    if connection.capabilities.dialect == "mysql":
                        columns = ("hour", "model", "task", *COUNTERS)
                        fields_sql = ",".join(f"`{key}`" for key in columns)
                        values_sql = ",".join("%s" for _ in columns)
                        updates_sql = ",".join(
                            f"`{key}`=`{key}`+%s" for key in COUNTERS
                        )
                        await connection.execute_query(
                            f"INSERT INTO `{cls._meta.db_table}` ({fields_sql}) "
                            f"VALUES ({values_sql}) "
                            f"ON DUPLICATE KEY UPDATE {updates_sql}",
                            [localtime(hour), model, task, *values, *values],
                        )
                        continue
                    item, _ = await cls.get_or_create(
                        hour=localtime(hour),
                        model=model,
                        task=task,
                        defaults=dict.fromkeys(COUNTERS, 0),
                        using_db=connection,
                    )
                    await (
                        cls.filter(pk=item.pk)
                        .using_db(connection)
                        .update(
                            **{
                                key: F(key) + value
                                for key, value in zip(COUNTERS, values)
                            }
                        )
                    )
        except BaseException as error:
            try:
                await transaction.__aexit__(type(error), error, error.__traceback__)
                if not connection._finalized:
                    raise RuntimeError("statistics_rollback_unconfirmed")
            except BaseException as cleanup_error:
                raise ModelStatWriteError("unknown", cleanup_error) from error
            raise ModelStatWriteError("rolled_back", error) from error
        try:
            await transaction.__aexit__(None, None, None)
        except BaseException as error:
            raise ModelStatWriteError("unknown", error) from error

    @classmethod
    async def cleanup(cls, keep_days: int = 90) -> int:
        """删除过期的统计

        参数:
            keep_days: 保留天数

        返回:
            int: 删除条数
        """
        cutoff = localtime(datetime.now(timezone.utc) - timedelta(days=keep_days))
        return await cls.filter(hour__lt=cutoff).delete()
