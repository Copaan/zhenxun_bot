"""
模型调用统计

每次线路请求的结果先累计在内存，按小时分桶，定时批量写入 ai_model_stat。
"""

import asyncio
from contextvars import Context
from datetime import datetime, timezone
from typing import Any, ClassVar

from zhenxun.services.ai.utils.logger import log_llm as logger
from zhenxun.services.lifecycle.deadline import (
    ShutdownBudget,
    current_budget,
    remaining_timeout,
    shutdown_budget,
)


class ModelStats:
    """按“小时 × 模型线路 × 任务类型”累计调用次数、失败、耗时与 token"""

    FLUSH_INTERVAL = 60
    _pending: ClassVar[dict[tuple[datetime, str, str], list[int]]] = {}
    _task: ClassVar[asyncio.Task | None] = None
    _flush_lock = asyncio.Lock()
    _shutdown_lock = asyncio.Lock()
    _inflight: ClassVar[asyncio.Task | None] = None
    _uncertain: ClassVar[dict[tuple[datetime, str, str], list[int]] | None] = None
    _write_budget: ClassVar[ShutdownBudget | None] = None
    _stopping = False
    _failures = 0

    @classmethod
    def record(
        cls, route: str, task: str, ok: bool, latency: float, response: Any = None
    ) -> None:
        """记录一次线路请求

        参数:
            route: 线路名
            task: 任务类型
            ok: 是否成功
            latency: 耗时，秒
            response: 统一响应对象，用于读取 token 用量
        """
        prompt = completion = 0
        if usage := getattr(response, "usage_info", None):
            from zhenxun.services.ai.core.engine.token_counter import parse_usage_info

            try:
                info = parse_usage_info(usage)
                prompt, completion = info.prompt_tokens, info.completion_tokens
            except Exception:
                pass
        hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        values = cls._pending.setdefault((hour, route, task), [0, 0, 0, 0, 0])
        for index, value in enumerate(
            (1, 0 if ok else 1, int(latency * 1000), prompt, completion)
        ):
            values[index] += value

    @classmethod
    async def flush(cls) -> None:
        """等待同一个在途批次，调用者取消不取消该批次。"""
        await cls._flush_pending(draining=False)

    @classmethod
    async def _flush_pending(cls, *, draining: bool) -> None:
        """串行认领待写增量，并在干净事务上下文中启动批次。"""
        async with cls._flush_lock:
            task = cls._inflight
            if task is None and cls._pending:
                if (cls._stopping and not draining) or remaining_timeout(7) <= 2:
                    return
                rows, cls._pending = cls._pending, {}
                budget = ShutdownBudget.start(remaining_timeout(7))
                budget.parent = current_budget.get()
                cls._write_budget = budget
                task = Context().run(
                    asyncio.create_task,
                    cls._persist(rows, budget),
                    name="ai-statistics-write",
                )
                cls._inflight = task
        if task is not None:
            await asyncio.shield(task)

    @classmethod
    async def _persist(cls, rows, budget: ShutdownBudget) -> None:
        """仅回填已确认回滚的批次，未确认结果保留且停止自动重放。"""
        try:
            from zhenxun.models.ai_model_stat import AIModelStat, ModelStatWriteError
            from zhenxun.services.db_context import with_db_timeout
        except Exception as error:
            cls._merge(rows)
            cls._write_budget = None
            cls._inflight = None
            cls._failures += 1
            logger.warning(f"模型统计写入尚未开始，保留批次：{error}")
            return

        token = current_budget.set(budget)
        try:
            try:
                await with_db_timeout(
                    AIModelStat.add(rows),
                    timeout=max(0, budget.remaining() - 2),
                    operation="AIModelStat.add",
                    source="ai_statistics",
                )
            except ModelStatWriteError as error:
                if error.outcome != "rolled_back":
                    cls._uncertain = rows
                    logger.error(f"模型统计提交结果暂不可确认，该批次不再重放：{error}")
                    return
                cls._merge(rows)
                message = f"模型统计批次已回滚，保留 {len(rows)} 个桶待重试：{error}"
                (logger.warning if not cls._failures else logger.debug)(message)
                cls._failures += 1
                return
            except BaseException as error:
                cls._uncertain = rows
                logger.error(
                    f"模型统计写入未完成确认，{len(rows)} 个桶不再重放："
                    f"{type(error).__name__}: {error}"
                )
                return
            if cls._failures:
                logger.info(f"模型统计写入已恢复，已提交 {len(rows)} 个桶")
            else:
                logger.info(
                    f"已保存模型调用统计：{len(rows)} 个桶，"
                    f"{sum(values[0] for values in rows.values())} 次调用"
                )
            cls._failures = 0
        finally:
            current_budget.reset(token)
            cls._write_budget = None
            cls._inflight = None

    @classmethod
    def _merge(cls, rows) -> None:
        """合并已确认未写入的增量，保留期间新到达的记录。"""
        for key, values in rows.items():
            pending = cls._pending.setdefault(key, [0, 0, 0, 0, 0])
            for index, value in enumerate(values):
                pending[index] += value

    @classmethod
    async def _loop(cls) -> None:
        while True:
            await asyncio.sleep(cls.FLUSH_INTERVAL)
            await cls.flush()

    @classmethod
    def start(cls) -> None:
        """启动定时写入"""
        if cls._task is None or cls._task.done():
            cls._stopping = False
            token = current_budget.set(None)
            try:
                cls._task = asyncio.create_task(cls._loop())
            finally:
                current_budget.reset(token)

    @classmethod
    async def shutdown(cls) -> None:
        """在关闭预算内结算在途批次，再尝试写入剩余增量。"""
        with shutdown_budget(10):
            async with cls._shutdown_lock:
                cls._stopping = True
                timer, cls._task = cls._task, None
                if timer is not None:
                    timer.cancel()
                    await asyncio.wait({timer}, timeout=remaining_timeout(1))
                    if timer.done() and not timer.cancelled():
                        timer.exception()
                if cls._write_budget is not None and current_budget.get() is not None:
                    cls._write_budget.tighten(current_budget.get())
                while cls._inflight is not None or cls._pending:
                    if remaining_timeout(7) <= 2:
                        break
                    failures = cls._failures
                    waiter = asyncio.create_task(cls._flush_pending(draining=True))
                    done, _ = await asyncio.wait(
                        {waiter}, timeout=max(0, remaining_timeout(7) - 2)
                    )
                    if not done:
                        waiter.cancel()
                        await asyncio.gather(waiter, return_exceptions=True)
                        break
                    await waiter
                    if cls._failures != failures:
                        break
                if cls._inflight is not None:
                    cls._inflight.cancel()
                    await asyncio.wait({cls._inflight}, timeout=remaining_timeout(2))
                if (
                    cls._inflight is not None
                    or cls._uncertain is not None
                    or cls._pending
                ):
                    logger.warning(
                        "模型统计关闭时仍有未确认或待写增量，未报告全部保存；"
                        f"pending={len(cls._pending)}, "
                        f"unknown={len(cls._uncertain or {})}, "
                        f"inflight={cls._inflight is not None}"
                    )

    @classmethod
    def snapshot(cls) -> dict[tuple[datetime, str, str], list[int]]:
        """尚未写入数据库的统计

        返回:
            dict[tuple[datetime, str, str], list[int]]: 与 ai_model_stat 同结构的增量
        """
        return {key: list(values) for key, values in cls._pending.items()}
