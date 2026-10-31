"""
模型调用统计

每次线路请求的结果先累计在内存，按小时分桶，定时批量写入 ai_model_stat。
"""

import asyncio
from datetime import datetime, timezone
from typing import Any, ClassVar

from zhenxun.services.ai.utils.logger import log_llm as logger
from zhenxun.services.lifecycle.deadline import current_budget


class ModelStats:
    """按“小时 × 模型线路 × 任务类型”累计调用次数、失败、耗时与 token"""

    FLUSH_INTERVAL = 60
    _pending: ClassVar[dict[tuple[datetime, str, str], list[int]]] = {}
    _task: ClassVar[asyncio.Task | None] = None

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
        """把内存中的统计写入数据库，失败时放回下次再写"""
        if not cls._pending:
            return
        from zhenxun.models.ai_model_stat import AIModelStat

        rows, cls._pending = cls._pending, {}
        try:
            await AIModelStat.add(rows)
        except Exception as e:
            logger.warning(f"保存模型调用统计失败: {e}")
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
            token = current_budget.set(None)
            try:
                cls._task = asyncio.create_task(cls._loop())
            finally:
                current_budget.reset(token)

    @classmethod
    async def shutdown(cls) -> None:
        """停止定时写入并写入剩余统计"""
        if cls._task:
            cls._task.cancel()
            try:
                await cls._task
            except asyncio.CancelledError:
                pass
            cls._task = None
        await cls.flush()

    @classmethod
    def snapshot(cls) -> dict[tuple[datetime, str, str], list[int]]:
        """尚未写入数据库的统计

        返回:
            dict[tuple[datetime, str, str], list[int]]: 与 ai_model_stat 同结构的增量
        """
        return {key: list(values) for key, values in cls._pending.items()}
