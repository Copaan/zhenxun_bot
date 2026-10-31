"""AI 模型调用统计：按模型的调用次数、成功率、耗时与 token 趋势"""

from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from fastapi import APIRouter, Query
from tortoise.timezone import localtime

from ....base_model import Result
from ....utils import authentication

router = APIRouter()

RANGES = {"24h": (timedelta(hours=24), "hour"), "7d": (timedelta(days=7), "day")}
RANGES["30d"] = (timedelta(days=30), "day")


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _bucket(hour: datetime, unit: str) -> datetime:
    """统计小时所属的趋势分组（本地时间的整点或零点）"""
    local = _aware(hour).astimezone()
    if unit == "day":
        return local.replace(hour=0, minute=0, second=0, microsecond=0)
    return local.replace(minute=0, second=0, microsecond=0)


def _summary(values: list[int]) -> dict[str, Any]:
    calls, failures, latency, prompt, completion = values
    return {
        "calls": calls,
        "failures": failures,
        "success_rate": round((calls - failures) / calls, 4) if calls else None,
        "avg_latency": round(latency / calls) if calls else None,
        "prompt_tokens": prompt,
        "completion_tokens": completion,
    }


@router.get("/stats", dependencies=[authentication()], response_model=Result)
async def model_stats(
    period: Literal["24h", "7d", "30d"] = "24h",
    model: str | None = Query(None, max_length=255),
) -> Result:
    from zhenxun.models.ai_model_stat import COUNTERS, AIModelStat
    from zhenxun.services.ai.llm.stats import ModelStats

    span, unit = RANGES[period]
    now = datetime.now(timezone.utc)
    since = (now - span).replace(minute=0, second=0, microsecond=0)
    rows: dict[tuple[datetime, str, str], list[int]] = {}
    for row in await AIModelStat.filter(hour__gte=localtime(since)):
        key = (_aware(row.hour), row.model, row.task)
        rows[key] = [getattr(row, name) for name in COUNTERS]
    for (hour, name, task), values in ModelStats.snapshot().items():
        current = rows.setdefault((hour, name, task), [0] * len(COUNTERS))
        for index, value in enumerate(values):
            current[index] += value
    models: dict[tuple[str, str], list[int]] = {}
    series: dict[datetime, list[int]] = {}
    total = [0] * len(COUNTERS)
    step = timedelta(hours=1) if unit == "hour" else timedelta(hours=26)
    cursor = _bucket(since, unit)
    while cursor <= _bucket(now, unit):
        series[cursor] = [0] * len(COUNTERS)
        cursor = _bucket(cursor + step, unit)
    for (hour, name, task), values in rows.items():
        if hour < since:
            continue
        current = models.setdefault((name, task), [0] * len(COUNTERS))
        for index, value in enumerate(values):
            current[index] += value
        if model and name != model:
            continue
        bucket = series.setdefault(_bucket(hour, unit), [0] * len(COUNTERS))
        for index, value in enumerate(values):
            bucket[index] += value
            total[index] += value
    return Result.ok(
        {
            "period": period,
            "unit": unit,
            "total": _summary(total),
            "series": [
                {"time": time.isoformat(), **_summary(values)}
                for time, values in sorted(series.items())
            ],
            "models": sorted(
                (
                    {"model": name, "task": task, **_summary(values)}
                    for (name, task), values in models.items()
                ),
                key=lambda item: -int(item["calls"]),
            ),
        }
    )
