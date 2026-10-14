import hashlib
import json
from pathlib import Path
from typing import Any

from zhenxun.configs.config import BotConfig
from zhenxun.utils.atomic_json import mutate_json_locked, read_json_locked

_STATE_PATH = Path("data/runtime/startup-reconcile-v1.json")


def payload_fingerprint(payload: Any) -> str:
    """计算启动同步内容的指纹

    参数:
        payload: 可 JSON 序列化的同步内容

    返回:
        str: sha256 指纹
    """
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _database_identity() -> str:
    return hashlib.sha256(str(BotConfig.db_url or "").encode()).hexdigest()


def reconcile_matches(section: str, fingerprint: str) -> bool:
    """当前数据库上次同步的指纹是否与本次一致，一致时可跳过同步

    参数:
        section: 同步项名称
        fingerprint: 本次指纹

    返回:
        bool: 是否一致
    """
    state = read_json_locked(_STATE_PATH, {})
    return bool(
        state.get("version") == 1
        and state.get("database") == _database_identity()
        and state.get("sections", {}).get(section) == fingerprint
    )


def commit_reconcile(section: str, fingerprint: str) -> None:
    """记录同步完成后的指纹，数据库变化时清空旧记录

    参数:
        section: 同步项名称
        fingerprint: 本次指纹
    """

    def update(state: dict[str, Any]) -> None:
        if state.get("database") != _database_identity():
            state.clear()
        state["version"] = 1
        state["database"] = _database_identity()
        sections = state.setdefault("sections", {})
        sections[section] = fingerprint

    mutate_json_locked(_STATE_PATH, {}, update, quarantine_corrupt=True)
