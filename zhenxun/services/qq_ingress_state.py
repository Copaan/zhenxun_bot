from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import psutil

from zhenxun.utils.atomic_json import mutate_json_locked, read_json_locked


def _path() -> Path:
    return Path(
        os.getenv("ZHENXUN_QQ_INGRESS_STATE_PATH", "data/runtime/qq-ingress-v1.json")
    )


def listener_identity() -> dict[str, Any]:
    """当前监听进程的身份：PID、创建时间与 launcher 启动标识

    返回:
        dict[str, Any]: 身份信息
    """
    return {
        "pid": os.getpid(),
        "process_created_at": psutil.Process().create_time(),
        "launcher_boot_id": os.getenv("ZHENXUN_LAUNCHER_BOOT_ID", ""),
    }


def listener_identity_matches(value: dict[str, Any]) -> bool:
    """状态记录是否属于本次 launcher 启动的、仍在运行的监听进程

    参数:
        value: 状态记录

    返回:
        bool: 是否匹配
    """
    expected_boot = os.getenv("ZHENXUN_LAUNCHER_BOOT_ID", "")
    if (
        not expected_boot
        or value.get("launcher_boot_id") != expected_boot
        or not value.get("startup_id")
    ):
        return False
    try:
        pid = int(value["pid"])
        created = float(value["process_created_at"])
        return pid > 0 and abs(psutil.Process(pid).create_time() - created) < 0.01
    except (psutil.Error, KeyError, ValueError, TypeError):
        return False


def publish_ingress_state(state: str, *, port: int) -> None:
    value = {
        **listener_identity(),
        "startup_id": os.getenv("ZHENXUN_QQ_INGRESS_STARTUP_ID", ""),
        "state": state,
        "port": port,
    }
    mutate_json_locked(_path(), {}, lambda current: current.update(value))


def read_ingress_state() -> dict:
    value = read_json_locked(_path(), {})
    if not isinstance(value, dict):
        return {"state": "unknown"}
    if not listener_identity_matches(value):
        return {"state": "unknown", "last_error": "listener_identity_unverified"}
    return value
