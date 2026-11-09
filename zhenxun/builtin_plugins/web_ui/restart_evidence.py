from __future__ import annotations

import os
from typing import Any

from zhenxun.configs.config import Config
from zhenxun.services.startup import startup_coordinator
from zhenxun.utils._restart_utils import restart_value_digest
from zhenxun.utils.restart_state import (
    _PENDING_RESTARTS_KEY,
    mutate_restart_state,
    read_restart_state,
)

CONFIGURATION_SOURCES = {
    "webui.environment",
    "webui.protocol",
    "webui.config",
    "webui.ai",
}


def _values(kind: str, *, startup: bool = False) -> dict[str, Any]:
    from zhenxun.services.runtime_config import (
        _STARTUP_SIMPLE_DATA,
        _canonical_values,
        _env_source,
        _flatten_simple_config,
        normalize_env,
    )

    if kind == "environment":
        return _canonical_values(normalize_env(_env_source().read_text("utf-8")))
    if startup:
        data = _STARTUP_SIMPLE_DATA
    else:
        from .config_validation import _yaml_parser

        data = _yaml_parser().load(Config._simple_file.read_text("utf-8")) or {}
    return {
        f"{module}.{key}": value
        for (module, key), value in _flatten_simple_config(data).items()
    }


def configuration_evidence(
    source: str, reasons: list[str], keys: list[str], consumers: list[str]
) -> dict[str, Any]:
    """登记配置目标及消费者，只持久化摘要。"""
    kind = (
        "environment" if source in {"webui.environment", "webui.protocol"} else "simple"
    )
    previous = read_restart_state().get(_PENDING_RESTARTS_KEY, {}).get(source, {})
    prior = previous.get("evidence") or {}
    selected = set(keys)
    selected.update(
        reason[12:] for reason in reasons if reason.startswith("environment:")
    )
    if prior.get("kind") == kind:
        selected.update(prior.get("targets", {}))
        consumers = sorted(set(consumers) | set(prior.get("consumers", [])))
    values = _values(kind)
    return {
        "kind": kind,
        "targets": {
            key: restart_value_digest(values.get(key)) for key in sorted(selected)
        },
        "consumers": consumers,
    }


def _consumers(
    evidence: dict[str, Any], snapshot: dict[str, Any]
) -> tuple[str, str] | None:
    from nonebot import get_plugin

    from zhenxun.services.lifecycle import lifecycle_kernel

    keys = set(evidence.get("targets", {}))
    paths = {key.upper() for key in keys}
    if evidence.get("kind") == "simple":
        paths.update(key.rsplit(".", 1)[0].upper() for key in keys)
    components = set(lifecycle_kernel.configuration_components(paths))
    plugins = set()
    for name in evidence.get("consumers", []):
        (components if lifecycle_kernel.component_status(name) else plugins).add(name)
    if evidence.get("kind") == "simple":
        for (module, key), readers in Config.import_readers.items():
            if f"{module}.{key}" in keys or f"{module}.*" in keys:
                plugins.update(readers)
    for name in sorted(components):
        status = lifecycle_kernel.component_status(name) or {}
        if status.get("state") in {"failed", "degraded", "stopped", "stopping"}:
            return "attention_required", f"configuration_component_failed:{name}"
        if status.get("state") != "ready":
            return "verifying", "configuration_consumer_starting"
    failures = {
        item.get("source_id")
        for item in snapshot.get("degraded_reasons", [])
        if item.get("source_type") == "plugin"
    }
    for name in sorted(plugins):
        if get_plugin(name) is None or name in failures:
            return "attention_required", f"configuration_plugin_unavailable:{name}"
    return None


def refresh_configuration_evidence(source: str) -> None:
    """按当前保存内容更新已有目标，保留并发保存的新记录。"""
    previous = read_restart_state().get(_PENDING_RESTARTS_KEY, {}).get(source)
    if not previous:
        return
    evidence = configuration_evidence(source, previous.get("reasons", []), [], [])

    def refresh(state):
        record = state.get(_PENDING_RESTARTS_KEY, {}).get(source)
        if record != previous or record.get("evidence") == evidence:
            return False
        record["evidence"] = evidence
        return True

    mutate_restart_state(refresh, write_if=bool)


def configuration_state(
    source: str, record: dict[str, Any], snapshot: dict[str, Any]
) -> tuple[str, str]:
    """核对当前 worker 的实际配置及相关消费者状态。"""
    current_id = os.getenv("ZHENXUN_WORKER_STARTUP_ID", "")
    bound = bool(current_id and record.get("verification_startup_id") == current_id)
    same_boot = record.get("boot_id") == snapshot.get("boot_id")
    if not bound and not same_boot:
        return "unknown", "restart_worker_identity_unconfirmed"
    evidence = record.get("evidence") or {}
    kind = evidence.get("kind")
    if not kind and source in {"webui.environment", "webui.protocol"}:
        keys = [
            r[12:] for r in record.get("reasons", []) if r.startswith("environment:")
        ]
        values = _values("environment")
        evidence = {
            "kind": "environment",
            "targets": {key: restart_value_digest(values.get(key)) for key in keys},
        }
        kind = "environment"
    targets = evidence.get("targets")
    if (
        kind not in {"environment", "simple"}
        or not isinstance(targets, dict)
        or not targets
    ):
        return "unknown", "restart_evidence_missing"
    values = _values(kind)
    if any(
        restart_value_digest(values.get(key)) != digest
        for key, digest in targets.items()
    ):
        return "attention_required", "configuration_target_changed"
    stages = snapshot.get("stages", {})
    stage = "management" if kind == "environment" else "runtime"
    runtime = stages.get(stage, {}).get("state")
    if runtime not in {"completed", "failed", "skipped"}:
        return "verifying", "configuration_consumer_starting"
    if runtime != "completed":
        return "attention_required", "configuration_runtime_unavailable"
    if kind == "environment":
        from .api.tabs.system.configuration import _environment_fields

        fields = _environment_fields(values)
        for key in targets:
            field = fields.get(key, {})
            if field.get("overridden"):
                return "attention_required", f"environment_overridden:{key}"
            if not field.get("runtime_available"):
                return "unknown", f"environment_effective_value_unknown:{key}"
            if field.get("apply_state") != "applied":
                return (
                    ("restart_pending", "configuration_not_applied")
                    if same_boot and not bound
                    else ("attention_required", f"environment_not_applied:{key}")
                )
    else:
        startup = _values(kind, startup=True)
        if any(
            restart_value_digest(startup.get(key)) != digest
            for key, digest in targets.items()
        ):
            return (
                ("restart_pending", "configuration_not_applied")
                if same_boot and not bound
                else ("attention_required", "configuration_startup_value_mismatch")
            )
    consumer = _consumers(evidence, snapshot)
    if consumer is not None:
        return consumer
    return "applied", "configuration_verified"


def reconcile_configuration_restarts() -> dict[str, dict[str, Any]]:
    """按 worker 身份和记录修订核销已生效配置，其他记录保留。"""
    state = read_restart_state()
    records = state.get(_PENDING_RESTARTS_KEY, {})
    snapshot = startup_coordinator.snapshot()
    results = {}
    confirmed = {}
    for source, record in records.items():
        if source not in CONFIGURATION_SOURCES:
            continue
        try:
            status, reason = configuration_state(source, record, snapshot)
        except Exception:
            status, reason = "unknown", "configuration_verification_unavailable"
        results[source] = {
            "source": source,
            "state": status,
            "reason": reason,
            "reasons": record.get("reasons", []),
            "change_id": record.get("change_id"),
            "updated_at": record.get("updated_at", 0),
        }
        if status == "applied":
            confirmed[source] = record
    if confirmed:
        current_id = os.getenv("ZHENXUN_WORKER_STARTUP_ID", "")
        acknowledged = set()

        def acknowledge(current):
            if current.get("verification_startup_id", "") != current_id:
                return False
            pending = current.get(_PENDING_RESTARTS_KEY, {})
            changed = False
            for source, expected in confirmed.items():
                if pending.get(source) == expected:
                    pending.pop(source)
                    acknowledged.add(source)
                    changed = True
            return changed

        mutate_restart_state(acknowledge, write_if=bool)
        for source in confirmed.keys() - acknowledged:
            results[source].update(state="unknown", reason="restart_record_changed")
    return results
