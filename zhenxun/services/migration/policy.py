"""恢复模式与历史任务的固定执行策略。"""

from __future__ import annotations

MODES = frozenset(
    {"empty", "replace_database", "replace_tables", "merge_upsert", "merge_skip"}
)


def mode(request: dict) -> str:
    """读取已绑定模式，历史任务沿用原先的空库或包表替换行为。"""
    value = request.get("restore_mode")
    if value is None:
        return (
            "replace_tables"
            if request.get("kind") == "restore" and request.get("snapshot_required")
            else "empty"
        )
    if value not in MODES:
        raise RuntimeError("migration_restore_mode_invalid")
    return value


def requires_empty(request: dict) -> bool:
    return request.get("kind") == "switch" or mode(request) == "empty"


def original_snapshot(request: dict) -> bool:
    return bool(
        request.get("original_snapshot_required", request.get("snapshot_required"))
    )


def target_backup(request: dict) -> bool:
    if "restore_mode" not in request or requires_empty(request):
        return False
    return mode(request) == "replace_database" and request.get(
        "backup_before_replace", True
    )


def selected_tables(request: dict, available) -> set[str]:
    tables = set(available)
    selected = request.get("included_tables")
    if selected is None:
        return tables
    if (
        not isinstance(selected, list)
        or not selected
        or any(not isinstance(name, str) or not name for name in selected)
    ):
        raise RuntimeError("migration_included_tables_invalid")
    selected = set(selected)
    if selected - tables:
        raise RuntimeError("migration_included_table_missing")
    if mode(request) == "replace_database" and selected != tables:
        raise RuntimeError("migration_database_replace_requires_all_tables")
    return selected
