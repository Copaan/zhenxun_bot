from __future__ import annotations


class MigrationError(RuntimeError):
    """Only stable codes and relative paths may cross the public API boundary."""

    def __init__(
        self,
        code: str,
        *,
        path: str | None = None,
        status: int = 400,
        details: dict | None = None,
    ):
        self.code = code
        self.path = path
        self.status = status
        self.details = details or {}
        super().__init__(code)

    def public(self) -> dict:
        return {
            "code": self.code,
            **({"path": self.path} if self.path else {}),
            **self.details,
        }


def conflict_error(conflicts: list[dict], *, directory=None) -> MigrationError:
    """Persist the complete conflict list and return a bounded public summary."""
    advice = {
        "external_root_mapping_required": "将外部目录的数据放入实例内后重新导出",
        "migration_target_protected": "核对路径；不能覆盖本体代码和运行目录",
        "migration_configuration_alias_forbidden": "检查本体配置的 YAML 别名",
        "migration_configuration_shape_invalid": "检查需要合并的本体配置结构",
        "migration_configuration_ambiguous_key": "消除配置中的重复或大小写歧义键",
        "migration_file_unreadable": "检查文件是否存在、可读且未被其他程序占用",
        "migration_link_forbidden": "将链接替换为实例内的实际数据文件后重新检查",
    }
    items = [
        {
            "path": item["path"],
            "reason": item["reason"],
            "suggestion": advice.get(item["reason"], "核对该路径及冲突原因后重新预检"),
        }
        for item in conflicts
    ]
    if directory is not None:
        from zhenxun.utils.atomic_json import write_json_locked

        write_json_locked(directory / "conflicts.json", {"items": items})
    return MigrationError(
        "migration_conflicts_unresolved",
        status=409,
        details={"conflict_total": len(items), "conflicts": items[:20]},
    )
