from __future__ import annotations

from dataclasses import dataclass

from .discovery import CATEGORIES, category_for
from .errors import MigrationError
from .paths import logical_path

DIRECTORY_SCOPES = {
    "configuration": ("data/config", "data/configs", "certs", "certificates"),
    "data": ("data",),
    "plugins": ("zhenxun/plugins", "plugins"),
    "resources": ("resources",),
}
CONFIGURATION_FILES = (
    ".env",
    ".env.dev",
    "config.yaml",
    "config.yml",
    "data/config.yaml",
    "data/config.yml",
)


@dataclass(frozen=True)
class ReplacementSelection:
    categories: frozenset[str]
    directories: tuple[str, ...]
    files: tuple[str, ...]

    def __post_init__(self):
        if not self.categories or not self.categories <= CATEGORIES:
            raise MigrationError("migration_categories_invalid")
        allowed = {
            path for category in self.categories for path in DIRECTORY_SCOPES[category]
        }
        if (
            len(self.directories) != len(set(self.directories))
            or not set(self.directories) <= allowed
        ):
            raise MigrationError("migration_replacement_scope_invalid")
        object.__setattr__(
            self,
            "directories",
            tuple(
                path
                for path in self.directories
                if not any(
                    path.startswith(parent + "/")
                    for parent in self.directories
                    if parent != path
                )
            ),
        )
        if len(self.files) > 1000 or len(self.files) != len(set(self.files)):
            raise MigrationError("migration_replacement_scope_invalid")
        for path in self.files:
            logical_path(path)
            if "configuration" not in self.categories or (
                path not in CONFIGURATION_FILES
                and not ("/" not in path and path.startswith(".env."))
            ):
                raise MigrationError("migration_replacement_scope_invalid")
        object.__setattr__(
            self,
            "files",
            tuple(
                path
                for path in self.files
                if not any(path.startswith(parent + "/") for parent in self.directories)
            ),
        )

    def contains(self, path: str) -> bool:
        logical_path(path)
        return path in self.files or any(
            path.startswith(parent + "/") for parent in self.directories
        )

    def public(self) -> dict:
        return {
            "schema": 1,
            "categories": sorted(self.categories),
            "directories": list(self.directories),
            "files": list(self.files),
        }

    @classmethod
    def from_manifest(cls, manifest: dict, *, legacy: dict | None = None):
        value = manifest.get("source", {}).get("replacement_selection")
        if value is None:
            value = legacy
        if value is None:
            raise MigrationError(
                "migration_legacy_scope_confirmation_required", status=409
            )
        if not isinstance(value, dict) or value.get("schema") != 1:
            raise MigrationError("migration_replacement_scope_invalid")
        for field in ("categories", "directories", "files"):
            if not isinstance(value.get(field), list) or not all(
                isinstance(item, str) for item in value[field]
            ):
                raise MigrationError("migration_replacement_scope_invalid")
        result = cls(
            frozenset(value["categories"]),
            tuple(value["directories"]),
            tuple(value["files"]),
        )
        for entry in manifest.get("files", []):
            if entry.get("root") != "project":
                raise MigrationError("migration_external_root_unsupported")
            category = entry.get("category")
            if category == "database":
                category = "data"
            if category not in result.categories:
                raise MigrationError("migration_selection_payload_mismatch")
            path = entry["path"]
            if entry["category"] == "database":
                primary = manifest.get("source", {}).get("primary_database") or {}
                engine = primary.get("engine")
                if engine in {"mysql", "postgres"}:
                    # Logical backups belong to the database transaction, never
                    # to the authority to replace ordinary project directories.
                    if (
                        primary.get("root") != "project"
                        or primary.get("path") != path
                        or path != f"migration/database/{engine}.backup"
                    ):
                        raise MigrationError("migration_database_payload_scope_invalid")
                    continue
            if not result.contains(path):
                raise MigrationError("migration_selection_payload_outside_scope")
            if entry["category"] != "database" and category_for(path) != category:
                raise MigrationError("migration_selection_payload_mismatch")
        return result

    def restore_options(self):
        from .restore import RestoreOptions

        return RestoreOptions(
            categories=self.categories,
            files="exact",
            configuration="backup",
            exact_directories=self.directories,
            exact_files=self.files,
            replace_configuration_scope="configuration" in self.categories,
        )


def export_selection(
    categories: frozenset[str], paths: list[str]
) -> ReplacementSelection:
    directories = tuple(
        path for key in sorted(categories) for path in DIRECTORY_SCOPES[key]
    )
    files = set(CONFIGURATION_FILES) if "configuration" in categories else set()
    files.update(path for path in paths if "/" not in path and path.startswith(".env"))
    return ReplacementSelection(categories, directories, tuple(sorted(files)))
