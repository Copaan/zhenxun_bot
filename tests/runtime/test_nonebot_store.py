from pathlib import Path
import subprocess
import sys
from typing import Any, ClassVar

import httpx
import pytest

from zhenxun.nonebot_store import dependencies, registry, runtime, storage


def test_orm_migration_import_does_not_require_plugin_private_sqlalchemy() -> None:
    code = """
import importlib.abc
import sys

class BlockSqlalchemy(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == 'sqlalchemy' or fullname.startswith('sqlalchemy.'):
            raise ModuleNotFoundError(fullname)
        return None

sys.meta_path.insert(0, BlockSqlalchemy())
import zhenxun.nonebot_store.orm_migration
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path.cwd(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        ("install", "已安装并热加载"),
        ("update", "已更新并热加载"),
        ("uninstall", "已卸载并热加载"),
    ],
)
def test_nonebot_hot_apply_info_matches_action(action: str, expected: str) -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage.nonebot_store import (
        _hot_apply_info,
    )

    assert _hot_apply_info(action) == f"插件{expected}"


def test_orm_migration_loads_only_operations_that_own_models(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from zhenxun.nonebot_store import orm_migration, runtime

    loaded: list[str] = []
    monkeypatch.setattr(runtime, "_load_managed_plugin", loaded.append)
    orm_migration._load_target_plugins(
        {
            "operations": [
                {
                    "module_name": "nonebot_plugin_plain",
                    "database_migration_possible": False,
                },
                {
                    "module_name": "nonebot_plugin_models",
                    "database_migration_possible": True,
                },
            ],
            "target_manifest": {
                "plugins": {
                    "nonebot:plain": {
                        "state": "managed",
                        "module_name": "nonebot_plugin_plain",
                    },
                    "nonebot:models": {
                        "state": "managed",
                        "module_name": "nonebot_plugin_models",
                    },
                }
            },
        }
    )

    assert loaded == ["nonebot_plugin_models"]


def test_catalog_marks_failed_transaction_as_failed() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage.nonebot_store import (
        _catalog_item,
    )

    item = _catalog_item(
        _registry_entry(),
        manifest=storage.default_manifest(),
        inventory={},
        pending={
            "project_link": "nonebot-plugin-demo",
            "action": "install",
            "state": "failed",
            "error_code": "plugin_startup_verification_failed",
            "failure_reasons": [
                {"code": "plugin_configuration_required", "paths": ["demo_token"]}
            ],
        },
    )

    assert item["install_state"] == "failed"
    assert item["apply_mode"] == "failed"
    assert item["pending_action"] is None
    assert item["failure_reasons"] == [
        {
            "code": "plugin_configuration_required",
            "store_key": None,
            "module_name": None,
            "paths": ["demo_token"],
        }
    ]


def test_catalog_filters_batch_failures_by_stable_plugin_identity() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage.nonebot_store import (
        _catalog_item,
    )

    item = _catalog_item(
        _registry_entry(),
        manifest=storage.default_manifest(),
        inventory={},
        pending={
            "state": "failed",
            "operations": [
                {
                    "project_link": "nonebot-plugin-demo",
                    "action": "install",
                }
            ],
            "failure_reasons": [
                {
                    "code": "plugin_configuration_required",
                    "store_key": "nonebot:nonebot-plugin-demo",
                    "module_name": "nonebot_plugin_demo",
                    "paths": ["demo_token"],
                },
                {
                    "code": "plugin_import_failed",
                    "store_key": "nonebot:nonebot-plugin-other",
                    "module_name": "nonebot_plugin_other",
                    "paths": [],
                },
            ],
        },
    )

    assert item["failure_reasons"] == [
        {
            "code": "plugin_configuration_required",
            "store_key": "nonebot:nonebot-plugin-demo",
            "module_name": "nonebot_plugin_demo",
            "paths": ["demo_token"],
        }
    ]


def test_catalog_reads_matching_operation_from_pending_batch() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage.nonebot_store import (
        _catalog_item,
    )

    item = _catalog_item(
        _registry_entry(),
        manifest=storage.default_manifest(),
        inventory={},
        pending={
            "state": "pending_restart",
            "operations": [
                {
                    "operation_id": "operation-1",
                    "project_link": "nonebot-plugin-demo",
                    "action": "install",
                }
            ],
        },
    )

    assert item["apply_mode"] == "restart_pending"
    assert item["pending_action"] == "install"
    assert item["pending_operation_id"] == "operation-1"


def test_effective_manifest_uses_staged_target() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage.nonebot_store import (
        _effective_manifest,
    )

    target = {**storage.default_manifest(), "plugins": {"nonebot:demo": {}}}

    assert (
        _effective_manifest({"state": "pending_restart", "target_manifest": target})
        == target
    )


def test_stage_generation_does_not_switch_active_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_file = tmp_path / "manifest.json"
    pending_file = tmp_path / "pending.json"
    layer_root = tmp_path / "layers"
    generation = layer_root / "generation-2"
    generation.mkdir(parents=True)
    monkeypatch.setattr(storage, "MANIFEST_FILE", manifest_file)
    monkeypatch.setattr(storage, "PENDING_FILE", pending_file)
    monkeypatch.setattr(storage, "LAYER_ROOT", layer_root)
    monkeypatch.setattr(runtime, "PENDING_FILE", pending_file)
    storage.save_manifest({**storage.default_manifest(), "active_generation": 1})
    transaction = {
        "target_manifest": {
            **storage.default_manifest(),
            "active_generation": 1,
        }
    }

    runtime.stage_generation(
        transaction,
        {
            "generation": 2,
            "path": generation,
            "digest": "digest",
            "native_extensions": [],
        },
    )

    assert storage.load_manifest()["active_generation"] == 1
    assert storage.pending_transaction()["generation"] == 2
    assert storage.pending_transaction()["state"] == "pending_restart"


def test_managed_orm_sqlite_backup_can_restore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.nonebot_store import orm_migration

    database = tmp_path / "plugin.sqlite3"
    with orm_migration.sqlite3.connect(database) as connection:
        connection.execute("create table demo (value text)")
        connection.execute("insert into demo values ('before')")
    monkeypatch.setattr(orm_migration, "STORE_ROOT", tmp_path / "store")
    monkeypatch.setattr(
        orm_migration,
        "ORM_MIGRATION_STATUS_FILE",
        tmp_path / "store" / "migration-status.json",
    )
    monkeypatch.setattr(
        orm_migration,
        "pending_transaction",
        lambda: {"revision": "revision-1"},
    )

    orm_migration._backup_sqlite([database], "revision-1")
    with orm_migration.sqlite3.connect(database) as connection:
        connection.execute("delete from demo")
        connection.execute("insert into demo values ('after')")

    assert orm_migration.restore() == 0
    with orm_migration.sqlite3.connect(database) as connection:
        assert connection.execute("select value from demo").fetchone() == ("before",)


def test_environment_fingerprint_includes_pending_revision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock = tmp_path / "uv.lock"
    lock.write_text("version = 1\n", encoding="utf-8")
    monkeypatch.setattr(dependencies, "LOCK_FILE", lock)
    monkeypatch.setattr(
        dependencies,
        "environment_report",
        lambda: {"fingerprint": "environment"},
    )

    first = dependencies.environment_fingerprint(
        _registry_entry(), pending_revision="first"
    )
    second = dependencies.environment_fingerprint(
        _registry_entry(), pending_revision="second"
    )

    assert first != second


def test_safe_plugin_import_failure_extracts_only_validation_paths() -> None:
    from pydantic import BaseModel, ValidationError

    class DemoConfig(BaseModel):
        demo_token: str

    try:
        DemoConfig.model_validate({})
    except ValidationError as validation_error:
        wrapped = NameError("sensitive plugin exception")
        wrapped.__context__ = validation_error
        result = runtime._safe_plugin_import_failure(wrapped)
    else:
        raise AssertionError("validation should fail")

    assert result == {
        "code": "plugin_configuration_required",
        "paths": ["demo_token"],
    }


def _registry_entry(**overrides: Any) -> dict[str, Any]:
    return {
        "module_name": "nonebot_plugin_demo",
        "project_link": "nonebot-plugin-demo",
        "name": "Demo",
        "version": "1.0.0",
        "valid": True,
        **overrides,
    }


class _RegistryResponse:
    def __init__(
        self,
        status_code: int,
        payload: Any = None,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            request = httpx.Request("GET", "https://registry.invalid/plugins.json")
            response = httpx.Response(self.status_code, request=request)
            raise httpx.HTTPStatusError(
                "registry error", request=request, response=response
            )

    def json(self) -> Any:
        return self._payload


class _RegistryClient:
    responses: ClassVar[list[_RegistryResponse]] = []
    requests: ClassVar[list[dict[str, str] | None]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def __aenter__(self) -> "_RegistryClient":
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def get(
        self, url: str, *, headers: dict[str, str] | None = None
    ) -> _RegistryResponse:
        self.requests.append(headers)
        return self.responses.pop(0)


def test_project_core_closure_protects_runtime_packages() -> None:
    protected = dependencies.protected_core()
    shared = dependencies.shared_dependencies()

    assert protected["nonebot2"]
    assert protected["pydantic"]
    assert protected["fastapi"]
    assert protected["nonebot-adapter-onebot"]
    assert "pillow" in shared
    assert "numpy" in shared
    assert "pillow" not in protected


def test_lock_closure_honors_selected_extras_and_resolution_markers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock = tmp_path / "uv.lock"
    lock.write_text(
        """
version = 1

[[package]]
name = "zhenxun-bot"
version = "1.0.0"
dependencies = [{ name = "demo", extra = ["wanted"] }]

[[package]]
name = "demo"
version = "1.0.0"

[package.optional-dependencies]
wanted = [{ name = "helper" }]
unused = [{ name = "unused" }]

[[package]]
name = "helper"
version = "1.0.0"
resolution-markers = ["python_full_version < '3.11'"]

[[package]]
name = "helper"
version = "2.0.0"
resolution-markers = ["python_full_version >= '3.11'"]

[[package]]
name = "unused"
version = "1.0.0"
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(dependencies, "LOCK_FILE", lock)

    closure = dependencies.project_closure()

    assert closure["demo"] == "1.0.0"
    assert closure["helper"] == ("1.0.0" if sys.version_info < (3, 11) else "2.0.0")
    assert "unused" not in closure


def test_metadata_rejects_nonebot1_and_incompatible_pydantic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        dependencies,
        "installed_inventory",
        lambda: {"nonebot2": "2.5.0", "pydantic": "2.13.0"},
    )
    metadata = {
        "info": {
            "requires_python": ">=3.10",
            "requires_dist": ["nonebot>=1.9", "pydantic<=2.0"],
        }
    }

    codes = {item["code"] for item in dependencies.metadata_compatibility(metadata)}

    assert codes == {"nonebot1_plugin", "pydantic_version_incompatible"}


def test_registry_rejects_identity_injection_and_isolates_bad_module() -> None:
    with pytest.raises(ValueError, match="registry_payload_has_no_valid_identity"):
        registry._validate_entries(
            [_registry_entry(project_link="demo\n--extra-index-url=invalid")]
        )

    entries = registry._validate_entries(
        [
            _registry_entry(),
            _registry_entry(
                project_link="nonebot-plugin-bad-module",
                module_name="demo-module",
            ),
        ]
    )

    assert len(entries) == 2
    assert entries[1]["valid"] is False
    assert entries[1]["registry_validation_errors"] == ["registry_module_invalid"]


def test_environment_drift_uses_lock_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        dependencies,
        "protected_core",
        lambda: {"nonebot2": "2.5.0", "pydantic": "2.13.0"},
    )
    monkeypatch.setattr(
        dependencies,
        "installed_inventory",
        lambda: {"nonebot2": "2.4.0", "pydantic": "2.13.0"},
    )

    assert dependencies.environment_drift() == [
        {"name": "nonebot2", "expected": "2.5.0", "actual": "2.4.0"}
    ]


@pytest.mark.asyncio
async def test_registry_uses_etag_and_cached_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache_file = tmp_path / "plugins.json"
    meta_file = tmp_path / "meta.json"
    monkeypatch.setattr(registry, "REGISTRY_CACHE_FILE", cache_file)
    monkeypatch.setattr(registry, "REGISTRY_META_FILE", meta_file)
    storage.write_json(cache_file, [_registry_entry()])
    storage.write_json(
        meta_file,
        {"etag": '"demo-etag"', "fetched_at": "2000-01-01T00:00:00+00:00"},
    )
    _RegistryClient.responses = [_RegistryResponse(304)]
    _RegistryClient.requests = []
    monkeypatch.setattr(registry.httpx, "AsyncClient", _RegistryClient)

    entries, meta = await registry.get_registry(refresh=True)

    assert entries[0]["project_link"] == "nonebot-plugin-demo"
    assert meta["cached"] is True
    assert _RegistryClient.requests == [
        {"Accept": "application/json", "If-None-Match": '"demo-etag"'}
    ]


@pytest.mark.asyncio
async def test_registry_falls_back_to_mirror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(registry, "REGISTRY_CACHE_FILE", tmp_path / "plugins.json")
    monkeypatch.setattr(registry, "REGISTRY_META_FILE", tmp_path / "meta.json")
    _RegistryClient.responses = [
        _RegistryResponse(503),
        _RegistryResponse(200, [_registry_entry()], headers={"etag": '"mirror"'}),
    ]
    _RegistryClient.requests = []
    monkeypatch.setattr(registry.httpx, "AsyncClient", _RegistryClient)

    entries, meta = await registry.get_registry(refresh=True)

    assert entries[0]["module_name"] == "nonebot_plugin_demo"
    assert meta["source"] == registry.REGISTRY_URLS[1]
    assert meta["cached"] is False


@pytest.mark.asyncio
async def test_source_requirements_reject_direct_urls(tmp_path: Path) -> None:
    requirement = tmp_path / "requirements.txt"
    requirement.write_text(
        "demo-plugin @ https://example.invalid/demo.whl\n", encoding="utf-8"
    )

    with pytest.raises(
        dependencies.DependencyAnalysisError,
        match="source_plugin_direct_url_forbidden",
    ):
        await dependencies.preflight_source_requirements([requirement])


@pytest.mark.asyncio
async def test_source_requirements_report_no_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requirement = tmp_path / "requirements.txt"
    requirement.write_text("demo-helper>=1\n", encoding="utf-8")
    monkeypatch.setattr(dependencies, "environment_drift", lambda: [])
    monkeypatch.setattr(
        dependencies, "installed_inventory", lambda: {"demo-helper": "1.0.0"}
    )
    monkeypatch.setattr(dependencies, "protected_core", lambda: {})

    async def compile_stub(*args: Any, **kwargs: Any):
        return {"demo-helper": "1.0.0"}, ""

    monkeypatch.setattr(dependencies, "_compile", compile_stub)

    plan = await dependencies.preflight_source_requirements([requirement])

    assert plan["package_changes"] == {"added": [], "changed": [], "removed": []}


@pytest.mark.asyncio
async def test_source_requirements_detect_source_build_before_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requirement = tmp_path / "requirements.txt"
    requirement.write_text("sdist-only-helper==1.0.0\n", encoding="utf-8")
    monkeypatch.setattr(dependencies, "environment_drift", lambda: [])
    monkeypatch.setattr(dependencies, "installed_inventory", lambda: {})
    monkeypatch.setattr(dependencies, "protected_core", lambda: {})
    calls: list[bool] = []

    async def compile_stub(
        _requirements: list[str],
        _constraints: dict[str, str],
        *,
        wheels_only: bool,
    ):
        calls.append(wheels_only)
        if wheels_only:
            return None, "no compatible wheel"
        return {"sdist-only-helper": "1.0.0"}, ""

    monkeypatch.setattr(dependencies, "_compile", compile_stub)

    plan = await dependencies.preflight_source_requirements([requirement])

    assert calls == [False, True]
    assert plan["source_build_required"] is True
    assert plan["source_build_detail"] == "no compatible wheel"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("candidate_core", "candidate_free", "expected_code"),
    [
        ({"demo": "1.0.0"}, None, "third_party_dependency_conflict"),
        (None, {"demo": "1.0.0"}, "project_dependency_conflict"),
        (None, None, "plugin_dependency_invalid"),
    ],
)
async def test_solver_classifies_unsatisfied_dependency_layers(
    monkeypatch: pytest.MonkeyPatch,
    candidate_core: dict[str, str] | None,
    candidate_free: dict[str, str] | None,
    expected_code: str,
) -> None:
    monkeypatch.setattr(dependencies, "environment_drift", lambda: [])
    monkeypatch.setattr(
        dependencies,
        "environment_report",
        lambda: {
            "interpreter_mismatch": False,
            "incompatible_shared_drift": [],
            "compatible_shared_drift": [],
            "extra_packages": [],
        },
    )
    monkeypatch.setattr(dependencies, "load_manifest", lambda: {"plugins": {}})
    monkeypatch.setattr(dependencies, "installed_inventory", lambda: {"core": "1"})
    monkeypatch.setattr(dependencies, "base_installed_inventory", lambda: {"core": "1"})
    monkeypatch.setattr(dependencies, "protected_core", lambda: {"core": "1"})
    monkeypatch.setattr(dependencies, "shared_dependencies", lambda: {})
    monkeypatch.setattr(dependencies, "project_closure", lambda: {"core": "1"})
    monkeypatch.setattr(dependencies, "project_requirements", lambda: [])
    results = [
        (None, "strict"),
        (None, "core"),
        (candidate_core, "candidate-core"),
    ]
    if candidate_core is None:
        results.append((candidate_free, "candidate-free"))

    async def compile_stub(*args: Any, **kwargs: Any):
        return results.pop(0)

    monkeypatch.setattr(dependencies, "_compile", compile_stub)

    with pytest.raises(dependencies.DependencyAnalysisError) as exc_info:
        await dependencies.solve_install(_registry_entry(), {"info": {}})

    assert exc_info.value.code == expected_code


def test_environment_report_does_not_block_extra_packages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dependencies, "protected_core", lambda: {"nonebot2": "2.5.0"})
    monkeypatch.setattr(
        dependencies, "shared_dependencies", lambda: {"pillow": "10.4.0"}
    )
    monkeypatch.setattr(
        dependencies,
        "base_installed_inventory",
        lambda: {
            "nonebot2": "2.5.0",
            "pillow": "10.4.0",
            "user-plugin-helper": "3.0.0",
        },
    )
    monkeypatch.setattr(dependencies, "layer_inventory", lambda: {})
    monkeypatch.setattr(dependencies, "_project_requirement_map", lambda: {})
    monkeypatch.setattr(dependencies, "_project_venv_active", lambda: True)

    report = dependencies.environment_report()

    assert report["status"] == "extra_packages"
    assert report["immutable_drift"] == []
    assert report["extra_packages"][0]["name"] == "user-plugin-helper"
    assert report["repairable"] is False


def test_layer_inventory_does_not_trust_manifest_without_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_file = tmp_path / "manifest.json"
    layer_root = tmp_path / "layers"
    monkeypatch.setattr(storage, "MANIFEST_FILE", manifest_file)
    monkeypatch.setattr(storage, "LAYER_ROOT", layer_root)
    monkeypatch.setattr(dependencies, "generation_path", storage.generation_path)
    storage.save_manifest(
        {
            **storage.default_manifest(),
            "active_generation": 4,
            "packages": {"missing-helper": {"version": "1.0.0"}},
        }
    )

    assert dependencies.layer_inventory() == {}


def test_launcher_persists_built_generation_before_orm_migration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction = {
        "state": "pending_restart",
        "database_migration_possible": True,
    }
    writes: list[dict[str, Any]] = []
    monkeypatch.setattr(runtime, "pending_transaction", lambda: transaction)
    monkeypatch.setattr(runtime, "_staged_build", lambda _: None)
    monkeypatch.setattr(
        runtime,
        "build_generation",
        lambda _: {
            "generation": 7,
            "digest": "generation-digest",
            "native_extensions": ["demo.pyd"],
        },
    )

    def write_json(_path: Path, value: dict[str, Any]) -> None:
        writes.append(dict(value))

    def run_migration(action: str) -> int:
        assert action == "apply"
        assert transaction["generation"] == 7
        assert transaction["generation_digest"] == "generation-digest"
        return 0

    monkeypatch.setattr(runtime, "write_json", write_json)
    monkeypatch.setattr(runtime, "_run_orm_migration", run_migration)
    monkeypatch.setattr(runtime, "commit_generation", lambda *args, **kwargs: {})

    assert runtime.apply_pending_transaction()
    assert any(write.get("generation") == 7 for write in writes)


def test_non_core_old_upper_bound_creates_compatibility_override() -> None:
    metadata = {"info": {"requires_dist": ["pillow<10"]}}

    overrides, rewritten = dependencies._compatibility_overrides(
        metadata,
        immutable={},
        shared={"pillow": "10.4.0"},
        current={"pillow": "10.4.0"},
    )

    assert overrides == [
        {
            "name": "pillow",
            "declared_requirement": "pillow<10",
            "effective_version": "10.4.0",
            "tier": "shared_compatible",
            "risk": "unverified_runtime_compatibility",
        }
    ]
    assert rewritten == ["pillow==10.4.0"]


def test_core_old_upper_bound_cannot_be_overridden() -> None:
    metadata = {"info": {"requires_dist": ["pydantic<2"]}}

    with pytest.raises(dependencies.DependencyAnalysisError) as exc_info:
        dependencies._compatibility_overrides(
            metadata,
            immutable={"pydantic": "2.13.0"},
            shared={},
            current={"pydantic": "2.13.0"},
        )

    assert exc_info.value.code == "core_dependency_conflict"
    assert exc_info.value.details[0]["tier"] == "immutable_core"


def test_dependency_repair_sync_preserves_extra_packages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun import update_service

    (tmp_path / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    commands: list[list[str]] = []

    class Result:
        returncode = 0

    def run(command: list[str], **kwargs: Any) -> Result:
        commands.append(command)
        return Result()

    monkeypatch.setattr(update_service, "_ROOT", tmp_path)
    monkeypatch.setattr(update_service.shutil, "which", lambda _: "uv")
    monkeypatch.setattr(update_service.subprocess, "run", run)

    update_service._sync_dependencies(preserve_extras=True)

    assert commands == [["uv", "sync", "--locked", "--inexact"]]


@pytest.mark.asyncio
async def test_environment_repair_requires_fingerprint_and_submits_locked_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage import (
        nonebot_store as web_store,
    )
    from zhenxun.nonebot_store import storage as store_storage
    from zhenxun.utils import _restart_utils

    fingerprint = "a" * 64
    captured: list[set[Path]] = []

    async def preflight() -> tuple[bool, str]:
        return True, "ready"

    async def request(_source: str, paths: set[Path]) -> tuple[bool, str]:
        captured.append(paths)
        return True, "accepted"

    monkeypatch.setenv("ZHENXUN_LAUNCHER_PID", "123")
    monkeypatch.setattr(
        web_store,
        "_environment_view",
        lambda: {
            "fingerprint": fingerprint,
            "repairable": True,
            "launcher_managed": True,
        },
    )
    monkeypatch.setattr(web_store, "preflight_environment_repair", preflight)
    monkeypatch.setattr(_restart_utils, "request_dependency_restart", request)
    monkeypatch.setattr(
        web_store,
        "restart_status_data",
        lambda: {
            "boot_id": "boot",
            "access_urls": ["http://localhost:8080"],
            "access_targets": [],
        },
    )
    monkeypatch.setattr(
        store_storage, "DEPENDENCY_SYNC_STATUS_FILE", tmp_path / "sync.json"
    )
    monkeypatch.setattr(
        web_store,
        "save_dependency_sync_status",
        store_storage.save_dependency_sync_status,
    )

    result = await web_store.repair_dependency_environment(
        web_store.EnvironmentRepairPayload(
            expected_fingerprint=fingerprint, confirmed=True
        )
    )

    assert result.data["apply_mode"] == "restart_requested"
    assert captured == [{Path("pyproject.toml"), Path("uv.lock")}]
    assert store_storage.dependency_sync_status()["status"] == "pending"


def test_uninstall_keeps_dependencies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_file = tmp_path / "manifest.json"
    monkeypatch.setattr(storage, "MANIFEST_FILE", manifest_file)
    storage.save_manifest(
        {
            **storage.default_manifest(),
            "packages": {
                "nonebot-plugin-demo": {"version": "1.0.0"},
                "shared-helper": {"version": "2.0.0"},
            },
        }
    )

    plan = dependencies.uninstall_plan(
        {"project_link": "nonebot-plugin-demo", "version": "1.0.0"}
    )

    assert "nonebot-plugin-demo" not in plan["resolved_packages"]
    assert plan["resolved_packages"]["shared-helper"] == "2.0.0"


def test_activate_generation_replaces_old_layer_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layer_root = tmp_path / "layers"
    manifest_file = tmp_path / "manifest.json"
    current = layer_root / "generation-2"
    old = layer_root / "generation-1"
    current.mkdir(parents=True)
    old.mkdir(parents=True)
    monkeypatch.setattr(storage, "LAYER_ROOT", layer_root)
    monkeypatch.setattr(storage, "MANIFEST_FILE", manifest_file)
    monkeypatch.setattr(runtime, "LAYER_ROOT", layer_root)
    monkeypatch.setattr(runtime, "generation_path", storage.generation_path)
    storage.save_manifest({**storage.default_manifest(), "active_generation": 2})
    monkeypatch.setattr(sys, "path", [str(old), "base-path"])

    selected = runtime.activate_current_generation()

    assert selected == current.resolve()
    assert sys.path == [str(current.resolve()), "base-path"]


def test_generation_native_extensions_reads_generation_metadata(tmp_path: Path) -> None:
    storage.write_json(
        tmp_path / ".zhenxun-generation.json",
        {"native_extensions": ["demo/native.pyd", "helper.so"]},
    )

    assert runtime.generation_native_extensions(tmp_path) == {
        "demo/native.pyd",
        "helper.so",
    }


def test_finalize_keeps_only_active_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layer_root = tmp_path / "layers"
    manifest_file = tmp_path / "manifest.json"
    rollback_file = tmp_path / "rollback.json"
    pending_file = tmp_path / "pending.json"
    for generation in (1, 2, 3):
        (layer_root / f"generation-{generation}").mkdir(parents=True)
    monkeypatch.setattr(storage, "LAYER_ROOT", layer_root)
    monkeypatch.setattr(storage, "MANIFEST_FILE", manifest_file)
    monkeypatch.setattr(storage, "ROLLBACK_FILE", rollback_file)
    monkeypatch.setattr(storage, "PENDING_FILE", pending_file)
    monkeypatch.setattr(runtime, "ROLLBACK_FILE", rollback_file)
    monkeypatch.setattr(runtime, "PENDING_FILE", pending_file)
    monkeypatch.setattr(runtime, "prune_generations", storage.prune_generations)
    storage.save_manifest(
        {
            **storage.default_manifest(),
            "active_generation": 3,
            "previous_generation": 2,
            "pending_verification": True,
        }
    )

    runtime.finalize_pending_transaction()

    manifest = storage.load_manifest()
    assert manifest["active_generation"] == 3
    assert manifest["previous_generation"] is None
    assert sorted(path.name for path in layer_root.iterdir()) == ["generation-3"]


def test_finalize_keeps_generation_referenced_by_loaded_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    layer_root = tmp_path / "layers"
    manifest_file = tmp_path / "manifest.json"
    rollback_file = tmp_path / "rollback.json"
    pending_file = tmp_path / "pending.json"
    for generation in (1, 2, 3):
        (layer_root / f"generation-{generation}").mkdir(parents=True)
    monkeypatch.setattr(storage, "LAYER_ROOT", layer_root)
    monkeypatch.setattr(storage, "MANIFEST_FILE", manifest_file)
    monkeypatch.setattr(storage, "ROLLBACK_FILE", rollback_file)
    monkeypatch.setattr(storage, "PENDING_FILE", pending_file)
    monkeypatch.setattr(runtime, "LAYER_ROOT", layer_root)
    monkeypatch.setattr(runtime, "ROLLBACK_FILE", rollback_file)
    monkeypatch.setattr(runtime, "PENDING_FILE", pending_file)
    monkeypatch.setattr(runtime, "prune_generations", storage.prune_generations)
    monkeypatch.setitem(
        sys.modules,
        "acceptance_referenced_package",
        SimpleNamespace(
            __file__=str(layer_root / "generation-1" / "demo" / "__init__.py"),
            __path__=[str(layer_root / "generation-1" / "demo")],
        ),
    )
    storage.save_manifest(
        {
            **storage.default_manifest(),
            "active_generation": 3,
            "previous_generation": 2,
            "pending_verification": True,
        }
    )

    runtime.finalize_pending_transaction()

    assert sorted(path.name for path in layer_root.iterdir()) == [
        "generation-1",
        "generation-3",
    ]
