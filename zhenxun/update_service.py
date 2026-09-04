from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timezone
import hashlib
from html.parser import HTMLParser
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import time
from typing import Any, Literal
from uuid import uuid4
import zipfile

import httpx

from zhenxun.services.lifecycle.operations import operation_registry
from zhenxun.services.runtime_mutation import runtime_mutation_coordinator
from zhenxun.utils.atomic_json import write_json_locked

logger = logging.getLogger(__name__)

UpdateComponent = Literal["bot", "resource", "webui"]
UpdateChannel = Literal["main", "release"]
UpdateMethod = Literal["download", "git"]
UpdateSource = Literal["github", "aliyun"]

_ROOT = Path.cwd()
_UPDATE_ROOT = _ROOT / "data" / "update"
_STAGING_ROOT = _UPDATE_ROOT / "staging"
_BACKUP_ROOT = _UPDATE_ROOT / "backups"
_JOBS_ROOT = _UPDATE_ROOT / "jobs"
_PENDING_FILE = _UPDATE_ROOT / "pending.json"
_APPLIED_FILE = _UPDATE_ROOT / "applied.json"
_STATUS_CACHE_SECONDS = 300
_WEBUI_API_VERSION = 1
_HTTP_TIMEOUT = httpx.Timeout(15.0, connect=8.0)
_STATUS_LOCK = asyncio.Lock()
_JOB_LOCK = asyncio.Lock()
_STATUS_CACHE: tuple[float, dict[str, Any]] | None = None
_ACTIVE_TASK: asyncio.Task[None] | None = None
_BLOCKED_BOT_RELEASES = {"0.2.4-fix"}
_RESOURCE_ENTRIES = (
    "font",
    "image",
    "record",
    "text",
    "themes",
    "__version__",
    "README.md",
)
_RESOURCE_REQUIRED_DIRS = ("font", "image", "record", "text", "themes")

_BOT_ROOT_FILES = (
    "pyproject.toml",
    "uv.lock",
    "requirements.txt",
    "__version__",
    "resources.spec",
)

_REPOSITORIES = {
    "bot": "https://github.com/zhenxun-org/zhenxun_bot.git",
    "resource": "https://github.com/zhenxun-org/zhenxun-bot-resources.git",
    "webui": "https://github.com/HibiKier/zhenxun_bot_webui.git",
}
_ARCHIVES = {
    "bot": "https://github.com/zhenxun-org/zhenxun_bot/archive/refs/{kind}/{ref}.zip",
    "resource": (
        "https://github.com/zhenxun-org/zhenxun-bot-resources/"
        "archive/refs/{kind}/{ref}.zip"
    ),
    "webui": (
        "https://github.com/HibiKier/zhenxun_bot_webui/" "archive/refs/{kind}/{ref}.zip"
    ),
}


class UpdateServiceError(RuntimeError):
    pass


class ResourceHotSwapUnavailable(UpdateServiceError):
    pass


def _normalized_release(value: object) -> str:
    normalized = str(value or "").strip().lower()
    return normalized[1:] if normalized.startswith("v") else normalized


def _blocked_release_reason(
    component: UpdateComponent, channel: UpdateChannel, value: object
) -> str | None:
    if (
        component == "bot"
        and channel == "release"
        and _normalized_release(value) in _BLOCKED_BOT_RELEASES
    ):
        return "该版本存在已知兼容性问题，已禁止更新。"
    return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_version_file(path: Path) -> str:
    if not path.is_file():
        return "unknown"
    value = path.read_text(encoding="utf-8").strip()
    return value.split(":", 1)[-1].strip() or "unknown"


def _read_webui_version() -> tuple[str, str | None]:
    manifest = _ROOT / "data" / "web_ui" / "public" / "version.json"
    if manifest.is_file():
        try:
            data = json.loads(manifest.read_text(encoding="utf-8"))
            version = str(data.get("version") or "unknown")
            commit = str(data.get("commit") or "").strip() or None
            return version, commit
        except (OSError, ValueError, TypeError):
            pass
    return "unknown", None


async def _get_json(url: str) -> Any:
    async with httpx.AsyncClient(
        timeout=_HTTP_TIMEOUT, follow_redirects=True
    ) as client:
        response = await client.get(
            url,
            headers={"Accept": "application/vnd.github+json"},
        )
        response.raise_for_status()
        return response.json()


async def _get_text(url: str) -> str:
    async with httpx.AsyncClient(
        timeout=_HTTP_TIMEOUT, follow_redirects=True
    ) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.text


async def _remote_versions(channel: UpdateChannel) -> dict[str, dict[str, Any]]:
    if channel == "release":
        release = await _get_json(
            "https://api.github.com/repos/zhenxun-org/zhenxun_bot/releases/latest"
        )
        bot_version = str(release.get("tag_name") or release.get("name") or "unknown")
        bot_ref = bot_version
    else:
        text = await _get_text(
            "https://raw.githubusercontent.com/zhenxun-org/zhenxun_bot/main/__version__"
        )
        bot_version = text.split(":", 1)[-1].strip() or "unknown"
        bot_ref = "main"

    resource_text, webui_commit = await asyncio.gather(
        _get_text(
            "https://raw.githubusercontent.com/zhenxun-org/"
            "zhenxun-bot-resources/main/__version__"
        ),
        _get_json(
            "https://api.github.com/repos/HibiKier/" "zhenxun_bot_webui/commits/dist"
        ),
    )
    resource_version = resource_text.split(":", 1)[-1].strip() or "unknown"
    webui_sha = str(webui_commit.get("sha") or "").strip()
    webui_version = "unknown"
    webui_manifest = False
    webui_api_version: int | None = None
    webui_source_commit: str | None = None
    try:
        manifest = await _get_json(
            "https://raw.githubusercontent.com/HibiKier/"
            "zhenxun_bot_webui/dist/version.json"
        )
        webui_version = str(manifest.get("version") or "unknown")
        webui_manifest = True
        webui_api_version = int(manifest.get("api_version"))
        webui_source_commit = str(manifest.get("commit") or "").strip() or None
    except Exception:
        if webui_sha:
            webui_version = f"dist-{webui_sha[:7]}"

    return {
        "bot": {"version": bot_version, "ref": bot_ref, "manifest": True},
        "resource": {
            "version": resource_version,
            "ref": "main",
            "manifest": True,
        },
        "webui": {
            "version": webui_version,
            "ref": "dist",
            "commit": webui_source_commit or webui_sha or None,
            "dist_commit": webui_sha or None,
            "manifest": webui_manifest,
            "api_version": webui_api_version,
        },
    }


def _component_status(
    component: UpdateComponent,
    current_version: str,
    current_commit: str | None,
    remote: dict[str, Any],
    channel: UpdateChannel = "main",
) -> dict[str, Any]:
    latest_version = str(remote.get("version") or "unknown")
    latest_commit = str(remote.get("commit") or "").strip() or None
    comparable_current = (
        current_commit if component == "webui" and current_commit else current_version
    )
    comparable_latest = (
        latest_commit if component == "webui" and latest_commit else latest_version
    )
    comparable = comparable_current != "unknown" and comparable_latest != "unknown"
    block_reason = _blocked_release_reason(component, channel, latest_version)
    return {
        "component": component,
        "current_version": current_version,
        "current_commit": current_commit,
        "latest_version": latest_version,
        "latest_commit": latest_commit,
        "ref": remote.get("ref"),
        "manifest_available": bool(remote.get("manifest")),
        "compatible": (
            component != "webui" or remote.get("api_version") == _WEBUI_API_VERSION
        ),
        "update_available": (
            comparable and comparable_current != comparable_latest and not block_reason
        ),
        "blocked": bool(block_reason),
        "block_reason": block_reason,
        "source": "official",
    }


async def check_updates(
    *, channel: UpdateChannel = "main", refresh: bool = False
) -> dict[str, Any]:
    global _STATUS_CACHE
    async with _STATUS_LOCK:
        now = time.monotonic()
        if (
            not refresh
            and _STATUS_CACHE is not None
            and now - _STATUS_CACHE[0] < _STATUS_CACHE_SECONDS
            and _STATUS_CACHE[1].get("channel") == channel
        ):
            return _STATUS_CACHE[1]

        bot_current = _read_version_file(_ROOT / "__version__")
        resource_current = _read_version_file(_ROOT / "resources" / "__version__")
        webui_current, webui_commit = _read_webui_version()
        errors: list[str] = []
        try:
            remote = await _remote_versions(channel)
        except Exception as exc:
            logger.warning(
                "WebUIUpdate: 检查官方更新失败（%s）", exc.__class__.__name__
            )
            remote = {
                name: {"version": "unknown", "ref": None, "manifest": False}
                for name in ("bot", "resource", "webui")
            }
            errors.append("official_source_unavailable")

        result = {
            "channel": channel,
            "checked_at": _now_iso(),
            "cache_ttl_seconds": _STATUS_CACHE_SECONDS,
            "launcher_managed": bool(os.getenv("ZHENXUN_LAUNCHER_PID")),
            "components": {
                "bot": _component_status(
                    "bot", bot_current, None, remote["bot"], channel
                ),
                "resource": _component_status(
                    "resource", resource_current, None, remote["resource"], channel
                ),
                "webui": _component_status(
                    "webui", webui_current, webui_commit, remote["webui"], channel
                ),
            },
            "errors": errors,
            "pending_job": pending_job(),
        }
        _STATUS_CACHE = (now, result)
        return result


def _job_path(job_id: str) -> Path:
    if not job_id or any(char not in "0123456789abcdef" for char in job_id):
        raise UpdateServiceError("invalid_job_id")
    return _JOBS_ROOT / f"{job_id}.json"


def _write_json(path: Path, value: dict[str, Any]) -> None:
    write_json_locked(path, value)


def read_job(job_id: str) -> dict[str, Any]:
    path = _job_path(job_id)
    if not path.is_file():
        raise UpdateServiceError("job_not_found")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise UpdateServiceError("job_unreadable") from exc
    if not isinstance(data, dict):
        raise UpdateServiceError("job_unreadable")
    return data


def _update_job(job_id: str, **changes: Any) -> dict[str, Any]:
    job = read_job(job_id)
    job.update(changes)
    job["updated_at"] = _now_iso()
    _write_json(_job_path(job_id), job)
    if operation_registry.get(job_id) is not None:
        with contextlib.suppress(Exception):
            operation_registry.update(
                job_id,
                phase=str(job.get("state") or "running"),
                progress=int(job.get("progress") or 0),
            )
    return job


def pending_job() -> dict[str, Any] | None:
    if not _PENDING_FILE.is_file():
        return None
    try:
        value = json.loads(_PENDING_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(value, dict):
        return None
    return {
        "job_id": value.get("job_id"),
        "component": value.get("component"),
        "state": "pending_restart",
    }


def applied_update_pending() -> bool:
    return _APPLIED_FILE.is_file()


def _git_dirty() -> bool:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=_ROOT,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return result.returncode == 0 and bool(result.stdout.strip())


async def _latest_ref(component: UpdateComponent, channel: UpdateChannel) -> str:
    if component == "webui":
        return "dist"
    if component == "resource" or channel == "main":
        return "main"
    release = await _get_json(
        "https://api.github.com/repos/zhenxun-org/zhenxun_bot/releases/latest"
    )
    ref = str(release.get("tag_name") or "").strip()
    if not ref:
        raise UpdateServiceError("release_not_found")
    if _blocked_release_reason(component, channel, ref):
        raise UpdateServiceError("release_blocked")
    return ref


async def _download_archive(
    component: UpdateComponent, ref: str, destination: Path
) -> str:
    kind = "heads" if ref in {"main", "dist"} else "tags"
    url = _ARCHIVES[component].format(kind=kind, ref=ref)
    digest = hashlib.sha256()
    async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
        async with client.stream("GET", url, timeout=_HTTP_TIMEOUT) as response:
            response.raise_for_status()
            with destination.open("wb") as stream:
                async for chunk in response.aiter_bytes(128 * 1024):
                    digest.update(chunk)
                    stream.write(chunk)
    return digest.hexdigest()


def _safe_extract(archive: Path, destination: Path) -> Path:
    destination.mkdir(parents=True, exist_ok=True)
    root = destination.resolve()
    with zipfile.ZipFile(archive) as bundle:
        for item in bundle.infolist():
            target = (destination / item.filename).resolve()
            if target != root and root not in target.parents:
                raise UpdateServiceError("archive_path_escape")
        bundle.extractall(destination)
    roots = [path for path in destination.iterdir() if path.is_dir()]
    if len(roots) != 1:
        raise UpdateServiceError("archive_root_invalid")
    return roots[0]


class _WebUIAssetParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.paths: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        target = "src" if tag == "script" else "href" if tag == "link" else None
        if target is None:
            return
        for key, value in attrs:
            if (
                key == target
                and value
                and not value.startswith(("http://", "https://", "//"))
            ):
                self.paths.add(value.split("?", 1)[0].split("#", 1)[0].lstrip("/"))


def _validate_webui_staging(root: Path) -> None:
    try:
        manifest = json.loads((root / "version.json").read_text(encoding="utf-8"))
        api_version = int(manifest.get("api_version"))
        version = str(manifest.get("version") or "").strip()
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise UpdateServiceError("webui_manifest_invalid") from exc
    if not version or api_version != _WEBUI_API_VERSION:
        raise UpdateServiceError("webui_api_incompatible")

    parser = _WebUIAssetParser()
    parser.feed((root / "index.html").read_text(encoding="utf-8"))
    if not parser.paths or any(not (root / path).is_file() for path in parser.paths):
        raise UpdateServiceError("webui_assets_incomplete")


def _clone_ref(
    component: UpdateComponent,
    ref: str,
    destination: Path,
    source: UpdateSource,
) -> Path:
    repo_url = _REPOSITORIES[component]
    authenticated_url = repo_url
    if source == "aliyun":
        from zhenxun.utils.repo_utils.utils import prepare_aliyun_url

        authenticated_url = prepare_aliyun_url(repo_url)
    from zhenxun.utils.repo_utils.utils import (
        canonicalize_git_url,
        git_auth_environment,
    )

    repo_url = canonicalize_git_url(authenticated_url)
    auth_env = git_auth_environment(authenticated_url, repo_url)
    result = subprocess.run(
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--branch",
            ref,
            "--single-branch",
            repo_url,
            str(destination),
        ],
        cwd=_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
        env={**os.environ, **auth_env} if auth_env else None,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").lower()
        transport_markers = (
            "could not resolve host",
            "failed to connect",
            "unable to access",
            "connection reset",
            "connection timed out",
            "operation timed out",
            "tls",
            "rpc failed",
            "early eof",
            "http 502",
            "http 503",
            "http 504",
        )
        code = (
            "git_transport_failed"
            if any(marker in stderr for marker in transport_markers)
            else "official_git_clone_failed"
        )
        raise UpdateServiceError(code)
    return destination


def _validate_staging(component: UpdateComponent, root: Path) -> None:
    if component == "bot":
        required = (root / "zhenxun", root / "pyproject.toml", root / "__version__")
    elif component == "resource":
        required = (root / "__version__",)
    else:
        required = (root / "index.html", root / "js", root / "css")
    if any(not path.exists() for path in required):
        raise UpdateServiceError("staged_package_incomplete")
    if component == "webui":
        _validate_webui_staging(root)
    elif component == "resource":
        if any(not (root / name).is_dir() for name in _RESOURCE_REQUIRED_DIRS):
            raise UpdateServiceError("resource_directories_incomplete")
        default_theme = root / "themes" / "default"
        if not default_theme.is_dir() or not any(default_theme.iterdir()):
            raise UpdateServiceError("resource_default_theme_invalid")


def _remove_path(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def _prepare_resource_swap(job_id: str, staged: Path) -> tuple[Path, Path]:
    swap_root = _STAGING_ROOT / job_id / "resource-hot-swap"
    if swap_root.exists():
        shutil.rmtree(swap_root)
    next_root = swap_root / "next"
    old_root = swap_root / "old"
    next_root.mkdir(parents=True)
    old_root.mkdir(parents=True)
    for name in _RESOURCE_ENTRIES:
        source = staged / name
        if not source.exists():
            continue
        destination = next_root / name
        if source.is_dir():
            shutil.copytree(source, destination)
        else:
            shutil.copy2(source, destination)
    return next_root, old_root


def _swap_resource_entries(next_root: Path, old_root: Path) -> list[str]:
    destination_root = _ROOT / "resources"
    swapped: list[str] = []
    try:
        for name in _RESOURCE_ENTRIES:
            replacement = next_root / name
            if not replacement.exists():
                continue
            destination = destination_root / name
            displaced = old_root / name
            if destination.exists():
                os.replace(destination, displaced)
            try:
                os.replace(replacement, destination)
            except Exception:
                if displaced.exists():
                    os.replace(displaced, destination)
                raise
            swapped.append(name)
    except OSError as error:
        _rollback_resource_entries(swapped, old_root)
        raise ResourceHotSwapUnavailable("resource_file_locked") from error
    return swapped


def _rollback_resource_entries(swapped: list[str], old_root: Path) -> None:
    destination_root = _ROOT / "resources"
    for name in reversed(swapped):
        destination = destination_root / name
        displaced = old_root / name
        if destination.exists():
            _remove_path(destination)
        if displaced.exists():
            os.replace(displaced, destination)


async def _apply_resource_update_hot(job_id: str, staged: Path) -> dict[str, Any]:
    from zhenxun.configs.path_config import UI_CACHE_PATH
    from zhenxun.services.renderer import renderer_service
    from zhenxun.services.renderer.engine import drain_rendering

    next_root, old_root = await asyncio.to_thread(
        _prepare_resource_swap, job_id, staged
    )
    swapped: list[str] = []
    try:
        async with drain_rendering("resource_update", timeout=15):
            swapped = await asyncio.to_thread(
                _swap_resource_entries, next_root, old_root
            )
            try:
                renderer_service.clear_runtime_caches()
                await renderer_service.reload_theme()
                renderer_service.clear_runtime_caches()
                await asyncio.to_thread(shutil.rmtree, UI_CACHE_PATH, True)
                UI_CACHE_PATH.mkdir(parents=True, exist_ok=True)
            except Exception:
                await asyncio.to_thread(_rollback_resource_entries, swapped, old_root)
                renderer_service.clear_runtime_caches()
                await renderer_service.reload_theme()
                raise
    except TimeoutError as error:
        raise ResourceHotSwapUnavailable("resource_render_drain_timeout") from error

    return {
        "apply_mode": "hot_reloaded",
        "resource_generation": _read_version_file(_ROOT / "resources" / "__version__"),
        "fallback_reason": None,
    }


def _copy_visible(source: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for item in destination.iterdir():
        if item.name == ".git":
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
    for item in source.iterdir():
        if item.name == ".git":
            continue
        target = destination / item.name
        if item.is_dir():
            shutil.copytree(item, target)
        else:
            shutil.copy2(item, target)


def _apply_webui(job_id: str, staged_root: Path) -> None:
    destination = _ROOT / "data" / "web_ui" / "public"
    backup = _BACKUP_ROOT / job_id / "webui"
    if backup.exists():
        shutil.rmtree(backup)
    backup.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(destination, backup, ignore=shutil.ignore_patterns(".git"))
    try:
        _copy_visible(staged_root, destination)
    except Exception:
        _copy_visible(backup, destination)
        raise


async def _prepare_job(job_id: str) -> None:
    job = read_job(job_id)
    component = job["component"]
    try:
        _update_job(job_id, state="preparing", progress=10)
        if component == "bot" and _git_dirty() and not job["force"]:
            raise UpdateServiceError("working_tree_dirty")
        ref = await _latest_ref(component, job["channel"])
        if _blocked_release_reason(component, job["channel"], ref):
            raise UpdateServiceError("release_blocked")
        stage = _STAGING_ROOT / job_id
        if stage.exists():
            shutil.rmtree(stage)
        stage.mkdir(parents=True, exist_ok=True)
        checksum: str | None = None
        if job["method"] == "git":
            attempted_sources = [job["source"]]
            effective_source = job["source"]
            try:
                root = await asyncio.to_thread(
                    _clone_ref, component, ref, stage / "source", job["source"]
                )
            except UpdateServiceError as exc:
                if str(exc) != "git_transport_failed" or job["source"] != "aliyun":
                    raise
                attempted_sources.append("github")
                effective_source = "github"
                source_root = stage / "source"
                if source_root.exists():
                    shutil.rmtree(source_root)
                root = await asyncio.to_thread(
                    _clone_ref, component, ref, source_root, "github"
                )
            _update_job(
                job_id,
                attempted_sources=attempted_sources,
                effective_source=effective_source,
            )
        else:
            archive = stage / "package.zip"
            checksum = await _download_archive(component, ref, archive)
            root = await asyncio.to_thread(_safe_extract, archive, stage / "content")
        _validate_staging(component, root)
        root_relative = str(root.resolve().relative_to(_ROOT.resolve()))
        _update_job(
            job_id,
            state="staged",
            progress=70,
            ref=ref,
            checksum=checksum,
        )
        if component == "webui":
            operation_registry.mark_commit_critical(job_id, "webui_hot_apply")
            async with runtime_mutation_coordinator.operation(
                "webui_update",
                operation_id=job_id,
                owner="webui.update",
                phase="hot_apply",
            ):
                await asyncio.to_thread(_apply_webui, job_id, root)
            _update_job(
                job_id,
                state="completed",
                progress=100,
                completed_at=_now_iso(),
                restart_required=False,
                apply_mode="webui_refresh",
            )
            return

        fallback_reason: str | None = None
        if component == "resource":
            _update_job(job_id, state="applying", progress=82)
            try:
                operation_registry.mark_commit_critical(job_id, "resource_hot_apply")
                async with runtime_mutation_coordinator.operation(
                    "resource_update",
                    operation_id=job_id,
                    owner="webui.update",
                    phase="hot_apply",
                ):
                    result = await _apply_resource_update_hot(job_id, root)
            except ResourceHotSwapUnavailable as error:
                fallback_reason = str(error)
                logger.info(
                    "WebUIUpdate: 资源热更新不可用，已降级为待重启: %s",
                    fallback_reason,
                )
            else:
                _update_job(
                    job_id,
                    state="completed",
                    progress=100,
                    completed_at=_now_iso(),
                    restart_required=False,
                    **result,
                )
                return

        pending = {
            "job_id": job_id,
            "component": component,
            "staged_root": root_relative,
            "created_at": _now_iso(),
        }
        _write_json(_PENDING_FILE, pending)
        launcher_managed = bool(os.getenv("ZHENXUN_LAUNCHER_PID"))
        _update_job(
            job_id,
            state="pending_restart",
            progress=80,
            restart_required=True,
            restart_available=launcher_managed,
            apply_mode="restart_pending",
            fallback_reason=fallback_reason,
        )
        from zhenxun.utils._restart_utils import (
            issue_restart_ticket,
            mark_restart_pending,
        )

        mark_restart_pending(f"webui.update:{job_id}", [f"update:{component}:{job_id}"])
        if launcher_managed:
            issue_restart_ticket("webui.update", ttl_seconds=10 * 60)
    except Exception as exc:
        code = (
            str(exc) if isinstance(exc, UpdateServiceError) else exc.__class__.__name__
        )
        logger.error("WebUIUpdate: 更新任务失败 component=%s code=%s", component, code)
        _update_job(
            job_id, state="failed", error=code, progress=100, apply_mode="failed"
        )
        raise


def _update_checkpoint(job_id: str) -> dict[str, Any]:
    try:
        job = read_job(job_id)
    except UpdateServiceError:
        return {"job_state": "missing"}
    return {
        "job_state": job.get("state"),
        "progress": job.get("progress"),
        "ref": job.get("ref"),
        "checksum": job.get("checksum"),
    }


def _recover_update(record: dict[str, Any]):
    job_id = str(record.get("operation_id") or "")
    if not job_id:
        return None
    try:
        job = read_job(job_id)
    except UpdateServiceError:
        return None
    if job.get("state") in {"completed", "failed", "pending_restart"}:
        return None
    return _prepare_job(job_id)


operation_registry.register_recovery_handler("update_prepare", _recover_update)


async def create_update_job(
    *,
    component: UpdateComponent,
    channel: UpdateChannel,
    method: UpdateMethod,
    source: UpdateSource,
    force: bool,
) -> dict[str, Any]:
    global _ACTIVE_TASK
    async with _JOB_LOCK:
        if _ACTIVE_TASK is not None and not _ACTIVE_TASK.done():
            raise UpdateServiceError("update_in_progress")
        if _PENDING_FILE.exists():
            raise UpdateServiceError("pending_update_exists")
        if source == "aliyun" and method == "download":
            method = "git"
        if component == "bot" and channel == "release":
            ref = await _latest_ref(component, channel)
            if _blocked_release_reason(component, channel, ref):
                raise UpdateServiceError("release_blocked")
        job_id = uuid4().hex
        job = {
            "job_id": job_id,
            "component": component,
            "channel": channel,
            "method": method,
            "source": source,
            "requested_method": method,
            "requested_source": source,
            "effective_source": None,
            "attempted_sources": [],
            "force": force,
            "state": "queued",
            "progress": 0,
            "error": None,
            "apply_mode": None,
            "fallback_reason": None,
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }
        _write_json(_job_path(job_id), job)
        _, _ACTIVE_TASK = operation_registry.start(
            "update_prepare",
            _prepare_job(job_id),
            operation_id=job_id,
            public_input={
                "component": component,
                "channel": channel,
                "method": method,
                "source": source,
            },
            recovery_policy="restart",
            checkpoint=lambda: _update_checkpoint(job_id),
            name=f"update-{component}-{job_id[:8]}",
        )
    return job


async def request_update_apply(job_id: str) -> tuple[bool, str, dict[str, Any]]:
    """Request the launcher to apply one validated staged update."""
    job = read_job(job_id)
    pending = pending_job()
    if (
        job.get("state") != "pending_restart"
        or pending is None
        or pending.get("job_id") != job_id
    ):
        raise UpdateServiceError("update_not_pending")
    if not os.getenv("ZHENXUN_LAUNCHER_PID"):
        return False, "当前不是 launcher 托管模式，请手动重启真寻。", job
    from zhenxun.utils._restart_utils import issue_restart_ticket, request_restart

    issue_restart_ticket("webui.update", ttl_seconds=10 * 60)
    ok, message = await request_restart("webui.update", require_ticket="webui.update")
    if ok:
        job = _update_job(
            job_id,
            state="restart_requested",
            progress=85,
            apply_mode="restart_requested",
        )
    return ok, message, job


def _restore_directory(backup: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(backup, destination)


def _apply_bot_update(staged: Path, backup: Path) -> None:
    if backup.exists():
        shutil.rmtree(backup)
    backup.mkdir(parents=True, exist_ok=True)
    current_package = _ROOT / "zhenxun"
    shutil.copytree(current_package, backup / "zhenxun")
    for name in _BOT_ROOT_FILES:
        source = _ROOT / name
        if source.exists():
            shutil.copy2(source, backup / name)

    preserved_plugins = backup / "user_plugins"
    plugins = current_package / "plugins"
    if plugins.exists():
        shutil.copytree(plugins, preserved_plugins)
    try:
        shutil.rmtree(current_package)
        shutil.copytree(staged / "zhenxun", current_package)
        if preserved_plugins.exists():
            target_plugins = current_package / "plugins"
            if target_plugins.exists():
                shutil.rmtree(target_plugins)
            shutil.copytree(preserved_plugins, target_plugins)
        for name in _BOT_ROOT_FILES:
            source = staged / name
            if source.exists():
                shutil.copy2(source, _ROOT / name)
        _sync_dependencies()
    except Exception:
        _restore_bot_update(backup)
        raise


def _restore_bot_update(backup: Path) -> None:
    _restore_directory(backup / "zhenxun", _ROOT / "zhenxun")
    for name in _BOT_ROOT_FILES:
        source = backup / name
        destination = _ROOT / name
        if source.exists():
            shutil.copy2(source, destination)
        else:
            destination.unlink(missing_ok=True)
    _sync_dependencies(raise_on_error=False)


def _sync_dependencies(
    *, raise_on_error: bool = True, preserve_extras: bool = False
) -> None:
    if not (_ROOT / "uv.lock").is_file():
        return
    uv_executable = shutil.which("uv")
    if uv_executable is None:
        if raise_on_error:
            raise UpdateServiceError("dependency_tool_unavailable")
        return
    command = [uv_executable, "sync", "--locked"]
    if preserve_extras:
        command.append("--inexact")
    result = subprocess.run(
        command,
        cwd=_ROOT,
        capture_output=True,
        timeout=900,
        check=False,
    )
    if result.returncode and raise_on_error:
        raise UpdateServiceError("dependency_sync_failed")


def _apply_resource_update(staged: Path, backup: Path) -> None:
    destination = _ROOT / "resources"
    if backup.exists():
        shutil.rmtree(backup)
    backup.parent.mkdir(parents=True, exist_ok=True)
    backup.mkdir(parents=True)
    destination.mkdir(parents=True, exist_ok=True)
    replaced: list[str] = []
    try:
        for name in _RESOURCE_ENTRIES:
            source = staged / name
            if not source.exists():
                continue
            target = destination / name
            previous = backup / name
            if target.exists():
                if target.is_dir():
                    shutil.copytree(target, previous)
                else:
                    shutil.copy2(target, previous)
                _remove_path(target)
            if source.is_dir():
                shutil.copytree(source, target)
            else:
                shutil.copy2(source, target)
            replaced.append(name)
    except Exception:
        for name in reversed(replaced):
            target = destination / name
            previous = backup / name
            if target.exists():
                _remove_path(target)
            if previous.exists():
                if previous.is_dir():
                    shutil.copytree(previous, target)
                else:
                    shutil.copy2(previous, target)
        raise


def apply_pending_update(project_root: Path | None = None) -> bool:
    """Apply a staged bot/resource update before the next worker starts."""
    if project_root is not None and project_root.resolve() != _ROOT.resolve():
        raise UpdateServiceError("project_root_mismatch")
    if not _PENDING_FILE.is_file():
        return False
    pending = json.loads(_PENDING_FILE.read_text(encoding="utf-8"))
    job_id = str(pending.get("job_id") or "")
    component = str(pending.get("component") or "")
    staged = (_ROOT / str(pending.get("staged_root") or "")).resolve()
    allowed_root = _STAGING_ROOT.resolve()
    if allowed_root not in staged.parents:
        raise UpdateServiceError("staged_path_invalid")
    _update_job(job_id, state="applying", progress=90)
    backup = _BACKUP_ROOT / job_id / component
    try:
        if component == "bot":
            _apply_bot_update(staged, backup)
            from zhenxun.utils.bytecode import precompile_path

            precompile_path(_ROOT / "zhenxun")
        elif component == "resource":
            _apply_resource_update(staged, backup)
        else:
            raise UpdateServiceError("pending_component_invalid")
    except Exception as exc:
        code = (
            str(exc) if isinstance(exc, UpdateServiceError) else exc.__class__.__name__
        )
        _update_job(job_id, state="failed", error=code, progress=100)
        _PENDING_FILE.unlink(missing_ok=True)
        raise
    if component == "bot":
        _write_json(
            _APPLIED_FILE,
            {
                "job_id": job_id,
                "component": component,
                "backup": str(backup.resolve().relative_to(_ROOT.resolve())),
                "applied_at": _now_iso(),
            },
        )
        _update_job(
            job_id,
            state="verifying",
            progress=95,
            restart_required=True,
        )
        _PENDING_FILE.unlink(missing_ok=True)
        return True
    _update_job(
        job_id,
        state="completed",
        progress=100,
        completed_at=_now_iso(),
        restart_required=False,
        apply_mode="restart_requested",
        resource_generation=_read_version_file(_ROOT / "resources" / "__version__"),
    )
    _PENDING_FILE.unlink(missing_ok=True)
    return False


def finalize_applied_update() -> bool:
    """Mark a Bot update complete after the replacement worker becomes healthy."""
    if not _APPLIED_FILE.is_file():
        return False
    applied = json.loads(_APPLIED_FILE.read_text(encoding="utf-8"))
    job_id = str(applied.get("job_id") or "")
    _update_job(
        job_id,
        state="completed",
        progress=100,
        completed_at=_now_iso(),
        restart_required=False,
    )
    _APPLIED_FILE.unlink(missing_ok=True)
    return True


def rollback_applied_update() -> bool:
    """Restore the previous Bot source after replacement-worker health failure."""
    if not _APPLIED_FILE.is_file():
        return False
    applied = json.loads(_APPLIED_FILE.read_text(encoding="utf-8"))
    job_id = str(applied.get("job_id") or "")
    backup = (_ROOT / str(applied.get("backup") or "")).resolve()
    allowed_root = _BACKUP_ROOT.resolve()
    if allowed_root not in backup.parents:
        raise UpdateServiceError("backup_path_invalid")
    try:
        _restore_bot_update(backup)
    except Exception as exc:
        code = exc.__class__.__name__
        _update_job(
            job_id,
            state="failed",
            error=f"health_check_failed_rollback_failed:{code}",
            progress=100,
        )
        raise
    _update_job(
        job_id,
        state="failed",
        error="health_check_failed_rolled_back",
        progress=100,
    )
    _APPLIED_FILE.unlink(missing_ok=True)
    return True


__all__ = [
    "UpdateServiceError",
    "applied_update_pending",
    "apply_pending_update",
    "check_updates",
    "create_update_job",
    "finalize_applied_update",
    "pending_job",
    "read_job",
    "request_update_apply",
    "rollback_applied_update",
]
