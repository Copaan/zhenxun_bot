from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import nullcontext, suppress
from datetime import datetime, timezone
import hashlib
from html.parser import HTMLParser
import json
import logging
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import time
from typing import Any, Literal
from uuid import uuid4
import zipfile

import httpx
from packaging.version import InvalidVersion, Version

from zhenxun.configs.repo_config import GITHUB_REPOS, aliyun_mirrored
from zhenxun.services.lifecycle import lifecycle_kernel
from zhenxun.services.network_proxy import ManagedAsyncClient
from zhenxun.services.resource_install import (
    ResourceInstallError,
    ResourceTransaction,
    extract_resources_zip,
    prepare_resource_overlay,
    resources_ready,
    validate_resources,
)
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
_WEBUI_LEGACY_VERSION = Version("0.1.0")
_RESOURCE_LEGACY_VERSION = Version("1.1.1")
_RESOURCE_RETRY_SECONDS = 600
_RESOURCE_STARTUP_TASK: asyncio.Task[None] | None = None
_RESOURCE_ACTIVE_TASK: asyncio.Task[None] | None = None
_WEBUI_RETRY_SECONDS = 600
_WEBUI_DOWNLOAD_SECONDS = 180
_WEBUI_STARTUP_TASK: asyncio.Task[None] | None = None
_WEBUI_REFRESH: Callable[[], Awaitable[Any]] | None = None
_WEBUI_STARTUP_STATUS: dict[str, Any] = {}
webui_publication_lock = asyncio.Lock()
_HTTP_TIMEOUT = httpx.Timeout(15.0, connect=8.0)
_STATUS_LOCK = asyncio.Lock()
_JOB_LOCK = asyncio.Lock()
_STATUS_CACHE: tuple[float, dict[str, Any]] | None = None
_ACTIVE_TASK: asyncio.Task[None] | None = None
_PREPARING_JOB_STATES = {"queued", "preparing", "staged"}
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

_ARCHIVE_FORMAT = "https://github.com/{repo}/archive/refs/{kind}/{ref}.zip"


class UpdateServiceError(RuntimeError):
    pass


class ResourceHotSwapUnavailable(UpdateServiceError):
    pass


def _release_numbers(value: object) -> tuple[int, ...] | None:
    """提取版本号开头的 x.y.z 数字部分

    参数:
        value: 版本号，如 v0.2.4-fix3、v0.2.4-203754e

    返回:
        tuple[int, ...] | None: 数字部分，无法识别时为 None
    """
    match = re.match(r"v?(\d+)\.(\d+)\.(\d+)", str(value or "").strip().lower())
    return tuple(int(part) for part in match.groups()) if match else None


def _blocked_release_reason(
    component: UpdateComponent,
    channel: UpdateChannel,
    value: object,
    current: object,
) -> str | None:
    """判断本体正式版能否更新，只允许更新到数字版本号更高的正式版

    参数:
        component: 更新组件
        channel: 更新通道
        value: 正式版版本号
        current: 当前版本号

    返回:
        str | None: 禁止更新的原因，允许时为 None
    """
    if component != "bot" or channel != "release":
        return None
    latest = _release_numbers(value)
    installed = _release_numbers(current)
    if latest is None or installed is None:
        return "无法比较当前版本与正式版的版本号，已禁止更新到正式版。"
    if latest <= installed:
        return "该正式版不比当前版本新，更新会退回旧代码，已禁止。"
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
        except (OSError, ValueError, TypeError, AttributeError):
            pass
    return "unknown", None


async def _get_json(url: str) -> Any:
    async with ManagedAsyncClient(
        timeout=_HTTP_TIMEOUT, follow_redirects=True
    ) as client:
        response = await client.get(
            url,
            headers={"Accept": "application/vnd.github+json"},
        )
        response.raise_for_status()
        return response.json()


async def _get_text(url: str) -> str:
    async with ManagedAsyncClient(
        timeout=_HTTP_TIMEOUT, follow_redirects=True
    ) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.text


async def _remote_versions(channel: UpdateChannel) -> dict[str, dict[str, Any]]:
    if channel == "release":
        release = await _get_json(
            f"https://api.github.com/repos/{GITHUB_REPOS['bot']}/releases/latest"
        )
        bot_version = str(release.get("tag_name") or release.get("name") or "unknown")
        bot_ref = bot_version
    else:
        text = await _get_text(
            f"https://raw.githubusercontent.com/{GITHUB_REPOS['bot']}/main/__version__"
        )
        bot_version = text.split(":", 1)[-1].strip() or "unknown"
        bot_ref = "main"

    resource_text, webui_commit = await asyncio.gather(
        _get_text(
            f"https://raw.githubusercontent.com/{GITHUB_REPOS['resource']}"
            "/main/__version__"
        ),
        _get_json(f"https://api.github.com/repos/{GITHUB_REPOS['webui']}/commits/dist"),
    )
    resource_version = resource_text.split(":", 1)[-1].strip() or "unknown"
    webui_sha = str(webui_commit.get("sha") or "").strip()
    webui_version = "unknown"
    webui_manifest = False
    webui_api_version: int | None = None
    webui_source_commit: str | None = None
    try:
        manifest = await _get_json(
            f"https://raw.githubusercontent.com/{GITHUB_REPOS['webui']}"
            "/dist/version.json"
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
    block_reason = (
        _blocked_release_reason(component, channel, latest_version, current_version)
        if latest_version != "unknown"
        else None
    )
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
        "comparable": comparable,
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
            return {
                **_STATUS_CACHE[1],
                "pending_job": pending_job(),
                "resource_startup_job": resource_startup_status(),
            }

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
            "resource_startup_job": resource_startup_status(),
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


def job_status(job_id: str) -> dict[str, Any]:
    """读取更新任务，准备阶段的任务已不在本进程运行时标记为中断

    参数:
        job_id: 任务 ID

    返回:
        dict[str, Any]: 任务内容
    """
    job = read_job(job_id)
    if job.get("state") in _PREPARING_JOB_STATES and (
        _ACTIVE_TASK is None or _ACTIVE_TASK.done()
    ):
        job = _update_job(
            job_id,
            state="failed",
            error="update_interrupted",
            progress=100,
            apply_mode="failed",
        )
    if job.get("startup_resource") and job.get("next_retry_at"):
        latest = resource_startup_status()
        if latest and latest["job_id"] != job_id:
            job = {**job, "retry_job_id": latest["job_id"]}
    return job


def _update_job(job_id: str, **changes: Any) -> dict[str, Any]:
    job = read_job(job_id)
    job.update(changes)
    job["updated_at"] = _now_iso()
    _write_json(_job_path(job_id), job)
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
    if component in {"webui", "resource"}:
        branch = "dist" if component == "webui" else "main"
        result = await _get_json(
            f"https://api.github.com/repos/{GITHUB_REPOS[component]}/commits/{branch}"
        )
        ref = str(result.get("sha") or "")
        if not re.fullmatch(r"[0-9a-f]{40}", ref):
            raise UpdateServiceError(f"{component}_ref_invalid")
        return ref
    if component == "resource" or channel == "main":
        return "main"
    release = await _get_json(
        f"https://api.github.com/repos/{GITHUB_REPOS['bot']}/releases/latest"
    )
    ref = str(release.get("tag_name") or "").strip()
    if not ref:
        raise UpdateServiceError("release_not_found")
    current = _read_version_file(_ROOT / "__version__")
    if _blocked_release_reason(component, channel, ref, current):
        raise UpdateServiceError("release_blocked")
    return ref


async def _download_archive(
    component: UpdateComponent, ref: str, destination: Path
) -> str:
    kind = "heads" if ref in {"main", "dist"} else "tags"
    url = _ARCHIVE_FORMAT.format(repo=GITHUB_REPOS[component], kind=kind, ref=ref)
    if component in {"webui", "resource"}:
        url = f"https://github.com/{GITHUB_REPOS[component]}/archive/{ref}.zip"
    digest = hashlib.sha256()
    async with ManagedAsyncClient(timeout=None, follow_redirects=True) as client:
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
        Version(version)
    except (OSError, TypeError, ValueError, AttributeError) as exc:
        raise UpdateServiceError("webui_manifest_invalid") from exc
    if not version or api_version != _WEBUI_API_VERSION:
        raise UpdateServiceError("webui_api_incompatible")

    parser = _WebUIAssetParser()
    try:
        parser.feed((root / "index.html").read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        raise UpdateServiceError("webui_assets_incomplete") from exc
    if not parser.paths or any(
        not (root / path).resolve().is_relative_to(root.resolve())
        or not (root / path).is_file()
        for path in parser.paths
    ):
        raise UpdateServiceError("webui_assets_incomplete")


def webui_resources_compatible(root: Path) -> bool:
    """确认本地页面清单、接口协议及入口资源均可用。"""
    try:
        _validate_webui_staging(root)
    except UpdateServiceError:
        return False
    return True


def webui_update_required(root: Path | None = None) -> bool:
    """判断本地页面是否需要旧版升级或完整性修复。"""
    root = root or _ROOT / "data" / "web_ui" / "public"
    try:
        _validate_webui_staging(root)
        manifest = json.loads((root / "version.json").read_text("utf-8"))
        return Version(str(manifest["version"])) <= _WEBUI_LEGACY_VERSION
    except (UpdateServiceError, InvalidVersion, OSError, ValueError, KeyError):
        return True


def resource_update_required(root: Path | None = None) -> bool:
    """判断资源是否过旧、缺失或不满足本体要求。"""
    root = root or _ROOT / "resources"
    if not resources_ready(root, _ROOT / "resources.spec"):
        return True
    try:
        return (
            Version(_read_version_file(root / "__version__"))
            <= _RESOURCE_LEGACY_VERSION
        )
    except (OSError, ValueError):
        return True


def resource_startup_status() -> dict[str, Any] | None:
    """读取自动资源更新的持久化任务，供已授权管理页面续查。"""
    pointer = _JOBS_ROOT.parent / "resource-startup.json"
    if not pointer.is_file():
        return None
    try:
        value = json.loads(pointer.read_text("utf-8"))
        return read_job(value["job_id"])
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise UpdateServiceError("resource_startup_record_invalid") from exc


async def _recover_resource_components() -> None:
    """资源可用后依次恢复资源检查和被阻塞的渲染组件。"""
    if lifecycle_kernel.shutdown_requested:
        raise asyncio.CancelledError
    await lifecycle_kernel.start_components({"warmup:resources"})
    await lifecycle_kernel.start_components({"warmup:renderer"})
    lifecycle_kernel.component_context("warmup:renderer")


async def _retry_resource_update() -> None:
    """串行提交资源更新，失败等待十分钟，未知写入保留现场。"""
    global _RESOURCE_ACTIVE_TASK
    try:
        previous = resource_startup_status()
    except UpdateServiceError:
        logger.exception("资源自动更新记录无法读取，已暂停自动覆盖")
        return
    if previous and previous["state"] in {"applying", "recovery_required"}:
        _update_job(previous["job_id"], state="recovery_required", next_retry_at=None)
        logger.error("资源更新结果待核验，已暂停自动覆盖：%s", previous["job_id"])
        return
    while not lifecycle_kernel.shutdown_requested:
        job_id = None
        try:
            if not await _webui_disk(resource_update_required):
                current = resource_startup_status()
                if current:
                    job_id = current["job_id"]
                await _recover_resource_components()
                if job_id:
                    _update_job(
                        job_id,
                        runtime_ready=True,
                        next_retry_at=None,
                        runtime_error=None,
                        retry_resolved=current["state"] != "completed",
                    )
                return
            if _ACTIVE_TASK is not None and not _ACTIVE_TASK.done():
                await asyncio.gather(
                    asyncio.shield(_ACTIVE_TASK), return_exceptions=True
                )
                continue
            job = await create_update_job(
                component="resource",
                channel="main",
                method="download",
                source="github",
                force=True,
                startup_resource=True,
            )
            job_id = job["job_id"]
            _RESOURCE_ACTIVE_TASK = _ACTIVE_TASK
            if _RESOURCE_ACTIVE_TASK is not None:
                await asyncio.shield(_RESOURCE_ACTIVE_TASK)
            job = read_job(job_id)
            if job["state"] == "recovery_required":
                return
            if job["state"] != "completed":
                raise UpdateServiceError(job.get("error") or "resource_update_failed")
            await _recover_resource_components()
            _update_job(job_id, runtime_ready=True, next_retry_at=None)
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            code = (
                str(exc) if isinstance(exc, UpdateServiceError) else type(exc).__name__
            )
            logger.warning("资源自动更新未完成：%s，600 秒后重试", code)
            if job_id:
                with suppress(OSError, UpdateServiceError):
                    _update_job(
                        job_id,
                        next_retry_at=datetime.fromtimestamp(
                            time.time() + _RESOURCE_RETRY_SECONDS, timezone.utc
                        ).isoformat(),
                        runtime_error=code
                        if read_job(job_id)["state"] == "completed"
                        else None,
                    )
            await asyncio.sleep(_RESOURCE_RETRY_SECONDS)


def start_resource_update() -> None:
    """启动归属于管理生命周期的单实例资源更新任务。"""
    global _RESOURCE_STARTUP_TASK
    if _RESOURCE_STARTUP_TASK is None or _RESOURCE_STARTUP_TASK.done():
        _RESOURCE_STARTUP_TASK = lifecycle_kernel.spawn_task(
            "management:resource-update",
            _retry_resource_update(),
            name="resource-startup-update",
        )


async def stop_resource_update() -> None:
    """停止自动资源更新，等待当前文件事务完成回滚或提交。"""
    global _RESOURCE_STARTUP_TASK, _RESOURCE_ACTIVE_TASK
    tasks = [
        task
        for task in (_RESOURCE_STARTUP_TASK, _RESOURCE_ACTIVE_TASK)
        if task is not None and not task.done()
    ]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _RESOURCE_STARTUP_TASK = _RESOURCE_ACTIVE_TASK = None


def webui_startup_status() -> dict[str, Any]:
    """返回不含连接信息和内部路径的启动更新状态。"""
    status = dict(_WEBUI_STARTUP_STATUS)
    if job_id := status.get("job_id"):
        try:
            job = read_job(job_id)
            if status.get("state") == "queued":
                status["state"] = job.get("phase") or job["state"]
        except UpdateServiceError:
            pass
    return {key: status.get(key) for key in ("state", "reason", "next_retry_at")}


async def _webui_disk(callback: Callable, *args: Any) -> Any:
    """取消时等待文件操作结束，避免临时目录清理与写入竞争。"""
    task = asyncio.create_task(asyncio.to_thread(callback, *args))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    result = task.result()
    if cancelled:
        raise asyncio.CancelledError
    return result


async def _retry_webui_update() -> None:
    """旧版页面立即更新，失败后每十分钟重试直到发布完成。"""
    while await _webui_disk(webui_update_required):
        job_id = None
        try:
            if _ACTIVE_TASK is not None and not _ACTIVE_TASK.done():
                await asyncio.gather(
                    asyncio.shield(_ACTIVE_TASK), return_exceptions=True
                )
                continue
            job = await create_update_job(
                component="webui",
                channel="main",
                method="download",
                source="github",
                force=False,
                startup_webui=True,
            )
            job_id = job["job_id"]
            _WEBUI_STARTUP_STATUS.update(
                state="queued", job_id=job_id, reason=None, next_retry_at=None
            )
            if _ACTIVE_TASK is not None:
                await asyncio.shield(_ACTIVE_TASK)
            job = read_job(job_id)
            if job["state"] != "completed":
                raise UpdateServiceError(job.get("error") or "webui_update_failed")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            code = (
                str(exc) if isinstance(exc, UpdateServiceError) else type(exc).__name__
            )
            logger.warning("WebUI 自动更新失败：%s，600 秒后重试", code)
            retry_at = datetime.fromtimestamp(
                time.time() + _WEBUI_RETRY_SECONDS, timezone.utc
            ).isoformat()
            _WEBUI_STARTUP_STATUS.update(
                state="waiting_retry",
                next_retry_at=retry_at,
                reason=_webui_failure_reason(code),
            )
            if job_id:
                try:
                    _update_job(job_id, next_retry_at=retry_at)
                except Exception:
                    logger.exception("WebUI 更新重试信息保存失败")
            await asyncio.sleep(_WEBUI_RETRY_SECONDS)
    _WEBUI_STARTUP_STATUS.update(state="completed", reason=None, next_retry_at=None)


def start_webui_update(refresh: Callable[[], Awaitable[Any]]) -> None:
    """注册资源刷新回调并启动单实例旧版更新任务。"""
    global _WEBUI_STARTUP_TASK, _WEBUI_REFRESH
    _WEBUI_REFRESH = refresh
    if _WEBUI_STARTUP_TASK is None or _WEBUI_STARTUP_TASK.done():
        _WEBUI_STARTUP_STATUS.clear()
        _WEBUI_STARTUP_TASK = lifecycle_kernel.spawn_task(
            "management:webui", _retry_webui_update(), name="webui-startup-update"
        )


async def stop_webui_update() -> None:
    """停止启动更新和未完成的页面更新任务，等待文件操作收尾。"""
    global _WEBUI_STARTUP_TASK, _WEBUI_REFRESH
    tasks = [
        task
        for task in (_WEBUI_STARTUP_TASK, _ACTIVE_TASK)
        if task and not task.done() and task is not _RESOURCE_ACTIVE_TASK
    ]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _WEBUI_STARTUP_TASK = None
    _WEBUI_REFRESH = None


def _webui_failure_reason(code: str) -> str:
    """将更新错误转换为公开页面可展示的固定诊断。"""
    reasons = {
        "webui_candidate_not_newer": "发布源尚未提供高于 0.1.0 的新版页面",
        "webui_api_incompatible": "发布页面与当前后端接口不兼容",
        "webui_manifest_invalid": "发布页面的版本清单缺失或无效",
        "webui_assets_incomplete": "下载的页面资源不完整",
        "staged_package_incomplete": "下载的发布包不完整",
        "update_in_progress": "另一项更新正在运行",
        "pending_update_exists": "另一项更新正在等待重启应用",
        "PermissionError": "资源目录被占用或没有写入权限",
        "TimeoutError": "下载超过本轮时间限制",
    }
    if code in reasons:
        return reasons[code] + "，十分钟后自动重试。"
    safe_code = code if re.fullmatch(r"[A-Za-z0-9_]{1,80}", code) else "update_failed"
    return f"更新未完成（{safe_code}），十分钟后自动重试。"


def _clone_ref(
    component: UpdateComponent,
    ref: str,
    destination: Path,
    source: UpdateSource,
) -> Path:
    repo_url = f"https://github.com/{GITHUB_REPOS[component]}.git"
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
    commands = [
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
        ]
    ]
    if component in {"webui", "resource"}:
        commands = [
            ["git", "init", str(destination)],
            ["git", "-C", str(destination), "fetch", "--depth", "1", repo_url, ref],
            ["git", "-C", str(destination), "checkout", "--detach", "FETCH_HEAD"],
        ]
    for command in commands:
        result = subprocess.run(
            command,
            cwd=_ROOT,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
            env={**os.environ, **auth_env} if auth_env else None,
        )
        if result.returncode:
            break
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
        validate_resources(root, _ROOT / "resources.spec")


def _clear_readonly(function: Any, target: str, _error: Any) -> None:
    os.chmod(target, stat.S_IWRITE)
    function(target)


def _remove_entry(path: Path) -> None:
    """删除文件或目录，遇到只读文件时去掉只读属性后重试

    参数:
        path: 要删除的路径
    """
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, onerror=_clear_readonly)
    elif path.exists() or path.is_symlink():
        try:
            path.unlink()
        except PermissionError:
            os.chmod(path, stat.S_IWRITE)
            path.unlink()


def _remove_tree(path: Path) -> None:
    """删除目录，遇到只读文件时去掉只读属性后重试，失败只记录日志

    参数:
        path: 要删除的目录
    """
    if not path.exists():
        return
    try:
        _remove_entry(path)
    except OSError as error:
        logger.warning(
            "WebUIUpdate: 清理更新目录失败 %s（%s）", path, error.__class__.__name__
        )


def _prune_backups(keep_job_id: str) -> None:
    """只保留指定任务的更新备份，删除其余任务的备份

    参数:
        keep_job_id: 要保留备份的任务 ID
    """
    if not _BACKUP_ROOT.is_dir():
        return
    for item in _BACKUP_ROOT.iterdir():
        if (item / "resource-startup").exists():
            continue
        if item.name != keep_job_id:
            _remove_tree(item)


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
    old_root.mkdir(parents=True)
    prepare_resource_overlay(staged, _ROOT / "resources", next_root, fill_only=False)
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

    target = _ROOT / "resources"
    job = read_job(job_id)
    automatic = bool(job.get("startup_resource"))
    force = bool(job.get("force"))
    ready = await asyncio.to_thread(resources_ready, target)
    work = (
        _BACKUP_ROOT / job_id / "resource-startup"
        if automatic
        else _STAGING_ROOT / job_id / "resource-transaction"
    )
    if automatic:
        _update_job(
            job_id,
            backup_path=work.relative_to(_ROOT).as_posix(),
            backup_purpose="恢复本次自动升级覆盖的资源文件",
            phase="preparing_transaction",
        )
    transaction = await _webui_disk(
        lambda: ResourceTransaction(
            staged,
            target,
            work,
            fill_only=not force and not ready,
            spec=_ROOT / "resources.spec",
            protect_modified=not force,
        )
    )
    render_ready = bool(renderer_service._initialized)
    guard = (
        drain_rendering("resource_update", timeout=15)
        if render_ready or not automatic
        else nullcontext()
    )
    try:
        async with guard:
            try:
                _update_job(job_id, phase="applying_resources")
                await _webui_disk(transaction.apply)
                if render_ready or not automatic:
                    renderer_service.clear_runtime_caches()
                    await renderer_service.reload_theme()
                    renderer_service.clear_runtime_caches()
                    await _webui_disk(shutil.rmtree, UI_CACHE_PATH, True)
                    UI_CACHE_PATH.mkdir(parents=True, exist_ok=True)
                await _webui_disk(transaction.commit)
            except BaseException as error:
                if isinstance(error, ResourceInstallError) and str(error).startswith(
                    "resource_rollback_blocked"
                ):
                    raise
                try:
                    await _webui_disk(transaction.rollback)
                except BaseException as rollback_error:
                    with suppress(OSError, UpdateServiceError):
                        _update_job(
                            job_id,
                            first_error=type(error).__name__,
                            rollback_error=type(rollback_error).__name__,
                        )
                    raise ResourceInstallError(
                        "resource_rollback_blocked"
                    ) from rollback_error
                with suppress(OSError, UpdateServiceError):
                    _update_job(
                        job_id,
                        rollback_state="confirmed",
                        first_error=type(error).__name__,
                    )
                if render_ready:
                    try:
                        renderer_service.clear_runtime_caches()
                        await renderer_service.reload_theme()
                    except Exception as reload_error:
                        with suppress(OSError, UpdateServiceError):
                            _update_job(
                                job_id,
                                rollback_runtime_error=type(reload_error).__name__,
                            )
                raise
    except TimeoutError as error:
        raise ResourceHotSwapUnavailable("resource_render_drain_timeout") from error
    except PermissionError as error:
        raise ResourceHotSwapUnavailable("resource_file_locked") from error

    return {
        "apply_mode": "hot_reloaded",
        "resource_generation": _read_version_file(_ROOT / "resources" / "__version__"),
        "fallback_reason": None,
    }


def _apply_webui(job_id: str, staged_root: Path) -> None:
    """切换同盘候选目录，失败时恢复旧目录并保留原仓库元数据。"""
    destination = _ROOT / "data" / "web_ui" / "public"
    backup = destination.parent / f".previous-{job_id}"
    if backup.exists():
        raise UpdateServiceError("webui_backup_exists")
    if (staged_root / ".git").exists():
        _remove_entry(staged_root / ".git")
    destination.parent.mkdir(parents=True, exist_ok=True)
    existed = destination.exists()
    if existed:
        os.replace(destination, backup)
    try:
        os.replace(staged_root, destination)
        if (backup / ".git").exists() and not (destination / ".git").exists():
            os.replace(backup / ".git", destination / ".git")
    except Exception:
        if destination.exists():
            os.replace(destination, staged_root)
        if existed:
            os.replace(backup, destination)
        raise
    _remove_tree(backup)


async def _prepare_job(job_id: str) -> None:
    job = read_job(job_id)
    component = job["component"]
    stage = (
        _ROOT / "data" / "web_ui" / ".staging" / job_id
        if component == "webui"
        else _STAGING_ROOT / job_id
    )
    try:
        _update_job(job_id, state="preparing", progress=10)
        if component == "bot" and _git_dirty() and not job["force"]:
            raise UpdateServiceError("working_tree_dirty")
        ref = (
            await asyncio.wait_for(_latest_ref(component, job["channel"]), 30)
            if component in {"webui", "resource"}
            else await _latest_ref(component, job["channel"])
        )
        await asyncio.to_thread(_remove_tree, stage)
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
            _update_job(job_id, state="preparing", phase="downloading", ref=ref)
            download = _download_archive(component, ref, archive)
            checksum = (
                await asyncio.wait_for(download, _WEBUI_DOWNLOAD_SECONDS)
                if component in {"webui", "resource"}
                else await download
            )
            disk = (
                _webui_disk if component in {"webui", "resource"} else asyncio.to_thread
            )
            extract = (
                extract_resources_zip if component == "resource" else _safe_extract
            )
            root = await disk(extract, archive, stage / "content")
            _update_job(job_id, effective_source="github", attempted_sources=["github"])
        _update_job(job_id, phase="validating")
        _validate_staging(component, root)
        if job.get("startup_webui") and webui_update_required(root):
            raise UpdateServiceError("webui_candidate_not_newer")
        if job.get("startup_resource") and resource_update_required(root):
            raise UpdateServiceError("resource_candidate_not_newer")
        root_relative = str(root.resolve().relative_to(_ROOT.resolve()))
        _update_job(
            job_id,
            state="staged",
            progress=70,
            ref=ref,
            checksum=checksum,
        )
        if component == "webui":
            async with runtime_mutation_coordinator.operation(
                "webui_update",
                operation_id=job_id,
                owner="webui.update",
                phase="hot_apply",
            ):
                async with webui_publication_lock:
                    if not job.get("startup_webui") or webui_update_required():
                        _update_job(job_id, phase="publishing")
                        _apply_webui(job_id, root)
                _update_job(
                    job_id,
                    state="completed",
                    progress=100,
                    completed_at=_now_iso(),
                    restart_required=False,
                    apply_mode="webui_refresh",
                    phase="completed",
                )
                if _WEBUI_REFRESH is not None:
                    try:
                        await _WEBUI_REFRESH()
                    except Exception:
                        logger.exception("WebUI 资源刷新失败，将在页面请求时重试")
            await asyncio.to_thread(_remove_tree, stage)
            return

        fallback_reason: str | None = None
        if component == "resource":
            _update_job(job_id, state="applying", progress=82)
            try:
                async with runtime_mutation_coordinator.operation(
                    "resource_update",
                    operation_id=job_id,
                    owner="webui.update",
                    phase="hot_apply",
                ):
                    result = await _apply_resource_update_hot(job_id, root)
            except ResourceHotSwapUnavailable as error:
                if job.get("startup_resource"):
                    raise
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
                    phase="completed",
                )
                await asyncio.to_thread(_remove_tree, stage)
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
        from zhenxun.utils._restart_utils import mark_restart_pending

        mark_restart_pending(f"webui.update:{job_id}", [f"update:{component}:{job_id}"])
    except asyncio.CancelledError:
        if read_job(job_id).get("state") != "completed":
            _update_job(job_id, state="cancelled", error="update_cancelled")
        await _webui_disk(_remove_tree, stage)
        raise
    except Exception as exc:
        code = (
            str(exc)
            if isinstance(exc, UpdateServiceError | ResourceInstallError)
            else f"http_status_{exc.response.status_code}"
            if isinstance(exc, httpx.HTTPStatusError)
            else exc.__class__.__name__
        )
        logger.error("WebUIUpdate: 更新任务失败 component=%s code=%s", component, code)
        recovery_required = component == "resource" and code.startswith(
            "resource_rollback_blocked"
        )
        if not recovery_required:
            await asyncio.to_thread(_remove_tree, stage)
        _update_job(
            job_id,
            state="recovery_required" if recovery_required else "failed",
            error=code,
            progress=100,
            apply_mode="failed",
            next_retry_at=None,
        )


async def create_update_job(
    *,
    component: UpdateComponent,
    channel: UpdateChannel,
    method: UpdateMethod,
    source: UpdateSource,
    force: bool,
    startup_webui: bool = False,
    startup_resource: bool = False,
) -> dict[str, Any]:
    global _ACTIVE_TASK
    async with _JOB_LOCK:
        if _ACTIVE_TASK is not None and not _ACTIVE_TASK.done():
            raise UpdateServiceError("update_in_progress")
        if _PENDING_FILE.exists():
            raise UpdateServiceError("pending_update_exists")
        if component == "resource":
            previous = resource_startup_status()
            if previous and previous["state"] in {"applying", "recovery_required"}:
                raise UpdateServiceError("resource_recovery_required")
        if source == "aliyun" and not aliyun_mirrored(component):
            source = "github"
        if source == "aliyun" and method == "download":
            method = "git"
        if component == "bot" and channel == "release":
            await _latest_ref(component, channel)
        await asyncio.to_thread(_remove_tree, _STAGING_ROOT)
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
            "startup_webui": startup_webui and component == "webui",
            "startup_resource": startup_resource and component == "resource",
            "state": "queued",
            "progress": 0,
            "error": None,
            "apply_mode": None,
            "fallback_reason": None,
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }
        _write_json(_job_path(job_id), job)
        if job["startup_resource"]:
            _write_json(_JOBS_ROOT.parent / "resource-startup.json", {"job_id": job_id})
        _ACTIVE_TASK = lifecycle_kernel.spawn_task(
            "management:resource-update"
            if job["startup_resource"]
            else "management:webui",
            _prepare_job(job_id),
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


def cancel_pending_update(job_id: str) -> dict[str, Any]:
    """取消等待重启应用的更新，删除暂存文件和待重启标记

    参数:
        job_id: 更新任务 ID

    返回:
        dict[str, Any]: 取消后的任务
    """
    global _STATUS_CACHE
    job = read_job(job_id)
    pending = pending_job()
    if (
        job.get("state") != "pending_restart"
        or pending is None
        or pending.get("job_id") != job_id
    ):
        raise UpdateServiceError("update_not_pending")
    from zhenxun.utils._restart_utils import clear_restart_pending

    _PENDING_FILE.unlink(missing_ok=True)
    clear_restart_pending(f"webui.update:{job_id}")
    _remove_tree(_STAGING_ROOT / job_id)
    _STATUS_CACHE = None
    return _update_job(
        job_id,
        state="cancelled",
        progress=100,
        apply_mode="cancelled",
        restart_required=False,
    )


def _replace_package(source: Path, destination: Path) -> None:
    """用 source 替换本体包内容，已存在的 plugins 目录保持不动

    参数:
        source: 新的 zhenxun 包目录
        destination: 要替换的 zhenxun 包目录
    """
    destination.mkdir(parents=True, exist_ok=True)
    keep_plugins = (destination / "plugins").exists()
    for item in destination.iterdir():
        if item.name != "plugins":
            _remove_entry(item)
    for item in source.iterdir():
        if item.name == "plugins" and keep_plugins:
            continue
        target = destination / item.name
        if item.is_dir():
            shutil.copytree(item, target)
        else:
            shutil.copy2(item, target)


def _apply_bot_update(staged: Path, backup: Path) -> None:
    _prune_backups(backup.parent.name)
    if backup.exists():
        _remove_entry(backup)
    backup.mkdir(parents=True, exist_ok=True)
    current_package = _ROOT / "zhenxun"
    shutil.copytree(
        current_package,
        backup / "zhenxun",
        ignore=lambda directory, _names: (
            ["plugins"] if Path(directory) == current_package else []
        ),
    )
    for name in _BOT_ROOT_FILES:
        source = _ROOT / name
        if source.exists():
            shutil.copy2(source, backup / name)
    try:
        _replace_package(staged / "zhenxun", current_package)
        for name in _BOT_ROOT_FILES:
            source = staged / name
            if source.exists():
                shutil.copy2(source, _ROOT / name)
        _sync_dependencies()
    except Exception:
        _restore_bot_update(backup)
        raise


def _restore_bot_update(backup: Path) -> None:
    _replace_package(backup / "zhenxun", _ROOT / "zhenxun")
    for name in _BOT_ROOT_FILES:
        source = backup / name
        destination = _ROOT / name
        if source.exists():
            shutil.copy2(source, destination)
        else:
            destination.unlink(missing_ok=True)
    _sync_dependencies(raise_on_error=False)


def _sync_dependencies(*, raise_on_error: bool = True) -> None:
    """按 uv.lock 同步核心虚拟环境

    本体为可编辑安装，入口程序被运行中的启动器占用，同步时不重装本体，也不安装
    开发依赖组。本体不再依赖、也没有其他来源需要的包随之移除，锁文件之外的其他
    已安装包保留。

    参数:
        raise_on_error: 同步失败时是否抛出异常
    """
    if not (_ROOT / "uv.lock").is_file():
        return
    from zhenxun.services.installer_network import project_environment, uv_executable
    from zhenxun.services.network_proxy import ProxyPolicyError
    from zhenxun.services.nonebot_store.dependencies import DependencyAnalysisError
    from zhenxun.services.nonebot_store.environment import (
        capture_environment,
        core_packages,
        dropped_core_packages,
    )

    uv = uv_executable()
    if uv is None:
        if raise_on_error:
            raise UpdateServiceError("dependency_tool_unavailable")
        return
    try:
        environment = project_environment()
    except ProxyPolicyError:
        if raise_on_error:
            raise UpdateServiceError("dependency_sync_failed") from None
        return
    try:
        previous = core_packages(capture_environment())
    except DependencyAnalysisError:
        previous = set()
    result = subprocess.run(
        [uv, "sync", "--locked", "--inexact", "--no-install-project", "--no-dev"],
        cwd=_ROOT,
        capture_output=True,
        timeout=900,
        check=False,
        env=environment,
    )
    if result.returncode == 0:
        try:
            dropped = dropped_core_packages(previous)
        except DependencyAnalysisError:
            dropped = []
        if dropped:
            logger.info("移除本体不再依赖的包: %s", ", ".join(dropped))
            result = subprocess.run(
                [uv, "pip", "uninstall", "--python", sys.executable, *dropped],
                cwd=_ROOT,
                capture_output=True,
                timeout=300,
                check=False,
                env=environment,
            )
    if result.returncode and raise_on_error:
        raise UpdateServiceError("dependency_sync_failed")


def _apply_resource_update(staged: Path, backup: Path, *, force: bool = False) -> None:
    target = _ROOT / "resources"
    transaction = ResourceTransaction(
        staged,
        target,
        backup,
        fill_only=not force and not resources_ready(target),
        spec=_ROOT / "resources.spec",
        protect_modified=not force,
    )
    transaction.apply()
    transaction.commit()


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
            _apply_resource_update(
                staged, backup, force=bool(read_job(job_id).get("force"))
            )
        else:
            raise UpdateServiceError("pending_component_invalid")
    except Exception as exc:
        code = (
            str(exc)
            if isinstance(exc, UpdateServiceError | ResourceInstallError)
            else exc.__class__.__name__
        )
        _update_job(job_id, state="failed", error=code, progress=100)
        _PENDING_FILE.unlink(missing_ok=True)
        _remove_tree(_STAGING_ROOT / job_id)
        raise
    _remove_tree(_STAGING_ROOT / job_id)
    if component == "resource":
        _remove_tree(_BACKUP_ROOT / job_id)
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
    "cancel_pending_update",
    "check_updates",
    "create_update_job",
    "finalize_applied_update",
    "pending_job",
    "read_job",
    "request_update_apply",
    "rollback_applied_update",
]
