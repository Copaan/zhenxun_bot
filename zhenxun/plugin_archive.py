"""Bounded, static archive inspection and restart-only source installation."""

from __future__ import annotations

import ast
import asyncio
from collections.abc import AsyncIterator
from contextvars import Context, ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
import gzip
from hashlib import sha256
import json
import keyword
import os
from pathlib import Path
import platform
import re
import shutil
import stat
import struct
import sys
import tarfile
import tempfile
import time
from typing import Any
import uuid
import zipfile

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import InvalidSpecifier, SpecifierSet

from zhenxun import plugin_store_transaction as transaction
from zhenxun.plugin_archive_metadata import (
    MetadataError,
    declared_package_paths,
    poetry_dependencies,
)
from zhenxun.plugin_store_receipts import StoreReceiptStore, source_digest
from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

MAX_COMPRESSED = 100 * 1024 * 1024
MAX_EXPANDED = 500 * 1024 * 1024
MAX_ENTRIES = 10000
TTL = 30 * 60
MAX_SESSIONS = 8
ROOT = Path("data/runtime/plugin-archives")
PLUGIN_ROOT = Path("zhenxun/plugins")
BUILTIN_ROOT = Path("zhenxun/builtin_plugins")
_RESERVED = re.compile(
    r"^(con|prn|aux|nul|conin\$|conout\$|com[1-9\u00b9\u00b2\u00b3]|lpt[1-9\u00b9\u00b2\u00b3])(?:\.|$)",
    re.I,
)


class ArchiveError(ValueError):
    def __init__(self, code: str, status: int = 400, *, candidates=None):
        super().__init__(code)
        self.code = code
        self.status = status
        self.candidates = candidates or []


def _name(raw: str) -> tuple[str, ...]:
    name = raw[:-1] if raw.endswith("/") else raw
    parts = name.split("/")
    if (
        not name
        or len(name) > 1024
        or len(parts) > 32
        or any(c in name for c in '\\:< >"|?*'.replace(" ", ""))
        or any(ord(c) < 32 or ord(c) == 127 for c in name)
        or any(
            p in {"", ".", ".."}
            or p.endswith((".", " "))
            or len(p) > 255
            or _RESERVED.match(p)
            for p in parts
        )
    ):
        raise ArchiveError("archive_path_unsafe")
    return tuple(parts)


class _Entries:
    def __init__(self) -> None:
        self.count = 0
        self.size = 0
        self.paths: dict[str, tuple[str, bool]] = {}
        self.explicit: set[str] = set()

    def add(self, raw: str, directory: bool, size: int) -> tuple[str, ...]:
        self.count += 1
        self.size += size
        if self.count > MAX_ENTRIES or size < 0 or self.size > MAX_EXPANDED:
            raise ArchiveError("archive_expansion_limit", 413)
        parts = _name(raw)
        for index in range(1, len(parts) + 1):
            path = "/".join(parts[:index])
            key = path.casefold()
            is_dir = index < len(parts) or directory
            previous = self.paths.get(key)
            if previous and previous != (path, is_dir):
                raise ArchiveError("archive_path_collision")
            self.paths[key] = (path, is_dir)
        key = "/".join(parts).casefold()
        if key in self.explicit:
            raise ArchiveError("archive_duplicate_entry")
        self.explicit.add(key)
        return parts


def _copy_member(source, target: Path, expected: int) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    remaining = expected
    with target.open("xb") as output:
        while remaining:
            chunk = source.read(min(1024 * 1024, remaining))
            if not chunk:
                raise ArchiveError("archive_truncated")
            output.write(chunk)
            remaining -= len(chunk)
        if source.read(1):
            raise ArchiveError("archive_size_mismatch")


def _check_zip_directory(path: Path) -> None:
    # Inspect fixed-size central records before ZipFile allocates ZipInfo objects.
    header = struct.Struct("<4s6H3I5H2I")
    with path.open("rb") as stream:
        try:
            end = zipfile._EndRecData(stream)
        except (AttributeError, IndexError, struct.error, OSError) as error:
            raise ArchiveError("archive_zip_directory_unsupported") from error
        if not end or end[zipfile._ECD_DISK_NUMBER] or end[zipfile._ECD_DISK_START]:
            raise ArchiveError("archive_invalid")
        offset, size = end[zipfile._ECD_OFFSET], end[zipfile._ECD_SIZE]
        if size > MAX_COMPRESSED or offset + size > path.stat().st_size:
            raise ArchiveError("archive_invalid")
        stream.seek(offset)
        consumed = count = 0
        while consumed < size:
            raw = stream.read(header.size)
            if len(raw) != header.size:
                raise ArchiveError("archive_invalid")
            fields = header.unpack(raw)
            if fields[0] != b"PK\x01\x02":
                raise ArchiveError("archive_invalid")
            count += 1
            if count > MAX_ENTRIES:
                raise ArchiveError("archive_entry_limit", 413)
            skip = sum(fields[10:13])
            consumed += header.size + skip
            stream.seek(skip, 1)
        if consumed != size or count != end[zipfile._ECD_ENTRIES_TOTAL]:
            raise ArchiveError("archive_invalid")


def _check_tar_headers(path: Path) -> None:
    count = 0
    size = path.stat().st_size
    with path.open("rb") as stream:
        while raw := stream.read(tarfile.BLOCKSIZE):
            if raw == b"\0" * tarfile.BLOCKSIZE:
                break
            member = tarfile.TarInfo.frombuf(raw, "utf-8", "strict")
            count += 1
            if count > MAX_ENTRIES:
                raise ArchiveError("archive_entry_limit", 413)
            if member.size < 0 or member.size > MAX_EXPANDED:
                raise ArchiveError("archive_expansion_limit", 413)
            if member.type in {
                tarfile.XHDTYPE,
                tarfile.XGLTYPE,
                tarfile.GNUTYPE_LONGNAME,
            }:
                if member.size > 1024 * 1024:
                    raise ArchiveError("archive_metadata_limit", 413)
            elif not (member.isfile() or member.isdir()):
                raise ArchiveError("archive_special_entry")
            offset = ((member.size + 511) // 512) * 512
            if stream.tell() + offset > size:
                raise ArchiveError("archive_truncated")
            stream.seek(offset, 1)


def extract_archive(archive: Path, destination: Path, kind: str) -> dict[str, int]:
    destination.mkdir()
    entries = _Entries()
    try:
        if kind == "zip":
            _check_zip_directory(archive)
            with zipfile.ZipFile(archive) as source:
                if len(source.infolist()) > MAX_ENTRIES:
                    raise ArchiveError("archive_entry_limit", 413)
                for member in source.infolist():
                    mode = member.external_attr >> 16
                    directory = member.is_dir()
                    if member.flag_bits & (1 | 64 | 8192):
                        raise ArchiveError("archive_encrypted")
                    if stat.S_IFMT(mode) not in {0, stat.S_IFREG, stat.S_IFDIR}:
                        raise ArchiveError("archive_special_entry")
                    if stat.S_IFMT(mode) == stat.S_IFDIR and not directory:
                        raise ArchiveError("archive_special_entry")
                    parts = entries.add(
                        member.orig_filename, directory, member.file_size
                    )
                    target = destination.joinpath(*parts)
                    if directory:
                        if member.file_size:
                            raise ArchiveError("archive_directory_payload")
                        target.mkdir(parents=True, exist_ok=True)
                    else:
                        with source.open(member) as stream:
                            _copy_member(stream, target, member.file_size)
        else:
            # Bound the entire decompressed tar, including headers/PAX metadata.
            raw_tar = archive.with_suffix(".tar")
            try:
                total = 0
                with gzip.open(archive, "rb") as source, raw_tar.open("xb") as output:
                    while chunk := source.read(1024 * 1024):
                        total += len(chunk)
                        if total > MAX_EXPANDED:
                            raise ArchiveError("archive_expansion_limit", 413)
                        output.write(chunk)
                _check_tar_headers(raw_tar)
                with tarfile.open(raw_tar, "r:") as source:
                    for member in source:
                        if (
                            not (member.isfile() or member.isdir())
                            or member.sparse is not None
                        ):
                            raise ArchiveError("archive_special_entry")
                        parts = entries.add(member.name, member.isdir(), member.size)
                        target = destination.joinpath(*parts)
                        if member.isdir():
                            if member.size:
                                raise ArchiveError("archive_directory_payload")
                            target.mkdir(parents=True, exist_ok=True)
                        else:
                            stream = source.extractfile(member)
                            if stream is None:
                                raise ArchiveError("archive_truncated")
                            with stream:
                                _copy_member(stream, target, member.size)
            finally:
                raw_tar.unlink(missing_ok=True)
    except (
        zipfile.BadZipFile,
        tarfile.TarError,
        EOFError,
        OSError,
        RuntimeError,
        UnicodeError,
    ) as error:
        raise ArchiveError("archive_invalid") from error
    return {"entries": entries.count, "expanded_bytes": entries.size}


def _module(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) or keyword.iskeyword(name):
        raise ArchiveError("archive_module_invalid")
    if name in {"__init__", "setup", "conftest", "src"}:
        raise ArchiveError("archive_module_invalid")
    return name


def identify_plugin(root: Path) -> tuple[Path, list[Path], dict[str, Any]]:
    def multiple(paths):
        return ArchiveError(
            "archive_multiple_plugins",
            candidates=sorted({p.relative_to(root).as_posix() for p in paths})[:100],
        )

    current = root
    scopes = [root]
    wrapper = False
    src = False
    ignored = {".github", "docs", "tests", "__MACOSX", "LICENSES", "licenses"}
    while True:
        children = list(current.iterdir())
        packages = [
            p
            for p in children
            if p.name not in ignored and p.is_dir() and (p / "__init__.py").is_file()
        ]
        modules = [
            p
            for p in children
            if p.suffix == ".py" and p.name not in {"setup.py", "__init__.py"}
        ]
        candidates = packages + modules
        metadata_path = current / "pyproject.toml"
        if metadata_path.is_file():
            if metadata_path.stat().st_size > 1024 * 1024:
                raise ArchiveError("archive_dependency_metadata_limit")
            try:
                declared = declared_package_paths(
                    tomllib.loads(metadata_path.read_text(encoding="utf-8-sig"))
                )
            except (ValueError, TypeError, AttributeError) as error:
                raise ArchiveError(
                    str(error)
                    if isinstance(error, MetadataError)
                    else "archive_dependency_metadata_invalid"
                ) from error
            if declared is not None:
                declared_candidates = [current / path for path in declared]
                if len(declared_candidates) != 1 or any(
                    p not in declared_candidates for p in candidates
                ):
                    raise multiple(declared_candidates + candidates)
                candidate = declared_candidates[0]
                if not (
                    (candidate.is_dir() and (candidate / "__init__.py").is_file())
                    or (candidate.is_file() and candidate.suffix == ".py")
                ):
                    raise ArchiveError("archive_package_declaration_invalid")
                if candidate.parent != current:
                    scopes.append(candidate.parent)
                break
        if (current / "__init__.py").is_file():
            if current == root or current.name == "src":
                raise ArchiveError("archive_package_name_missing")
            candidate = current
            break
        if len(candidates) == 1:
            if any(
                p.is_dir() and p not in candidates and p.name not in ignored
                for p in children
            ):
                raise multiple(
                    candidates
                    + [p for p in children if p.is_dir() and p.name not in ignored]
                )
            candidate = candidates[0]
            break
        if candidates:
            raise multiple(candidates)
        directories = [p for p in children if p.is_dir() and p.name not in ignored]
        if len(directories) != 1:
            raise ArchiveError("archive_plugin_not_identified")
        current = directories[0]
        if current.name == "src" and not src:
            src = True
        elif not wrapper and not src:
            wrapper = True
        else:
            raise ArchiveError("archive_wrapper_limit")
        scopes.append(current)
    _module(candidate.stem if candidate.is_file() else candidate.name)
    if candidate.is_dir() and candidate not in scopes:
        scopes.append(candidate)
    # Parse Python, never import it. Bound both individual ASTs and total work.
    metadata: dict[str, Any] = {}
    entrypoint = candidate if candidate.is_file() else candidate / "__init__.py"
    sources = [candidate] if candidate.is_file() else list(candidate.rglob("*.py"))
    if sum(p.stat().st_size for p in sources) > 32 * 1024 * 1024:
        raise ArchiveError("archive_python_source_limit")
    try:
        for source in sources:
            if source.stat().st_size > 2 * 1024 * 1024:
                raise ArchiveError("archive_python_source_limit")
            tree = ast.parse(source.read_bytes())
            if source == entrypoint:
                for node in tree.body:
                    if isinstance(node, ast.Assign):
                        for target in node.targets:
                            if (
                                isinstance(target, ast.Name)
                                and target.id == "__version__"
                            ):
                                if isinstance(node.value, ast.Constant) and isinstance(
                                    node.value.value, str
                                ):
                                    metadata["version"] = node.value.value[:100]
    except (SyntaxError, ValueError, UnicodeError, RecursionError) as error:
        raise ArchiveError("archive_python_invalid") from error
    return candidate, scopes, metadata


def static_requirements(
    scopes: list[Path], metadata: dict[str, Any] | None = None
) -> list[str]:
    requirements: list[str] = []
    for scope in scopes:
        for name in ("requirements.txt", "requirement.txt"):
            path = scope / name
            if path.is_file():
                if path.stat().st_size > 1024 * 1024:
                    raise ArchiveError("archive_dependency_metadata_limit")
                requirements.extend(
                    line.strip()
                    for line in path.read_text(encoding="utf-8-sig").splitlines()
                    if line.strip() and not line.lstrip().startswith("#")
                )
        path = scope / "pyproject.toml"
        if path.is_file():
            if path.stat().st_size > 1024 * 1024:
                raise ArchiveError("archive_dependency_metadata_limit")
            try:
                data = tomllib.loads(path.read_text(encoding="utf-8-sig"))
                project = data.get("project", {})
                python = project.get("requires-python")
                if python is not None:
                    if not isinstance(python, str):
                        raise ArchiveError("archive_requires_python_invalid")
                    try:
                        compatible = SpecifierSet(python).contains(
                            platform.python_version(), prereleases=True
                        )
                    except InvalidSpecifier as error:
                        raise ArchiveError("archive_requires_python_invalid") from error
                    if not compatible:
                        raise ArchiveError("archive_python_incompatible")
                    if metadata is not None:
                        metadata.setdefault("requires_python", []).append(python)
                if "dependencies" in project.get("dynamic", []):
                    raise ArchiveError("archive_dynamic_dependencies")
                values = project.get("dependencies", [])
                if not isinstance(values, list) or not all(
                    isinstance(v, str) for v in values
                ):
                    raise ArchiveError("archive_dependency_metadata_invalid")
                requirements.extend(values)
                poetry = data.get("tool", {}).get("poetry", {})
                if poetry.get("dependencies"):
                    poetry_values, poetry_python = poetry_dependencies(poetry)
                    requirements.extend(poetry_values)
                    if poetry_python:
                        if not SpecifierSet(poetry_python).contains(
                            platform.python_version(), prereleases=True
                        ):
                            raise ArchiveError("archive_python_incompatible")
                        if metadata is not None:
                            metadata.setdefault("requires_python", []).append(
                                poetry_python
                            )
            except (ValueError, TypeError, AttributeError) as error:
                if isinstance(error, ArchiveError):
                    raise
                if isinstance(error, MetadataError):
                    raise ArchiveError(str(error)) from error
                raise ArchiveError("archive_dependency_metadata_invalid") from error
        if (scope / "setup.py").exists() or (scope / "setup.cfg").exists():
            raise ArchiveError("archive_setup_dependencies_unsupported")
    if len(requirements) > 1000:
        raise ArchiveError("archive_dependency_metadata_limit")
    for value in requirements:
        try:
            requirement = Requirement(value)
        except InvalidRequirement as error:
            raise ArchiveError("archive_requirement_unsupported") from error
        if requirement.url or requirement.name.lower() == "nonebot":
            raise ArchiveError("archive_requirement_unsupported")
    return sorted(set(requirements))


async def inspect_in_worker(path: Path, kind: str) -> dict[str, Any]:
    """Own and reap a static inspector; cancellation never leaves a writer alive."""
    job = await _start_process(
        path,
        sys.executable,
        "-m",
        "zhenxun.plugin_archive_worker",
        str(path.resolve()),
        kind,
        cwd=str(Path(__file__).resolve().parents[1]),
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        process = await _wait_process(job)
        result_path = path / "inspection.json"
        if not result_path.is_file() or result_path.stat().st_size > 2 * 1024 * 1024:
            raise ArchiveError("archive_inspection_failed")
        result = json.loads(result_path.read_text(encoding="utf-8"))
        if result.get("error"):
            raise ArchiveError(
                result["error"],
                result.get("status", 400),
                candidates=result.get("candidates"),
            )
        if process.returncode:
            raise ArchiveError("archive_inspection_failed")
        return result
    except asyncio.TimeoutError as error:
        raise ArchiveError("archive_inspection_timeout", 408) from error
    finally:
        if not await _finish_process(job):
            raise ArchiveError("archive_process_cleanup_unconfirmed", 503)


_PROCESS_COMPONENT = "management:archive_processes"
_PROCESS_TIMEOUT = 60.0
_PROCESS_STOP_GRACE = 2.0
_preflight_directory: ContextVar[Path | None] = ContextVar(
    "archive_preflight_directory", default=None
)
_archive_processes: dict[str, _OwnedArchiveProcess] = {}


@dataclass
class _OwnedArchiveProcess:
    identity: str
    context: Any
    directory: Path
    preflight: Path | None
    task: asyncio.Task | None = None
    process: Any = None
    resource: Any = None
    stopping: bool = False
    killing: bool = False

    @property
    def confirmed(self) -> bool:
        return bool(
            self.task is not None
            and self.task.done()
            and not self.task.cancelled()
            and (self.process is None or self.process.returncode is not None)
        )

    def signal(self, *, kill: bool = False) -> None:
        self.stopping = True
        self.killing = self.killing or kill
        if self.process is not None and self.process.returncode is None:
            try:
                if self.killing:
                    self.process.kill()
                else:
                    self.process.terminate()
            except (ProcessLookupError, OSError):
                # A failed signal is not proof of exit; keep waiting/ownership.
                pass

    def kill(self) -> None:
        self.signal(kill=True)

    def require_recovery(self) -> None:
        self.resource.state = "leaked"
        self.resource.error_code = "archive_process_cleanup_unconfirmed"
        self.resource.detail["retained_directory"] = str(self.directory)
        self.resource.detail["preflight_directory"] = (
            str(self.preflight) if self.preflight else None
        )
        self.context.kernel.require_recovery(_PROCESS_COMPONENT)

    async def run(self, args, kwargs):
        self.process = await asyncio.create_subprocess_exec(*args, **kwargs)
        self.resource.detail["pid"] = getattr(self.process, "pid", None)
        if self.stopping:
            self.signal()
        await self.process.wait()
        return self.process


async def _start_process(directory: Path, *args, **kwargs) -> _OwnedArchiveProcess:
    from zhenxun.services.lifecycle import ComponentSpec, lifecycle_kernel

    if lifecycle_kernel.status()["snapshot_phase"] == "shutdown":
        raise ArchiveError("archive_process_shutdown", 503)
    if lifecycle_kernel.component_status(_PROCESS_COMPONENT) is None:
        lifecycle_kernel.register(
            ComponentSpec(
                _PROCESS_COMPONENT,
                stage="management",
                cancel_timeout=2 * _PROCESS_STOP_GRACE,
                failure_policy="degrade",
            ),
            lambda: None,
        )
    await lifecycle_kernel.start_components({_PROCESS_COMPONENT})
    if lifecycle_kernel.status()["snapshot_phase"] == "shutdown":
        raise ArchiveError("archive_process_shutdown", 503)
    parent = lifecycle_kernel.component_context(_PROCESS_COMPONENT)
    identity = uuid.uuid4().hex
    context = parent.create_child_scope("task", f"archive-process:{identity}")
    job = _OwnedArchiveProcess(
        identity, context, directory.resolve(), _preflight_directory.get()
    )
    job.resource = context.own_resource(
        receipt_id=f"archive-process:{job.identity}",
        provider="subprocess",
        resource_type="process",
        detail={"directory": str(job.directory), "pid": None},
        release_check=lambda: job.confirmed,
    )
    _archive_processes[job.identity] = job
    # Never inherit an HTTP request's short-lived execution lease or cancel spawn.
    coroutine = job.run(args, kwargs)
    try:
        job.task = Context().run(
            context.spawn_task,
            coroutine,
            name=f"archive-process:{job.identity}",
            persistent=False,
            cancel=job.kill,
        )
    except BaseException:
        coroutine.close()
        job.resource.state = "released"
        _archive_processes.pop(job.identity, None)
        Context().run(lifecycle_kernel._schedule_scope_close, context)
        raise

    def completed(task: asyncio.Task) -> None:
        try:
            if not task.cancelled():
                task.exception()
            if job.confirmed:
                job.resource.state = "released"
                job.resource.completed_at = datetime.now(timezone.utc).isoformat()
                _archive_processes.pop(job.identity, None)
            else:
                job.require_recovery()
        finally:
            if job.confirmed:
                # Close outside the request lease, pruning job-holding callbacks.
                Context().run(lifecycle_kernel._schedule_scope_close, context)

    job.task.add_done_callback(completed)
    return job


async def _wait_process(job: _OwnedArchiveProcess):
    from zhenxun.services.lifecycle.deadline import remaining_timeout

    done, _ = await asyncio.wait(
        {job.task}, timeout=remaining_timeout(_PROCESS_TIMEOUT)
    )
    if not done:
        raise asyncio.TimeoutError
    return job.task.result()


async def _finish_process(job: _OwnedArchiveProcess) -> bool:
    from zhenxun.services.lifecycle.deadline import remaining_timeout

    if job.confirmed:
        return True
    try:
        for kill in (False, True):
            job.signal(kill=kill)
            await asyncio.wait(
                {job.task}, timeout=remaining_timeout(_PROCESS_STOP_GRACE)
            )
            if job.confirmed:
                return True
    except asyncio.CancelledError:
        job.kill()
        job.require_recovery()
        raise
    job.require_recovery()
    return False


def _process_directory_busy(path: Path) -> bool:
    path = path.resolve()
    return any(
        not job.confirmed and (job.directory == path or job.preflight == path)
        for job in _archive_processes.values()
    )


async def _compile_wheels(requirements: list[str], constraints: dict[str, str]):
    from zhenxun.nonebot_store import dependencies as deps

    # Reuse the store's constraints/output parser, but own process cancellation:
    # its general resolver has neither a deadline nor child cleanup on cancel.
    directory = Path(tempfile.mkdtemp(prefix="zhenxun_archive_deps_"))
    job = None
    try:
        source = directory / "requirements.in"
        constraints_file = directory / "constraints.txt"
        output = directory / "requirements.txt"
        source.write_text("\n".join(requirements) + "\n", encoding="utf-8")
        deps._write_constraints(constraints_file, constraints)
        job = await _start_process(
            directory,
            "uv",
            "pip",
            "compile",
            str(source),
            "--output-file",
            str(output),
            "--constraints",
            str(constraints_file),
            "--no-header",
            "--no-annotate",
            "--python",
            sys.executable,
            "--only-binary=:all:",
            "--no-build",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            env={**os.environ, "UV_NO_PROGRESS": "1"},
        )
        try:
            process = await _wait_process(job)
            if process.returncode or not output.exists():
                return None
            if output.stat().st_size > 2 * 1024 * 1024:
                raise ArchiveError("archive_dependency_metadata_limit")
            return deps._parse_compiled(output)
        except asyncio.TimeoutError as error:
            raise ArchiveError("archive_dependency_timeout", 408) from error
        finally:
            if not await _finish_process(job):
                raise ArchiveError("archive_process_cleanup_unconfirmed", 503)
    finally:
        if job is None or job.confirmed:
            shutil.rmtree(directory, ignore_errors=True)


async def dependency_plan(requirements: list[str]) -> dict[str, Any]:
    from zhenxun.nonebot_store import dependencies as deps
    from zhenxun.plugin_archive_dependencies import archive_dependency_contract

    fingerprint = dependency_fingerprint()
    if not requirements:
        return {
            "candidate_inputs": [],
            "resolved_packages": {},
            "package_changes": {},
            "fingerprint": fingerprint,
        }
    if deps.environment_drift():
        raise ArchiveError("environment_drift", 409)
    current = deps.installed_inventory()
    core = deps.protected_core()
    # Archive installation is additive: no fallback may relax an installed pin.
    pins = dict(current)
    for required in (
        archive_dependency_contract()["packages"],
        _pending_dependency_pins(),
        core,
    ):
        for name, version in required.items():
            if name in pins and pins[name] != version:
                raise ArchiveError("archive_dependency_conflict", 409)
            pins[name] = version
    resolved = await _compile_wheels(requirements, pins)
    if resolved is None:
        raise ArchiveError("archive_wheel_dependencies_unresolved")
    if any(
        name in core and core[name] != version for name, version in resolved.items()
    ):
        raise ArchiveError("core_dependency_conflict")
    if any(
        name in pins and pins[name] != version for name, version in resolved.items()
    ):
        raise ArchiveError("archive_dependency_conflict", 409)
    if any(
        name in deps.FORBIDDEN_LAYER_PACKAGES and name not in core for name in resolved
    ):
        raise ArchiveError("archive_dependency_forbidden")
    return {
        "candidate_inputs": requirements,
        "resolved_packages": resolved,
        "package_changes": deps._package_changes(resolved, current),
        "fingerprint": fingerprint,
        "source_build_required": False,
    }


def _pending_dependency_pins() -> dict[str, str]:
    from zhenxun.nonebot_store.storage import pending_transaction as nonebot_pending

    source = _pending_source()
    other = nonebot_pending() or {}
    if (source and source.get("state") != "pending_restart") or (
        other and other.get("state") != "pending_restart"
    ):
        raise ArchiveError("plugin_transaction_not_mutable", 409)
    if other.get("source_build_confirmed") or any(
        op.get("source_build_confirmed") for op in source.get("operations", [])
    ):
        raise ArchiveError("archive_source_build_transaction_conflict", 409)
    pins = transaction.dependency_packages() if source else {}
    base = other.get("base_manifest", {}).get("packages", {})
    for name, info in other.get("target_manifest", {}).get("packages", {}).items():
        if info != base.get(name) and isinstance(info, dict) and info.get("version"):
            if name in pins and pins[name] != info["version"]:
                raise ArchiveError("cross_store_dependency_conflict", 409)
            pins[name] = info["version"]
    return pins


def dependency_fingerprint() -> str:
    from zhenxun.nonebot_store.dependencies import environment_fingerprint
    from zhenxun.nonebot_store.storage import pending_transaction as nonebot_pending
    from zhenxun.plugin_archive_dependencies import archive_dependency_contract

    _pending_dependency_pins()
    revisions = [
        _pending_source().get("revision"),
        (nonebot_pending() or {}).get("revision"),
        archive_dependency_contract(),
    ]
    return environment_fingerprint({}, pending_revision=json.dumps(revisions))


def _pending_source() -> dict[str, Any]:
    return (
        (transaction.pending_transaction() or {})
        if transaction.PENDING_FILE.exists()
        else {}
    )


def _is_link(path: Path) -> bool:
    try:
        return path.is_symlink() or bool(
            getattr(path.lstat(), "st_file_attributes", 0)
            & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        )
    except FileNotFoundError:
        return False


def _safe_tree(path: Path) -> None:
    for item in [path, *path.parents]:
        if _is_link(item):
            raise ArchiveError("archive_target_link", 409)
    if path.is_dir():
        for item in path.rglob("*"):
            if _is_link(item):
                raise ArchiveError("archive_target_link", 409)


def target_snapshot(module: str, package: bool) -> tuple[Path, dict[str, Any]]:
    _module(module)
    for item in [PLUGIN_ROOT, *PLUGIN_ROOT.parents]:
        if _is_link(item):
            raise ArchiveError("archive_target_link", 409)
    if BUILTIN_ROOT.exists() and any(
        p.stem.casefold() == module.casefold()
        for p in BUILTIN_ROOT.rglob("*")
        if p.is_dir() or p.suffix == ".py"
    ):
        raise ArchiveError("archive_builtin_forbidden", 409)
    target = PLUGIN_ROOT / (module if package else f"{module}.py")
    if PLUGIN_ROOT.exists():
        for path in PLUGIN_ROOT.iterdir():
            if path.stem.casefold() == module.casefold() and path.name != target.name:
                raise ArchiveError("archive_target_collision", 409)
    _safe_tree(target)
    receipts = StoreReceiptStore.load()
    owners = {
        key: value
        for key, value in receipts.items()
        if value.get("runtime_module") == f"zhenxun.plugins.{module}"
        or value.get("module") == module
    }
    if any(key != f"local_archive:{module}" for key in owners):
        raise ArchiveError("archive_target_managed_elsewhere", 409)
    pending = _pending_source()
    if pending and any(
        item.get("module") == module for item in pending.get("operations", [])
    ):
        raise ArchiveError("archive_target_pending", 409)
    return target, {"digest": source_digest(target) or "missing", "receipts": owners}


def _file_digest(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def payload_digest(path: Path) -> str:
    """Bind all staged bytes, including bytecode that receipt digests exclude."""
    digest = sha256()
    root = path.parent if path.is_file() else path
    for item in sorted([path] if path.is_file() else path.rglob("*")):
        digest.update(b"D" if item.is_dir() else b"F")
        digest.update(item.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        if item.is_file():
            with item.open("rb") as stream:
                while chunk := stream.read(1024 * 1024):
                    digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def _session_path(preflight_id: str) -> Path:
    if not re.fullmatch(r"[0-9a-f]{32}", preflight_id):
        raise ArchiveError("archive_preflight_not_found", 404)
    return ROOT / preflight_id


def _session(preflight_id: str, session_id: str) -> tuple[Path, dict[str, Any]]:
    path = _session_path(preflight_id)
    if not path.is_dir():
        raise ArchiveError("archive_preflight_not_found", 404)
    data = read_json_locked(path / "session.json", None)
    if (
        not isinstance(data, dict)
        or data.get("owner") != sha256(session_id.encode()).hexdigest()
    ):
        raise ArchiveError("archive_preflight_not_found", 404)
    if data["expires_at"] <= time.time():
        shutil.rmtree(path)
        raise ArchiveError("archive_preflight_expired", 410)
    return path, data


def _public(data: dict[str, Any]) -> dict[str, Any]:
    return {
        key: data[key]
        for key in (
            "preflight_id",
            "operation_id",
            "archive_digest",
            "module",
            "package",
            "filename",
            "compressed_bytes",
            "entries",
            "expanded_bytes",
            "expires_at",
            "metadata",
            "dependency_plan",
            "replace_required",
            "candidate_digest",
            "target",
            "target_digest",
        )
    }


async def preflight(
    stream: AsyncIterator[bytes], filename: str, session_id: str
) -> dict[str, Any]:
    from zhenxun.plugin_store_coordinator import plugin_store_operation_coordinator

    kind = (
        "zip"
        if filename.lower().endswith(".zip")
        else "tar.gz"
        if filename.lower().endswith(".tar.gz")
        else None
    )
    if kind is None or len(filename) > 255:
        raise ArchiveError("archive_format_unsupported")
    ROOT.mkdir(parents=True, exist_ok=True)
    for child in ROOT.iterdir():
        if child.is_dir() and re.fullmatch(r"[0-9a-f]{32}", child.name):
            if _process_directory_busy(child):
                continue
            data = read_json_locked(child / "session.json", {})
            if data.get("expires_at", child.stat().st_mtime + TTL) <= time.time():
                shutil.rmtree(child)
    if sum(p.is_dir() for p in ROOT.iterdir()) >= MAX_SESSIONS:
        raise ArchiveError("archive_preflight_capacity", 429)
    preflight_id = uuid.uuid4().hex
    path = _session_path(preflight_id)
    path.mkdir()
    token = _preflight_directory.set(path.resolve())
    try:
        archive = path / "upload"
        size = 0
        with archive.open("xb") as output:
            async for chunk in stream:
                size += len(chunk)
                if size > MAX_COMPRESSED:
                    raise ArchiveError("archive_upload_limit", 413)
                output.write(chunk)
        if not size:
            raise ArchiveError("archive_empty")
        inspected = await inspect_in_worker(path, kind)
        summary = inspected["summary"]
        candidate = path / inspected["candidate"]
        metadata = inspected["metadata"]
        module = candidate.stem if candidate.is_file() else candidate.name
        # Upload and CPU/disk inspection never own the global mutation lock.
        async with plugin_store_operation_coordinator.operation(
            owner="webui.archive_plan"
        ):
            target, snapshot = target_snapshot(module, candidate.is_dir())
            plan = await dependency_plan(inspected["requirements"])
        data = {
            **summary,
            "preflight_id": preflight_id,
            "operation_id": preflight_id,
            "owner": sha256(session_id.encode()).hexdigest(),
            "expires_at": time.time() + TTL,
            "filename": Path(filename).name,
            "archive_digest": _file_digest(archive),
            "compressed_bytes": size,
            "module": module,
            "package": candidate.is_dir(),
            "metadata": metadata,
            "candidate": candidate.relative_to(path).as_posix(),
            "candidate_digest": inspected["candidate_digest"],
            "target": str(target),
            "source_digest": inspected["source_digest"],
            "snapshot": snapshot,
            "replace_required": target.exists(),
            "dependency_plan": plan,
            "target_digest": snapshot["digest"],
        }
        write_json_locked(path / "session.json", data)
        return _public(data)
    except BaseException:
        if not _process_directory_busy(path):
            shutil.rmtree(path, ignore_errors=True)
        raise
    finally:
        _preflight_directory.reset(token)


def confirm(
    preflight_id: str, session_id: str, digest: str, *, replace: bool, trusted: bool
) -> dict[str, Any]:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage.operation_journal import (  # noqa: E501
        operation_status,
        record_operation,
    )

    _session_path(preflight_id)
    owner = sha256(session_id.encode()).hexdigest()
    # Journal replay survives committed transactions, TTL cleanup and explicit
    # upload deletion, but never crosses a login-session or digest boundary.
    previous = operation_status(preflight_id)
    if previous and previous.get("status") == "completed":
        recorded = previous["result"]
        if recorded.get("archive_owner") != owner:
            raise ArchiveError("archive_preflight_not_found", 404)
        if recorded.get("archive_digest") != digest or not trusted:
            raise ArchiveError("archive_confirmation_required")
        return operation_view(
            {k: v for k, v in recorded.items() if k != "archive_owner"}
        )
    path, data = _session(preflight_id, session_id)
    if digest != data["archive_digest"] or not trusted:
        raise ArchiveError("archive_confirmation_required")
    operation_id = data["operation_id"]
    key = f"local_archive:{data['module']}"
    candidate = path / data["candidate"]
    _safe_tree(path)
    if (
        _file_digest(path / "upload") != digest
        or payload_digest(candidate) != data["candidate_digest"]
    ):
        raise ArchiveError("archive_digest_changed", 409)
    pending = _pending_source()
    existing = next(
        (
            op
            for op in pending.get("operations", [])
            if op.get("operation_id") == operation_id
        ),
        None,
    )
    if existing:
        if existing.get("store_key") != key:
            raise ArchiveError("plugin_operation_id_conflict", 409)
        if pending.get("state") != "pending_restart":
            raise ArchiveError("plugin_transaction_not_mutable", 409)
        result = {
            "operation_id": operation_id,
            "apply_mode": "restart_pending",
            "transaction_revision": pending.get("revision"),
        }
    else:
        target, snapshot = target_snapshot(data["module"], data["package"])
        if str(target) != data["target"] or snapshot != data["snapshot"]:
            raise ArchiveError("archive_target_changed", 409)
        if data["replace_required"] and not replace:
            raise ArchiveError("archive_replace_required", 409)
        if dependency_fingerprint() != data["dependency_plan"]["fingerprint"]:
            raise ArchiveError("archive_environment_changed", 409)
        plan = data["dependency_plan"]
        receipt = {
            "source": "local_archive",
            "module": data["module"],
            "runtime_module": f"zhenxun.plugins.{data['module']}",
            "module_path": f"zhenxun.plugins.{data['module']}",
            "local_path": str(target),
            "archive_digest": digest,
            "source_digest": data["source_digest"],
            "installed_version": data["metadata"].get("version"),
            "installed_at": datetime.now(timezone.utc).isoformat(),
            "filename": data["filename"],
            "package": data["package"],
            "dependency_inputs": list(plan["candidate_inputs"]),
            "dependency_packages": dict(plan["resolved_packages"]),
        }
        record_operation(
            key,
            {
                "operation_id": operation_id,
                "action": "update" if data["replace_required"] else "install",
                "status": "running",
                "archive_owner": owner,
                "archive_digest": digest,
            },
        )
        result = transaction.stage_operation(
            action="update" if data["replace_required"] else "install",
            store_key=key,
            module=data["module"],
            runtime_module=receipt["runtime_module"],
            live_path=target,
            candidate_path=candidate,
            receipt=receipt,
            base_digest=snapshot["digest"],
            reason="local_archive_install",
            dependency_inputs=plan["candidate_inputs"],
            dependency_packages=plan["resolved_packages"],
            source_build_confirmed=False,
            operation_id=operation_id,
        )
    result.update(
        store_key=key,
        status="completed",
        restart_required=True,
        reason="local_archive_install",
    )
    result["archive_digest"] = digest
    record_operation(key, {**result, "archive_owner": owner})
    return result


def operation_view(result: dict[str, Any]) -> dict[str, Any]:
    pending = _pending_source()
    active = any(
        op.get("operation_id") == result.get("operation_id")
        for op in pending.get("operations", [])
    )
    if active and pending.get("state") == "pending_restart":
        return result
    return {
        **result,
        "restart_required": False,
        "transaction_state": pending.get("state") if active else "not_pending",
    }


def delete_preflight(preflight_id: str, session_id: str) -> None:
    path = _session_path(preflight_id)
    if not path.exists():
        return
    path, _ = _session(preflight_id, session_id)
    shutil.rmtree(path)


def archive_receipts() -> list[dict[str, Any]]:
    result = []
    pending = _pending_source()
    for key, receipt in StoreReceiptStore.load().items():
        if (
            not key.startswith("local_archive:")
            or receipt.get("source") != "local_archive"
        ):
            continue
        current_digest = None
        try:
            module = _module(key.removeprefix("local_archive:"))
            target = PLUGIN_ROOT / (
                module if receipt.get("package") else f"{module}.py"
            )
            _safe_tree(target)
            current_digest = source_digest(target)
        except ArchiveError:
            pass
        result.append(
            {
                **receipt,
                "store_key": key,
                "current_digest": current_digest,
                "pending": any(
                    op.get("store_key") == key for op in pending.get("operations", [])
                ),
            }
        )
    return result


def managed_archive(key: str) -> tuple[Path, dict[str, Any]]:
    receipt = StoreReceiptStore.load().get(key)
    if (
        not receipt
        or not key.startswith("local_archive:")
        or receipt.get("source") != "local_archive"
    ):
        raise ArchiveError("archive_receipt_not_found", 404)
    module = _module(key.removeprefix("local_archive:"))
    target, _ = target_snapshot(module, bool(receipt.get("package")))
    if (
        str(target) != receipt.get("local_path")
        or receipt.get("runtime_module") != f"zhenxun.plugins.{module}"
    ):
        raise ArchiveError("archive_receipt_invalid", 409)
    return target, receipt
