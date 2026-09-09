"""Validated resource overlays and identity-checked compensation."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import zipfile

from packaging.specifiers import SpecifierSet
from packaging.version import Version

RESOURCE_ENTRIES = (
    "font",
    "image",
    "record",
    "text",
    "themes",
    "README.md",
    "__version__",
)


class ResourceInstallError(RuntimeError):
    pass


_INSTALL_STATE = ".zhenxun-resource-state.json"


def _baseline(target: Path) -> dict[str, str]:
    path = target / _INSTALL_STATE
    _safe(path)
    if not path.exists():
        return {}
    try:
        if path.stat().st_size > 32 * 1024**2:
            raise ValueError("oversized state")
        files = json.loads(path.read_text("utf-8"))["files"]
        if not isinstance(files, dict):
            raise ValueError("invalid files")
        return files
    except (ValueError, KeyError) as error:
        raise ResourceInstallError("resource_install_state_invalid") from error


def _git_baseline(target: Path) -> dict[str, str]:
    git = target / ".git"
    _safe(git)
    if not git.is_dir():
        return {}
    result = subprocess.run(
        ["git", "-C", str(target), "ls-tree", "-r", "HEAD", "-z"],
        capture_output=True,
        timeout=30,
        check=False,
    )
    if result.returncode:
        raise ResourceInstallError("resource_git_baseline_unavailable")
    entries = {}
    for record in result.stdout.decode("utf-8").split("\0"):
        if not record:
            continue
        metadata, name = record.split("\t", 1)
        _, kind, digest = metadata.split()
        if kind == "blob":
            entries[name] = digest
    return entries


def _git_blob(path: Path) -> str:
    _safe(path)
    digest = hashlib.sha1()
    digest.update(f"blob {path.stat().st_size}\0".encode())
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024**2), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe(path: Path) -> None:
    for item in (path, *path.parents):
        if item.is_symlink() or (
            item.exists() and getattr(item.lstat(), "st_file_attributes", 0) & 0x400
        ):
            raise ResourceInstallError(f"resource_link_forbidden: {item.name}")


def _files(root: Path):
    _safe(root)
    count = 0
    for name in RESOURCE_ENTRIES:
        entry = root / name
        _safe(entry)
        if not entry.exists():
            continue
        pending = [entry]
        while pending:
            path = pending.pop()
            _safe(path)
            count += 1
            if count > 200_000:
                raise ResourceInstallError("resource_entry_limit")
            if path.is_dir():
                pending.extend(path.iterdir())
            elif path.is_file():
                yield path
            else:
                raise ResourceInstallError("resource_special_file_forbidden")


def _digest(path: Path) -> str | None:
    _safe(path)
    if not path.exists():
        return None
    if not path.is_file():
        raise ResourceInstallError(f"resource_path_conflict: {path.name}")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_resources(root: Path, spec: Path | None = None) -> str:
    files = list(_files(root))
    if not any(
        path.relative_to(root).parts[0] == "font"
        and path.suffix.lower() in {".ttf", ".otf", ".woff", ".woff2"}
        and path.stat().st_size > 0
        for path in files
    ):
        raise ResourceInstallError("resource_font_missing")
    for name in ("palette.json", "theme.css.jinja", "partials/_base.html"):
        path = root / "themes" / "default" / name
        _safe(path)
        if not path.is_file() or not 0 < path.stat().st_size <= 8 * 1024 * 1024:
            raise ResourceInstallError(f"resource_theme_missing: {name}")
    try:
        palette = json.loads((root / "themes/default/palette.json").read_text("utf-8"))
        if not isinstance(palette, dict) or not palette:
            raise ValueError("empty palette")
        version_file = root / "__version__"
        if not version_file.is_file() or version_file.stat().st_size > 4096:
            raise ValueError("version missing")
        version = str(
            Version(version_file.read_text("utf-8").strip().split(":", 1)[-1].strip())
        )
        requirement = ">=0.0.0"
        if spec is not None and spec.is_file():
            for line in spec.read_text("utf-8").splitlines():
                if line.strip().startswith("require_resources_version:"):
                    requirement = line.split(":", 1)[1].strip().strip("'\"")
        if Version(version) not in SpecifierSet(requirement):
            raise ResourceInstallError("resource_version_incompatible")
        return version
    except (ValueError, OSError) as error:
        raise ResourceInstallError("resource_metadata_invalid") from error


def resources_ready(root: Path, spec: Path | None = None) -> bool:
    try:
        validate_resources(root, spec)
        return True
    except (ResourceInstallError, OSError):
        return False


def prepare_resource_overlay(
    staged: Path, target: Path, output: Path, *, fill_only: bool
) -> None:
    output.mkdir(parents=True, exist_ok=False)
    for source in _files(target):
        destination = output / source.relative_to(target)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    for source in _files(staged):
        relative = source.relative_to(staged)
        original = target / relative
        _safe(original)
        if original.exists() and not original.is_file():
            raise ResourceInstallError(f"resource_path_conflict: {relative}")
        destination = output / relative
        if fill_only and original.is_file():
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)


class ResourceTransaction:
    def __init__(
        self,
        staged: Path,
        target: Path,
        work: Path,
        *,
        fill_only: bool,
        spec: Path | None = None,
        protect_modified: bool = False,
    ) -> None:
        self.target, self.work, self.spec = target, work, spec
        self.entries: list[dict] = []
        self.directories: list[Path] = []
        validate_resources(staged, spec)
        initial = {
            path.relative_to(staged): _digest(target / path.relative_to(staged))
            for path in _files(staged)
        }
        source_hashes = {path.as_posix(): _digest(staged / path) for path in initial}
        if protect_modified and not fill_only:
            baseline = _baseline(target)
            git = _git_baseline(target) if not baseline else {}
            for relative, before in initial.items():
                name = relative.as_posix()
                if before is None or before == source_hashes[name]:
                    continue
                if baseline.get(name) == before:
                    continue
                if name in git and _git_blob(target / relative) == git[name]:
                    continue
                raise ResourceInstallError(
                    f"resource_overwrite_confirmation_required: {name}; "
                    "请使用 -f 确认覆盖"
                )
        work.mkdir(parents=True, exist_ok=False)
        self.overlay = work / "candidate"
        prepare_resource_overlay(staged, target, self.overlay, fill_only=fill_only)
        self.version = validate_resources(self.overlay, spec)
        self.planned = []
        # Publish the version only after every payload file has been applied.
        for relative in sorted(
            initial, key=lambda p: (p.name == "__version__", str(p))
        ):
            before = initial[relative]
            if _digest(target / relative) != before:
                raise ResourceInstallError(f"resource_target_changed: {relative}")
            if fill_only and before is not None:
                continue
            after = _digest(self.overlay / relative)
            if before != after:
                self.planned.append((relative, before, after))
        state = self.overlay / _INSTALL_STATE
        state.write_text(json.dumps({"files": source_hashes}, sort_keys=True), "utf-8")
        before, after = _digest(target / _INSTALL_STATE), _digest(state)
        if before != after:
            self.planned.append((Path(_INSTALL_STATE), before, after))

    def _record(self, value: dict) -> None:
        with (self.work / "undo.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(value, ensure_ascii=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def _parents(self, parent: Path) -> None:
        missing = []
        while not parent.exists():
            _safe(parent)
            missing.append(parent)
            parent = parent.parent
        for directory in reversed(missing):
            self._record({"mkdir": str(directory)})
            directory.mkdir()
            self.directories.append(directory)

    def apply(self) -> list[str]:
        try:
            for relative, before, after in self.planned:
                destination = self.target / relative
                if _digest(destination) != before:
                    raise ResourceInstallError(f"resource_target_changed: {relative}")
                backup = self.work / "backup" / relative
                if before is not None:
                    backup.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(destination, backup)
                    if _digest(backup) != before:
                        raise ResourceInstallError(
                            f"resource_target_changed: {relative}"
                        )
                self._parents(destination.parent)
                entry = {
                    "path": relative.as_posix(),
                    "before": before,
                    "after": after,
                    "applied": False,
                }
                self._record(entry)
                self.entries.append(entry)
                temporary = self.work / "next-file"
                shutil.copy2(self.overlay / relative, temporary)
                if _digest(temporary) != after:
                    raise ResourceInstallError(
                        f"resource_candidate_changed: {relative}"
                    )
                if _digest(destination) != before:
                    raise ResourceInstallError(f"resource_target_changed: {relative}")
                os.replace(temporary, destination)
                entry["applied"] = True
                self._record(entry)
            validate_resources(self.target, self.spec)
            return [
                item[0].as_posix()
                for item in self.planned
                if item[0].name != _INSTALL_STATE
            ]
        except BaseException as error:
            with contextlib.suppress(OSError):
                self._record({"first_error": str(error), "type": type(error).__name__})
            self.rollback()
            raise

    def rollback(self) -> None:
        conflicts = []
        for entry in reversed(self.entries):
            if not entry["applied"]:
                continue
            destination = self.target / entry["path"]
            try:
                if _digest(destination) != entry["after"]:
                    raise ResourceInstallError("unknown external change")
                if entry["before"] is None:
                    destination.unlink()
                else:
                    backup = self.work / "backup" / entry["path"]
                    if _digest(backup) != entry["before"]:
                        raise ResourceInstallError("backup identity mismatch")
                    temporary = self.work / "rollback-file"
                    shutil.copy2(backup, temporary)
                    os.replace(temporary, destination)
                entry["applied"] = False
                self._record({"rolled_back": entry["path"]})
            except (OSError, ResourceInstallError):
                conflicts.append(entry["path"])
        for directory in reversed(self.directories):
            try:
                directory.rmdir()
            except OSError:
                pass  # Never remove a directory containing an external change.
        if conflicts:
            self._record({"rollback_blocked": conflicts})
            raise ResourceInstallError(
                "resource_rollback_blocked: " + ", ".join(conflicts)
            )

    def commit(self) -> None:
        try:
            self._record({"committed": True, "version": self.version})
        except OSError:
            self.rollback()
            raise


def extract_resources_zip(archive: Path, output: Path) -> Path:
    with zipfile.ZipFile(archive) as bundle:
        entries = bundle.infolist()
        if (
            len(entries) > 200_000
            or sum(item.file_size for item in entries) > 20 * 1024**3
        ):
            raise ResourceInstallError("resource_archive_limit")
        seen = set()
        for item in entries:
            name = PurePosixPath(item.orig_filename)
            mode = item.external_attr >> 16
            if (
                name.is_absolute()
                or ".." in name.parts
                or "\\" in item.orig_filename
                or "\x00" in item.orig_filename
                or ":" in item.filename
                or stat.S_ISLNK(mode)
                or item.filename.casefold() in seen
            ):
                raise ResourceInstallError("resource_archive_path_invalid")
            seen.add(item.filename.casefold())
        output.mkdir(parents=True, exist_ok=False)
        bundle.extractall(output)
    roots = list(output.iterdir())
    if len(roots) != 1 or not roots[0].is_dir():
        raise ResourceInstallError("resource_archive_root_invalid")
    return roots[0]
