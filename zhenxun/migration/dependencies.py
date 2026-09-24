from __future__ import annotations

import asyncio
from functools import partial
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from urllib.parse import urlsplit
import uuid

from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from .access import private_directory
from .errors import MigrationError
from .paths import contained_path
from .tasks import MigrationBudget


def effective_requests(
    source: dict, core: dict[str, str]
) -> tuple[list[dict], list[dict]]:
    records = source.get("distributions", [])
    if not isinstance(records, list) or len(records) > 20_000:
        raise MigrationError("migration_dependency_inventory_limit")
    selected, issues = {}, []
    disabled = {
        canonicalize_name(item["distribution"])
        for item in source.get("plugin_installations", [])
        if isinstance(item, dict)
        and item.get("enabled") is False
        and isinstance(item.get("distribution"), str)
    }
    references: dict[str, list[bool]] = {}
    for item in records:
        if not isinstance(item, dict) or not item.get("effective"):
            continue
        for raw in item.get("requires_dist", []):
            try:
                requirement = Requirement(raw)
            except (InvalidRequirement, TypeError):
                raise MigrationError("migration_dependency_inventory_invalid") from None
            active = (
                not requirement.marker
                or "extra" in str(requirement.marker)
                or requirement.marker.evaluate()
            )
            references.setdefault(canonicalize_name(requirement.name), []).append(
                bool(active)
            )
    for item in records:
        if not isinstance(item, dict) or type(item.get("effective")) is not bool:
            raise MigrationError("migration_dependency_inventory_invalid")
        if not item["effective"]:
            continue
        try:
            name = canonicalize_name(item["name"], validate=True)
            version = str(Version(item["version"]))
        except (KeyError, TypeError, ValueError, InvalidVersion):
            raise MigrationError("migration_dependency_inventory_invalid") from None
        if name in selected:
            raise MigrationError("migration_dependency_effective_conflict")
        selected[name] = {"name": name, "version": version}
        if name in disabled and not any(references.get(name, [])):
            selected[name]["skip"] = "plugin_explicitly_disabled"
            continue
        if (
            item.get("requested") is False
            and references.get(name)
            and not any(references[name])
        ):
            selected[name]["skip"] = "target_platform_not_applicable"
            continue
        if name in core:
            selected[name]["skip"] = "target_core_preserved"
        elif item.get("source_mapping_required") or item.get("source_type") in {
            "editable",
            "local_directory",
            "unobserved",
        }:
            selected[name]["skip"] = "source_mapping_required"
            issues.append(
                {"name": name, "code": "migration_dependency_source_unavailable"}
            )
    return [item for _, item in sorted(selected.items()) if "skip" not in item], issues


def _pins(path: Path) -> dict[str, str]:
    if path.stat().st_size > 4 * 1024 * 1024:
        raise MigrationError("migration_dependency_resolution_limit")
    result = {}
    for line in path.read_text("utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        try:
            requirement = Requirement(line)
            specs = list(requirement.specifier)
            if (
                requirement.url
                or requirement.marker
                or len(specs) != 1
                or specs[0].operator != "=="
            ):
                raise ValueError
            name = canonicalize_name(requirement.name)
            version = str(Version(specs[0].version))
            if name in result:
                raise ValueError
            result[name] = version
        except (InvalidRequirement, InvalidVersion, ValueError):
            raise MigrationError("migration_dependency_resolution_invalid") from None
    return result


def _installed(path: Path) -> dict[str, str]:
    result = {}
    for distribution in importlib.metadata.distributions(path=[str(path)]):
        name = canonicalize_name(distribution.metadata["Name"], validate=True)
        if name in result:
            raise MigrationError("migration_dependency_duplicate_distribution")
        result[name] = str(Version(distribution.version))
    return result


def _consistency(
    candidate: Path,
    core: dict[str, str],
    declarations: tuple[str, ...] = (),
    *,
    include_core: bool = False,
) -> list[dict]:
    versions = {**core, **_installed(candidate)}
    issues = []
    extras: dict[str, set[str]] = {}
    for raw in declarations:
        declaration = Requirement(raw)
        extras.setdefault(canonicalize_name(declaration.name), set()).update(
            declaration.extras
        )
    distributions = list(importlib.metadata.distributions(path=[str(candidate)]))
    if include_core:
        for name in core:
            try:
                distributions.append(importlib.metadata.distribution(name))
            except importlib.metadata.PackageNotFoundError:
                issues.append(
                    {"name": name, "code": "migration_core_dependency_missing"}
                )
    # Propagate requested extras through the closure before checking markers.
    changed = True
    while changed:
        changed = False
        for distribution in distributions:
            owner = canonicalize_name(distribution.metadata["Name"])
            for raw in distribution.requires or []:
                requirement = Requirement(raw)
                if requirement.marker and not any(
                    requirement.marker.evaluate({"extra": extra})
                    for extra in {"", *extras.get(owner, set())}
                ):
                    continue
                selected = extras.setdefault(canonicalize_name(requirement.name), set())
                before = len(selected)
                selected.update(requirement.extras)
                changed |= len(selected) != before
    for distribution in distributions:
        owner = canonicalize_name(distribution.metadata["Name"])
        python = distribution.metadata.get("Requires-Python")
        if python and not SpecifierSet(python).contains(
            ".".join(map(str, sys.version_info[:3])), prereleases=True
        ):
            issues.append(
                {"name": owner, "code": "migration_dependency_python_incompatible"}
            )
        for raw in distribution.requires or []:
            try:
                requirement = Requirement(raw)
                if requirement.marker and not any(
                    requirement.marker.evaluate({"extra": extra})
                    for extra in {"", *extras.get(owner, set())}
                ):
                    continue
                name = canonicalize_name(requirement.name)
                if (
                    requirement.url
                    or name not in versions
                    or not requirement.specifier.contains(
                        versions[name], prereleases=True
                    )
                ):
                    issues.append(
                        {
                            "name": canonicalize_name(distribution.metadata["Name"]),
                            "dependency": name,
                            "code": "migration_dependency_unsatisfied",
                        }
                    )
            except (InvalidRequirement, ValueError):
                issues.append(
                    {
                        "name": canonicalize_name(distribution.metadata["Name"]),
                        "code": "migration_dependency_metadata_invalid",
                    }
                )
            if len(issues) >= 1000:
                raise MigrationError("migration_dependency_consistency_limit")
    return issues


class DependencyRestorer:
    def __init__(
        self,
        directory: Path,
        supervisor,
        *,
        core: dict[str, str],
        index_url: str | None = None,
        find_links: Path | None = None,
        declarations: tuple[str, ...] = (),
    ):
        self.directory = private_directory(directory)
        self.supervisor = supervisor
        self.core = {
            canonicalize_name(name, validate=True): str(Version(version))
            for name, version in core.items()
        }
        if (index_url is None) == (find_links is None):
            raise MigrationError("migration_trusted_index_required")
        if index_url is not None:
            parsed = urlsplit(index_url)
            if parsed.scheme != "https" or not parsed.hostname or parsed.fragment:
                raise MigrationError("migration_trusted_index_invalid")
        self.index_url, self.find_links = index_url, find_links
        self.declarations = []
        for raw in declarations:
            try:
                requirement = Requirement(raw)
                if requirement.url:
                    raise ValueError
                if not requirement.marker or requirement.marker.evaluate({"extra": ""}):
                    self.declarations.append(str(requirement))
            except (InvalidRequirement, ValueError, TypeError):
                raise MigrationError(
                    "migration_dependency_declaration_invalid"
                ) from None
        self.uv = shutil.which("uv")
        if self.uv is None:
            raise MigrationError("migration_dependency_tool_missing")

    async def _command(
        self, command: list[str], directory: Path, budget: MigrationBudget, checkpoint
    ) -> bool:
        output = directory / (uuid.uuid4().hex + ".output")
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("UV_", "PIP_")) and key != "PYTHONPATH"
        }
        environment.update(UV_NO_PROGRESS="1", UV_PYTHON_DOWNLOADS="never")
        process = None
        try:
            with output.open("xb") as stream:
                process = await self.supervisor.start_process(
                    "migration_dependency",
                    partial(
                        subprocess.Popen,
                        command,
                        cwd=directory,
                        env=environment,
                        stdin=subprocess.DEVNULL,
                        stdout=stream,
                        stderr=stream,
                        creationflags=subprocess.CREATE_NO_WINDOW
                        if os.name == "nt"
                        else 0,
                    ),
                    completion_expected=True,
                )
                while process.poll() is None:
                    budget.checkpoint()
                    checkpoint()
                    self.supervisor._publish_processes()
                    if output.stat().st_size > 1024 * 1024:
                        raise MigrationError("migration_dependency_output_limit")
                    await asyncio.sleep(0.05)
            budget.checkpoint()
            checkpoint()
            if output.stat().st_size > 1024 * 1024:
                raise MigrationError("migration_dependency_output_limit")
            raw = output.read_bytes().lower()
            if any(
                message in raw
                for message in (
                    b"no space left",
                    b"not enough space",
                    b"disk full",
                    b"os error 28",
                    b"os error 112",
                )
            ):
                raise MigrationError("migration_dependency_disk_exhausted")
            if process.returncode:
                # Only a solver proof permits relaxing source version pins.
                if b"no solution found" not in raw:
                    raise MigrationError("migration_dependency_execution_failed")
                if any(
                    token in raw
                    for token in (
                        b"failed to fetch",
                        b"failed to download",
                        b"connection",
                        b"timed out",
                        b"unauthorized",
                        b"forbidden",
                        b"certificate",
                        b"proxy",
                        b"401",
                        b"403",
                    )
                ):
                    raise MigrationError("migration_dependency_index_unavailable")
            return process.returncode == 0
        finally:
            if process is not None:
                await self.supervisor.stop_process(process, allow_force=True)
            try:
                output.unlink(missing_ok=True)
            except OSError:
                raise MigrationError(
                    "migration_sensitive_temporary_cleanup_failed"
                ) from None

    def _remove(self, directory: Path):
        path = contained_path(
            self.directory, directory.relative_to(self.directory).as_posix()
        )
        try:
            shutil.rmtree(path)
        except OSError:
            raise MigrationError("migration_candidate_cleanup_failed") from None

    async def restore(
        self,
        source: dict,
        *,
        budget: MigrationBudget,
        checkpoint=lambda: None,
        progress=None,
    ) -> dict:
        requests, missing = effective_requests(source, self.core)
        if progress is not None:
            progress(step="分析依赖声明", current=0, total=len(requests), unit="包")
        requested_names = {item["name"] for item in requests}
        unavailable_names = {item["name"] for item in missing}
        disabled_names = {
            canonicalize_name(item["distribution"])
            for item in source.get("plugin_installations", [])
            if isinstance(item, dict)
            and item.get("enabled") is False
            and isinstance(item.get("distribution"), str)
        }
        excluded = [
            {
                "name": canonicalize_name(item["name"]),
                "reason": "plugin_explicitly_disabled"
                if canonicalize_name(item["name"]) in disabled_names
                else "target_platform_not_applicable",
            }
            for item in source.get("distributions", [])
            if item["effective"]
            and canonicalize_name(item["name"])
            not in requested_names | unavailable_names | self.core.keys()
        ]
        accepted: dict[str, str] = {}
        attempts, relaxed = [], []
        current = private_directory(self.directory / "candidate-empty")
        for batch in [requests] if requests or self.declarations else []:
            budget.checkpoint()
            checkpoint()
            name = "migration_dependency_set"
            for exact in (True, False):
                attempt_budget = budget.phase(900)
                directory = private_directory(self.directory / uuid.uuid4().hex)
                candidate = private_directory(directory / "site-packages")
                config = directory / "uv.toml"
                config.write_text(
                    '[[index]]\nname = "migration"\ndefault = true\nurl = '
                    + json.dumps(self.index_url)
                    + "\n"
                    if self.index_url
                    else "no-index = true\nfind-links = ["
                    + json.dumps(str(self.find_links))
                    + "]\n",
                    encoding="utf-8",
                )
                constraints = directory / "constraints.txt"
                constraints.write_text(
                    "\n".join(
                        f"{key}=={value}" for key, value in sorted(self.core.items())
                    )
                    + "\n",
                    encoding="utf-8",
                )
                roots = directory / "requirements.in"
                roots.write_text(
                    "\n".join(
                        [
                            *(
                                f"{item['name']}=={item['version']}"
                                if exact
                                else item["name"]
                                for item in batch
                            ),
                            *self.declarations,
                        ]
                    )
                    + "\n",
                    encoding="utf-8",
                )
                compiled = directory / "requirements.txt"
                common = [
                    "--config-file",
                    str(config),
                    "--no-python-downloads",
                    "--python",
                    sys.executable,
                    "--cache-dir",
                    str(self.directory / "cache"),
                ]
                success = False
                keep = False
                try:
                    if progress is not None:
                        progress(
                            step="解析原版本依赖" if exact else "解析目标兼容版本",
                            current=0,
                            total=len(requests),
                            unit="包",
                        )
                    success = await self._command(
                        [
                            self.uv,
                            "pip",
                            "compile",
                            str(roots),
                            "--constraints",
                            str(constraints),
                            "--output-file",
                            str(compiled),
                            "--no-header",
                            "--no-annotate",
                            *common,
                        ],
                        directory,
                        attempt_budget,
                        checkpoint,
                    )
                    if success:
                        resolved = _pins(compiled)
                        if any(
                            resolved.get(key, value) != value
                            for key, value in self.core.items()
                        ):
                            raise MigrationError("migration_core_dependency_conflict")
                        selected = {
                            key: value
                            for key, value in resolved.items()
                            if key not in self.core
                        }
                        compiled.write_text(
                            "\n".join(
                                f"{key}=={value}"
                                for key, value in sorted(selected.items())
                            ),
                            encoding="utf-8",
                        )
                        if progress is not None:
                            progress(
                                step="安装候选依赖",
                                current=0,
                                total=len(selected),
                                unit="包",
                            )
                        success = await self._command(
                            [
                                self.uv,
                                "pip",
                                "install",
                                "--target",
                                str(candidate),
                                "--no-deps",
                                "--requirements",
                                str(compiled),
                                *common,
                            ],
                            directory,
                            attempt_budget,
                            checkpoint,
                        )
                        if success and _installed(candidate) != selected:
                            raise MigrationError(
                                "migration_dependency_installation_unconfirmed"
                            )
                    attempts.append(
                        {
                            "name": name,
                            "requested_versions": {
                                item["name"]: item["version"] for item in batch
                            },
                            "exact": exact,
                            "state": "completed" if success else "failed",
                            "error_code": None
                            if success
                            else "migration_dependency_install_failed",
                        }
                    )
                    if success:
                        previous = (
                            current.parent
                            if current.name == "site-packages"
                            else current
                        )
                        current, accepted = candidate, selected
                        if progress is not None:
                            progress(
                                step="候选依赖已安装，核对闭包",
                                current=len(selected),
                                total=len(selected),
                                unit="包",
                                percent=100,
                            )
                        keep = True
                        self._remove(previous)
                        for item in batch:
                            actual = accepted.get(item["name"])
                            if actual != item["version"]:
                                relaxed.append(
                                    {
                                        "name": item["name"],
                                        "requested_version": item["version"],
                                        "actual_version": actual,
                                        "reason": "target_compatible_resolution",
                                    }
                                )
                        break
                finally:
                    try:
                        config.unlink(missing_ok=True)
                    except OSError:
                        raise MigrationError(
                            "migration_sensitive_temporary_cleanup_failed"
                        ) from None
                    if not keep:
                        self._remove(directory)
            else:
                missing.extend(
                    {
                        "name": item["name"],
                        "code": "migration_dependency_install_failed",
                    }
                    for item in batch
                )
                if not batch:
                    missing.append(
                        {"name": name, "code": "migration_dependency_install_failed"}
                    )
        consistency = _consistency(current, self.core, tuple(self.declarations))
        return {
            "core": self.core,
            "declarations": self.declarations,
            "source_versions": {item["name"]: item["version"] for item in requests},
            "target_environment": {
                "python": ".".join(map(str, sys.version_info[:3])),
                "platform": sys.platform,
                "abi": sys.implementation.cache_tag,
            },
            "candidate": current.relative_to(self.directory).as_posix(),
            "installed": accepted,
            "attempts": attempts,
            "relaxed": relaxed,
            "excluded": excluded,
            "missing": missing,
            "consistency": consistency,
            "state": "partial" if missing or consistency else "prepared",
            "plugin_initialization": "not_attempted",
        }
