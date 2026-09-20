"""Static packaging metadata; never imports or executes uploaded Python."""

from pathlib import PurePosixPath
import re

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet


class MetadataError(ValueError):
    pass


def poetry_constraint(value: str) -> str:
    value = value.strip()
    if value in {"", "*"}:
        return ""
    if value.startswith(("^", "~")) and not value.startswith("~="):
        operator, raw = value[0], value[1:]
        if not re.fullmatch(r"\d+(?:\.\d+){0,2}", raw):
            raise MetadataError("archive_poetry_constraint_unsupported")
        parts = [int(part) for part in raw.split(".")]
        if operator == "^":
            index = next((i for i, part in enumerate(parts) if part), len(parts) - 1)
        else:
            index = min(1, len(parts) - 1)
        upper = parts[:]
        upper[index] += 1
        upper[index + 1 :] = [0] * (len(upper) - index - 1)
        value = f">={raw},<{'.'.join(map(str, upper))}"
    elif re.fullmatch(r"\d+(?:\.\d+)*(?:\.\*)?(?:[a-zA-Z0-9.+-]*)", value):
        value = "==" + value
    try:
        return str(SpecifierSet(value))
    except ValueError as error:
        raise MetadataError("archive_poetry_constraint_unsupported") from error


def _python_marker(value: str) -> str:
    specs = SpecifierSet(poetry_constraint(value))
    return " and ".join(
        f'python_full_version {spec.operator} "{spec.version}"'
        for spec in sorted(specs, key=str)
    )


def poetry_dependencies(poetry: dict) -> tuple[list[str], str | None]:
    if poetry.get("source"):
        raise MetadataError("archive_poetry_dependency_unsupported")
    dependencies = poetry.get("dependencies", {})
    if not isinstance(dependencies, dict):
        raise MetadataError("archive_dependency_metadata_invalid")
    result = []
    python = None
    for name, raw in dependencies.items():
        if name.lower() == "python":
            if not isinstance(raw, str):
                raise MetadataError("archive_requires_python_invalid")
            python = poetry_constraint(raw)
            continue
        entries = raw if isinstance(raw, list) else [raw]
        if len(entries) > 32:
            raise MetadataError("archive_dependency_metadata_limit")
        for item in entries:
            options = {"version": item} if isinstance(item, str) else item
            if not isinstance(options, dict) or set(options) - {
                "version",
                "extras",
                "markers",
                "python",
                "platform",
                "optional",
            }:
                raise MetadataError("archive_poetry_dependency_unsupported")
            version = options.get("version", "*")
            extras = options.get("extras", [])
            if (
                not isinstance(version, str)
                or not isinstance(extras, list)
                or not all(
                    isinstance(extra, str) and re.fullmatch(r"[\w.-]+", extra)
                    for extra in extras
                )
            ):
                raise MetadataError("archive_dependency_metadata_invalid")
            markers = []
            if options.get("markers"):
                if not isinstance(options["markers"], str):
                    raise MetadataError("archive_dependency_metadata_invalid")
                markers.append(options["markers"])
            if options.get("python"):
                if not isinstance(options["python"], str):
                    raise MetadataError("archive_dependency_metadata_invalid")
                markers.append(_python_marker(options["python"]))
            if options.get("platform"):
                platform = options["platform"]
                if platform not in {"linux", "darwin", "win32"}:
                    raise MetadataError("archive_poetry_platform_unsupported")
                markers.append(f'sys_platform == "{platform}"')
            requirement = name + (f"[{','.join(extras)}]" if extras else "")
            requirement += poetry_constraint(version)
            if markers:
                requirement += "; " + " and ".join(
                    f"({marker})" for marker in markers if marker
                )
            try:
                parsed = Requirement(requirement)
            except ValueError as error:
                raise MetadataError("archive_requirement_unsupported") from error
            if not isinstance(options.get("optional", False), bool):
                raise MetadataError("archive_dependency_metadata_invalid")
            if not options.get("optional", False):
                result.append(str(parsed))
    return result, python


def declared_package_paths(data: dict) -> list[PurePosixPath] | None:
    poetry = data.get("tool", {}).get("poetry", {})
    packages = poetry.get("packages")
    if packages is None:
        return None
    if not isinstance(packages, list) or not packages or len(packages) > 100:
        raise MetadataError("archive_package_declaration_invalid")
    result = []
    for item in packages:
        if not isinstance(item, dict) or set(item) - {"include", "from", "format"}:
            raise MetadataError("archive_package_declaration_invalid")
        parts = [item.get("from", ""), item.get("include", "")]
        if not parts[1] or any(
            not isinstance(part, str) or any(c in part for c in "\\:*?[]")
            for part in parts
        ):
            raise MetadataError("archive_package_declaration_invalid")
        path = PurePosixPath(*parts)
        if path.is_absolute() or ".." in path.parts or str(path) == ".":
            raise MetadataError("archive_package_declaration_invalid")
        result.append(path)
    return result
