"""Target-owned package sources and child-only network policy for installers."""

import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from urllib.parse import urlsplit

from .network_proxy import ProxyPolicyError, proxy_runtime

UV_BINARY_POLICY_VERSION = 1


def uv_binary_options(
    *, wheels_only: bool, restricted_packages: tuple[str, ...] = ()
) -> list[str]:
    """Return mutually compatible uv binary/source policy arguments."""
    if wheels_only:
        # --only-binary already disables source builds. Combining it with
        # --no-build is rejected by current uv releases.
        return ["--only-binary=:all:"]
    return [
        argument
        for package in sorted(set(restricted_packages))
        for argument in ("--only-binary", package)
    ]


_uv_path: str | None = None
_uv_versions: dict[str, str] = {}


def uv_executable() -> str | None:
    """查找 uv 可执行文件

    返回:
        str | None: uv 路径，未安装时为 None
    """
    global _uv_path
    if _uv_path is None:
        _uv_path = shutil.which("uv")
    return _uv_path


def uv_command(*arguments: str) -> list[str]:
    """构造 uv 命令，未找到 uv 时保留命令名由启动子进程时报错

    参数:
        arguments: uv 子命令及参数

    返回:
        list[str]: 完整命令
    """
    return [uv_executable() or "uv", *arguments]


def uv_compile_command(
    source: Path, output: Path, constraints: Path, *options: str
) -> list[str]:
    """构造针对当前解释器的 uv pip compile 命令

    参数:
        source: 依赖声明文件
        output: 解析结果输出文件
        constraints: 约束文件
        options: 额外参数

    返回:
        list[str]: 完整命令
    """
    return uv_command(
        "pip",
        "compile",
        str(source),
        "--output-file",
        str(output),
        "--constraints",
        str(constraints),
        "--no-header",
        "--no-annotate",
        "--python",
        sys.executable,
        *options,
    )


def uv_version() -> str:
    executable = uv_executable()
    if executable is None:
        return "unavailable"
    if executable in _uv_versions:
        return _uv_versions[executable]
    try:
        result = subprocess.run(
            [executable, "--version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    version = " ".join((result.stdout or result.stderr).split()) or "unknown"
    if result.returncode == 0:
        _uv_versions[executable] = version
    return version


def resolver_configuration_error(text: str) -> bool:
    lowered = text.casefold()
    return any(
        marker in lowered
        for marker in (
            "cannot be used with",
            "unexpected argument",
            "unrecognized option",
            "unknown option",
        )
    )


def _configured_sources(environment, project: Path) -> tuple[str, str]:
    # Ignore configuration from uploaded/staging trees. Only the target's
    # configured default index and explicit index environment are authoritative.
    index = (
        environment.get("UV_DEFAULT_INDEX")
        or environment.get("UV_INDEX_URL")
        or environment.get("PIP_INDEX_URL")
    )
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib
    uv = {}
    for path in (project / "uv.toml", project / "pyproject.toml"):
        if path.is_file():
            values = tomllib.loads(path.read_text("utf-8"))
            uv = (
                values
                if path.name == "uv.toml"
                else values.get("tool", {}).get("uv", {})
            )
            break
    indexes = uv.get("index", [])
    index = (
        index
        or uv.get("index-url")
        or next((item.get("url") for item in indexes if item.get("default")), None)
    )
    extra = (
        environment.get("UV_INDEX")
        or environment.get("UV_EXTRA_INDEX_URL")
        or environment.get("PIP_EXTRA_INDEX_URL")
    )
    if extra is None:
        extra = " ".join(
            (item.get("name", "") + "=" if item.get("name") else "") + item["url"]
            for item in indexes
            if not item.get("default") and not item.get("explicit")
        )
        legacy = uv.get("extra-index-url", [])
        extra = " ".join(
            filter(None, [extra, *(legacy if isinstance(legacy, list) else [legacy])])
        )
    index = index or "https://pypi.org/simple"
    for value in [index, *extra.split()]:
        validate_index(re.sub(r"^[A-Za-z0-9_-]+=", "", value))
    return index, extra


def default_index(project: Path = Path()) -> str:
    """解析项目配置的默认包索引

    参数:
        project: 项目根目录

    返回:
        str: 默认索引地址
    """
    return _configured_sources(os.environ, project)[0]


def source_environment():
    environment = dict(os.environ)
    index, extra = _configured_sources(environment, Path())
    authentication = {
        key: value
        for key, value in environment.items()
        if re.fullmatch(r"UV_INDEX_[A-Z0-9_]+_(USERNAME|PASSWORD)", key)
    }
    for key in list(environment):
        if key.startswith(("UV_", "PIP_")) and key not in {
            "UV_CACHE_DIR",
            "UV_PYTHON",
            "UV_PYTHON_INSTALL_DIR",
        }:
            environment.pop(key)
    environment.update(authentication)
    environment.update(UV_DEFAULT_INDEX=index, UV_NO_CONFIG="1", UV_NO_PROGRESS="1")
    if extra:
        environment["UV_INDEX"] = extra
    return environment


def validate_index(index):
    parsed = urlsplit(index)
    try:
        local = (
            parsed.hostname == "localhost"
            or ipaddress.ip_address(parsed.hostname or "").is_loopback
        )
    except ValueError:
        local = False
    if parsed.scheme != "https" and not (parsed.scheme == "http" and local):
        raise ProxyPolicyError("installer_trusted_index_invalid")
    if not parsed.hostname or parsed.fragment:
        raise ProxyPolicyError("installer_trusted_index_invalid")


def source_revision():
    environment = source_environment()
    return hashlib.sha256(
        json.dumps(
            {
                key: value
                for key, value in environment.items()
                if key in {"UV_DEFAULT_INDEX", "UV_INDEX"}
                or re.fullmatch(r"UV_INDEX_[A-Z0-9_]+_(USERNAME|PASSWORD)", key)
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()


def installer_environment():
    return _apply_proxy(source_environment())


def project_environment():
    """构造 uv 项目命令（sync/lock）的子进程环境

    项目命令按 pyproject.toml 和 uv.lock 解析索引，这里只附加代理策略，
    并在虚拟环境中运行时把目标环境固定为当前解释器所在的环境。

    返回:
        dict[str, str]: 子进程环境变量
    """
    environment = {**os.environ, "UV_NO_PROGRESS": "1"}
    if sys.prefix != sys.base_prefix:
        environment["UV_PROJECT_ENVIRONMENT"] = sys.prefix
    return _apply_proxy(environment)


def _apply_proxy(environment):
    policy = proxy_runtime.policy_snapshot()
    if policy.selected(None):
        if not policy.proxy:
            raise ProxyPolicyError("proxy_configuration_unavailable")
        if urlsplit(policy.proxy).scheme not in {"http", "https", "socks5", "socks5h"}:
            raise ProxyPolicyError("proxy_installer_protocol_unsupported")
        for key in list(environment):
            if key.casefold() in {"http_proxy", "https_proxy", "all_proxy", "no_proxy"}:
                environment.pop(key)
        environment.update(
            HTTP_PROXY=policy.proxy,
            HTTPS_PROXY=policy.proxy,
            ALL_PROXY=policy.proxy,
            NO_PROXY="localhost,127.0.0.1,::1," + ",".join(policy.bypass),
        )
    return environment


_ENVIRONMENT_FAILURES = tuple(
    (re.compile(pattern), code)
    for pattern, code in (
        (
            r"cannot be used with|unexpected argument|unrecognized option"
            r"|unknown option",
            "dependency_resolver_configuration",
        ),
        (r"no space left|disk full", "dependency_disk_full"),
        (
            r"proxy authentication|\b407 proxy|\bstatus(?: code)?:? 407\b",
            "dependency_proxy_auth",
        ),
        (
            r"\bunauthorized\b|\bforbidden\b|\bstatus(?: code)?:? 40[13]\b",
            "dependency_index_auth",
        ),
        (r"\bcertificate\b|\btls\b|\bssl\b", "dependency_tls"),
        (
            r"failed to connect|tcp connect error|failed to fetch"
            r"|connection (?:was )?(?:forcibly )?(?:refused|reset|aborted|closed)"
            r"|\btimed out\b|\bdns error\b|failed to lookup address"
            r"|error sending request|request failed after"
            r"|network (?:is )?unreachable",
            "dependency_network",
        ),
    )
)
_RESOLUTION_FAILURES = tuple(
    (re.compile(pattern), code)
    for pattern, code in (
        (r"requires-python|python version|requires python", "dependency_python"),
        (
            r"no wheels|no usable wheels|source distribution"
            r"|building (?:source distributions )?is disabled",
            "dependency_source_required",
        ),
        (r"no solution|incompatible|conflict|unsatisfiable", "dependency_conflict"),
    )
)


def resolver_failure(text):
    """对 uv 解析失败输出分类并脱敏

    输出包含解析结论（No solution found / unsatisfiable）时只按 Python 版本、
    缺少 wheel、依赖冲突分类，其中出现的包名不会被当作网络或证书错误；
    否则先匹配配置、磁盘、代理、索引认证、TLS、网络错误。

    参数:
        text: uv 输出

    返回:
        dict: code 错误码、detail 脱敏后的输出、
            source_build_available 是否可尝试源码构建
    """
    lowered = text.lower()
    rules = _RESOLUTION_FAILURES
    if not re.search(r"no solution found|unsatisfiable", lowered):
        rules = _ENVIRONMENT_FAILURES + _RESOLUTION_FAILURES
    code = next(
        (code for pattern, code in rules if pattern.search(lowered)),
        "dependency_resolution_failed",
    )
    from zhenxun.services.nonebot_store.dependencies import safe_process_error

    detail = re.sub(r"https?://[^\s<>]+", "<package-source>", text)
    return {
        "code": code,
        "detail": safe_process_error(detail),
        "source_build_available": code
        in {
            "dependency_source_required",
            "dependency_resolution_failed",
        },
    }
