"""Target-owned package sources and child-only network policy for installers."""

import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

from .network_proxy import ProxyPolicyError, proxy_runtime


def source_environment():
    environment = dict(os.environ)
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
    for path in (Path("uv.toml"), Path("pyproject.toml")):
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
        _validate_index(re.sub(r"^[A-Za-z0-9_-]+=", "", value))
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


def _validate_index(index):
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
    environment = source_environment()
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


def resolver_failure(text):
    """Classify before redaction; never infer a missing wheel from any failure."""
    lowered = text.lower()
    code = "archive_dependency_resolution_failed"
    for needles, candidate in (
        (("no space left", "disk full"), "archive_dependency_disk_full"),
        (("407", "proxy authentication"), "archive_dependency_proxy_auth"),
        (("401", "403", "unauthorized", "forbidden"), "archive_dependency_index_auth"),
        (("certificate", "tls", "ssl"), "archive_dependency_tls"),
        (
            (
                "failed to connect",
                "connection",
                "timed out",
                "dns",
                "failed to fetch",
                "network",
            ),
            "archive_dependency_network",
        ),
        (
            ("requires-python", "python version", "requires python"),
            "archive_dependency_python",
        ),
        (
            (
                "no wheels",
                "no usable wheels",
                "source distribution",
                "building is disabled",
                "building source distributions is disabled",
            ),
            "archive_dependency_source_required",
        ),
        (
            ("no solution", "incompatible", "conflict", "unsatisfiable"),
            "archive_dependency_conflict",
        ),
    ):
        if any(needle in lowered for needle in needles):
            code = candidate
            break
    from zhenxun.nonebot_store.dependencies import safe_process_error

    detail = re.sub(r"https?://[^\s<>]+", "<package-source>", text)
    return {
        "code": code,
        "detail": safe_process_error(detail),
        "source_build_available": code
        in {
            "archive_dependency_source_required",
            "archive_dependency_resolution_failed",
        },
    }
