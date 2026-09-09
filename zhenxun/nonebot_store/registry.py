from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import re
from typing import Any

import httpx
from packaging.version import InvalidVersion, Version

from zhenxun.services.network_proxy import ManagedAsyncClient

from .storage import (
    REGISTRY_CACHE_FILE,
    REGISTRY_META_FILE,
    read_json,
    utc_now,
    write_json,
)

REGISTRY_URLS = (
    "https://registry.nonebot.dev/plugins.json",
    "https://cdn.jsdelivr.net/gh/nonebot/registry@results/plugins.json",
    "https://cdn.staticaly.com/gh/nonebot/registry@results/plugins.json",
    "https://jsd.cdn.zzko.cn/gh/nonebot/registry@results/plugins.json",
    "https://mirror.ghproxy.com/https://raw.githubusercontent.com/nonebot/registry/results/plugins.json",
    "https://gh-proxy.com/https://raw.githubusercontent.com/nonebot/registry/results/plugins.json",
)
_CACHE_SECONDS = 30 * 60
_LOCK = asyncio.Lock()
_PROJECT_NAME = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$")


class RegistryUnavailableError(RuntimeError):
    pass


def _validate_entries(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError("registry_payload_invalid")
    entries: list[dict[str, Any]] = []
    required = {"module_name", "project_link", "name", "version", "valid"}
    for raw in value:
        if not isinstance(raw, dict) or not required.issubset(raw):
            continue
        project_link = str(raw["project_link"]).strip()
        module_name = str(raw["module_name"]).strip()
        version = str(raw["version"]).strip()
        try:
            Version(version)
        except InvalidVersion:
            continue
        if not _PROJECT_NAME.fullmatch(project_link):
            continue
        entry = dict(raw)
        validation_errors: list[str] = []
        if not module_name or any(
            not part.isidentifier() for part in module_name.split(".")
        ):
            validation_errors.append("registry_module_invalid")
        if validation_errors:
            entry["valid"] = False
            entry["registry_validation_errors"] = validation_errors
        entries.append(entry)
    if not entries and value:
        raise ValueError("registry_payload_has_no_valid_identity")
    return entries


def _validation_meta(entries: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "invalid_entries": sum(
            bool(entry.get("registry_validation_errors")) for entry in entries
        )
    }


def _cache_is_fresh(meta: dict[str, Any]) -> bool:
    try:
        fetched = datetime.fromisoformat(str(meta["fetched_at"]))
    except (KeyError, TypeError, ValueError):
        return False
    if fetched.tzinfo is None:
        fetched = fetched.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - fetched).total_seconds() < _CACHE_SECONDS


async def get_registry(
    *, refresh: bool = False
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    async with _LOCK:
        meta = read_json(REGISTRY_META_FILE, {})
        cached = read_json(REGISTRY_CACHE_FILE, None)
        if not refresh and _cache_is_fresh(meta):
            try:
                entries = _validate_entries(cached)
                return entries, {
                    **meta,
                    **_validation_meta(entries),
                    "cached": True,
                }
            except ValueError:
                pass

        errors: list[str] = []
        headers = {"Accept": "application/json"}
        if meta.get("etag"):
            headers["If-None-Match"] = str(meta["etag"])
        async with ManagedAsyncClient(timeout=12, follow_redirects=True) as client:
            for index, url in enumerate(REGISTRY_URLS):
                try:
                    response = await client.get(
                        url, headers=headers if index == 0 else None
                    )
                    if response.status_code == 304:
                        entries = _validate_entries(cached)
                        updated = {**meta, "fetched_at": utc_now(), "source": url}
                        write_json(REGISTRY_META_FILE, updated)
                        return entries, {
                            **updated,
                            **_validation_meta(entries),
                            "cached": True,
                        }
                    response.raise_for_status()
                    entries = _validate_entries(response.json())
                    updated = {
                        "fetched_at": utc_now(),
                        "source": url,
                        "etag": response.headers.get("etag"),
                        "count": len(entries),
                    }
                    write_json(REGISTRY_CACHE_FILE, entries)
                    write_json(REGISTRY_META_FILE, updated)
                    return entries, {
                        **updated,
                        **_validation_meta(entries),
                        "cached": False,
                    }
                except (httpx.HTTPError, ValueError, json.JSONDecodeError) as error:
                    errors.append(type(error).__name__)

        try:
            entries = _validate_entries(cached)
        except ValueError as error:
            raise RegistryUnavailableError("registry_unavailable") from error
        return entries, {
            **meta,
            **_validation_meta(entries),
            "cached": True,
            "stale": True,
            "fallback_errors": errors,
        }


async def get_registry_plugin(
    project_link: str, *, refresh: bool = False
) -> tuple[dict[str, Any], dict[str, Any]]:
    entries, meta = await get_registry(refresh=refresh)
    plugin = next(
        (item for item in entries if item.get("project_link") == project_link), None
    )
    if plugin is None:
        raise KeyError("nonebot_plugin_not_found")
    return plugin, meta
