from io import StringIO
from typing import Literal
from urllib.parse import urlsplit

from dotenv import dotenv_values
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse
import nonebot
from pydantic import BaseModel, Field, ValidationError

from zhenxun.models.plugin_info import PluginInfo
from zhenxun.services.network_proxy import (
    DEFAULT_BYPASS,
    ProxyPolicy,
    ProxyPolicyError,
    credential_proxy,
    probe_proxy,
    proxy_runtime,
)
from zhenxun.services.runtime_mutation import managed_mutation

from ...base_model import Result
from ...utils import authentication
from .system.configuration import (
    ConfigurationFileUpdate,
    _path,
    _read,
    _revision,
    update_configuration_file,
)

router = APIRouter(prefix="/network-proxy", dependencies=[authentication()])


async def check_origin(request: Request):
    try:
        origin = urlsplit(request.headers.get("origin", ""))
        actual = urlsplit(str(request.base_url))

        def identity(url):
            return (
                url.scheme,
                url.hostname,
                url.port or (443 if url.scheme == "https" else 80),
            )

        valid = (
            origin.scheme in {"http", "https"}
            and not any(
                (
                    origin.username,
                    origin.password,
                    origin.path,
                    origin.query,
                    origin.fragment,
                )
            )
            and identity(origin) == identity(actual)
        )
    except ValueError:
        valid = False
    if not valid:
        raise HTTPException(403, detail={"code": "proxy_origin_forbidden"})


class ProxyDraft(BaseModel):
    mode: Literal["disabled", "global", "selected"]
    url: str = Field(default="", max_length=2048)
    username: str | None = Field(default=None, max_length=512)
    password: str | None = Field(default=None, max_length=1024)
    clear_auth: bool = False
    plugins: list[str] = Field(default_factory=list)
    core_enabled: bool = False
    bypass: list[str] = Field(default_factory=lambda: list(DEFAULT_BYPASS))
    expected_revision: str = ""
    confirm_legacy: bool = False


def _configuration():
    content = _read(_path("env"))
    values = dict(dotenv_values(stream=StringIO(content)))
    return content, values


def _error(error):
    return HTTPException(
        status_code=409
        if error.code
        in {
            "proxy_pools_draining",
            "proxy_revision_conflict",
            "proxy_legacy_confirmation_required",
        }
        else 422,
        detail={"code": error.code, "message": error.code},
    )


def _draft(payload, values):
    try:
        draft = ProxyDraft(**payload)
    except ValidationError:
        raise ProxyPolicyError("proxy_configuration_invalid") from None
    fields = {
        "SYSTEM_PROXY": credential_proxy(
            draft.url,
            str(values.get("SYSTEM_PROXY") or ""),
            draft.username,
            draft.password,
            draft.clear_auth,
        ),
        "NETWORK_PROXY_MODE": draft.mode,
        "NETWORK_PROXY_PLUGINS": draft.plugins,
        "NETWORK_PROXY_CORE_ENABLED": draft.core_enabled,
        "NETWORK_PROXY_BYPASS": draft.bypass,
    }
    return draft, fields, ProxyPolicy.from_values(fields)


@router.get("/configuration", response_model=Result, response_class=JSONResponse)
async def configuration(response: Response):
    response.headers["Cache-Control"] = "no-store"
    content, values = _configuration()
    try:
        saved = ProxyPolicy.from_values(values).public()
    except ProxyPolicyError as error:
        saved = {
            "mode": "disabled",
            "url": "",
            "plugins": [],
            "bypass": list(DEFAULT_BYPASS),
            "error_code": error.code,
        }
    return Result.ok(
        {
            "revision": _revision(content),
            "saved": saved,
            "runtime": proxy_runtime.status(),
        }
    )


@router.get("/status", response_model=Result, response_class=JSONResponse)
async def status(response: Response):
    response.headers["Cache-Control"] = "no-store"
    return Result.ok(proxy_runtime.status())


@router.get("/plugins", response_model=Result, response_class=JSONResponse)
async def plugins():
    rows = {
        item.module: {
            "module": item.module,
            "name": item.name,
            "loaded": bool(item.load_status),
            "observation": "unknown",
        }
        for item in await PluginInfo.get_plugins(load_status=None, filter_parent=False)
    }
    for plugin in nonebot.get_loaded_plugins():
        rows[plugin.id_] = {
            "module": plugin.id_,
            "name": plugin.metadata.name if plugin.metadata else plugin.name,
            "loaded": True,
            "observation": "managed_entries_only",
        }
    for module in proxy_runtime.initialize().policy.plugins:
        rows.setdefault(
            module,
            {
                "module": module,
                "name": module,
                "loaded": False,
                "missing": True,
                "observation": "unknown",
            },
        )
    for module, row in rows.items():
        row["observation"] = (
            "observed" if module in proxy_runtime.observed_owners else "unobserved"
        )
    return Result.ok({"plugins": sorted(rows.values(), key=lambda row: row["module"])})


@router.put(
    "/configuration",
    response_model=Result,
    response_class=JSONResponse,
    dependencies=[Depends(check_origin)],
)
@managed_mutation("network_proxy.configure")
async def update(payload: dict):
    content, values = _configuration()
    try:
        draft, fields, _policy = _draft(payload, values)
        if draft.expected_revision != _revision(content):
            raise ProxyPolicyError("proxy_revision_conflict")
        if (
            values.get("SYSTEM_PROXY")
            and not values.get("NETWORK_PROXY_MODE")
            and not draft.confirm_legacy
        ):
            raise ProxyPolicyError("proxy_legacy_confirmation_required")
        result = await update_configuration_file(
            "env",
            ConfigurationFileUpdate(
                expected_revision=draft.expected_revision, fields=fields
            ),
        )
    except ProxyPolicyError as error:
        raise _error(error) from None
    result.data["runtime"] = proxy_runtime.status()
    result.data["saved"] = ProxyPolicy.from_values(fields).public()
    return result


@router.post(
    "/probe",
    response_model=Result,
    response_class=JSONResponse,
    dependencies=[Depends(check_origin)],
)
async def probe(payload: dict):
    _, values = _configuration()
    try:
        _, _, policy = _draft(payload, values)
        return Result.ok(await probe_proxy(policy))
    except ProxyPolicyError as error:
        raise _error(error) from None
