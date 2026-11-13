from __future__ import annotations

import asyncio
from copy import deepcopy
import os
import re
import time
from typing import Any, Literal
from urllib.parse import urlencode

from dotenv import dotenv_values
from fastapi import APIRouter, HTTPException, Response
import httpx
import nonebot
from nonebot.adapters.qq.exception import ActionFailed, NetworkError
import psutil
from pydantic import BaseModel, ConfigDict, Field

from zhenxun.adapters.qq_official.diagnostics import public_error_from_exception
from zhenxun.adapters.qq_official.presentation import (
    CARD_PATH,
    digest,
    read_cards,
    recover_cards,
    save_cards,
    validate_menu,
    validate_panel,
)
from zhenxun.utils.atomic_json import (
    AtomicJsonRecordError,
    mutate_json_locked,
    read_json_locked,
)

from ...base_model import Result
from ...utils import authentication
from .configuration import _credential_headers, _parse_bots, _source_path

router = APIRouter(prefix="/qq", dependencies=[authentication()])
_locks: dict[str, asyncio.Lock] = {}
_tasks: dict[tuple[str, str], asyncio.Task] = {}
_RECEIPTS = CARD_PATH.with_name("presentation-operations.json")
_BASE = "https://api.bot.qq.com"


class PresentationMutation(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    operation_id: str = Field(min_length=8, max_length=100, pattern=r"^[\w-]+$")
    payload: dict[str, Any]
    recover_corrupt: bool = False
    model_config = ConfigDict(extra="forbid")


def _saved_bot(app_id: str) -> dict:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", app_id):
        raise HTTPException(422, "机器人 AppID 无效。")
    values = dotenv_values(_source_path())
    bots = [
        item
        for item in _parse_bots(values.get("QQ_BOTS"))
        if isinstance(item, dict) and str(item.get("id")) == app_id
    ]
    if len(bots) != 1 or not bots[0].get("secret"):
        raise HTTPException(404, "此机器人没有唯一的已保存凭据，请先保存接入设置。")
    return bots[0]


async def _call(app_id: str, method: str, path: str, payload=None) -> dict:
    config = _saved_bot(app_id)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15, connect=5)) as client:
            bot = nonebot.get_bots().get(app_id)
            if (
                bot
                and getattr(getattr(bot, "bot_info", None), "secret", None)
                == config["secret"]
            ):
                headers = await asyncio.wait_for(bot.get_authorization_header(), 10)
            else:
                headers = await _credential_headers(client, app_id, config["secret"])
            response = await client.request(
                method, _BASE + path, headers=headers, json=payload
            )
            response.raise_for_status()
            data = response.json() if response.content else {}
            if not isinstance(data, dict):
                raise ValueError("QQ response is not an object")
            if data.get("code") not in (None, 0, "0"):
                raise httpx.HTTPStatusError(
                    "QQ rejected operation", request=response.request, response=response
                )
            return data
    except (httpx.HTTPStatusError, ActionFailed) as error:
        diagnostic = public_error_from_exception(error, stage="credential").to_dict()
        diagnostic.update(
            code="qq_presentation_rejected",
            message="QQ 未接受此操作，请检查错误码及配置。",
        )
        raise HTTPException(502, diagnostic) from error
    except (
        httpx.HTTPError,
        NetworkError,
        asyncio.TimeoutError,
        TimeoutError,
        ValueError,
        TypeError,
    ) as error:
        raise HTTPException(
            504,
            {
                "code": "qq_presentation_unconfirmed",
                "message": "QQ 请求超时或响应不可确认，请先重新读取，勿重复发布。",
            },
        ) from error


async def _panel_list(app_id: str, scope: str) -> dict:
    """读取指定场景的全部面板并校验分页完整性。"""
    if not isinstance(scope, str) or scope not in {"c2c", "group", "channel", "dm"}:
        raise HTTPException(422, "面板场景无效。")
    records = []
    cursor = ""
    seen = set()
    for _ in range(10):
        query = urlencode({"scope": scope, "limit": 50, "cursor": cursor})
        page = await _call(app_id, "GET", "/v2/panels?" + query)
        items = page.get("records")
        if not isinstance(items, list) or any(
            not isinstance(item, dict) for item in items
        ):
            raise HTTPException(504, "QQ 面板列表格式无法确认，未覆盖本地草稿。")
        records.extend(items)
        next_cursor = page.get("next_cursor")
        if page.get("is_end") is True or next_cursor == "":
            return {
                "panels": sorted(
                    records, key=lambda item: str(item.get("panel_id", ""))
                ),
                "scope": scope,
            }
        if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen:
            raise HTTPException(504, "QQ 面板分页不完整，不能据此发布或确认删除。")
        seen.add(next_cursor)
        cursor = next_cursor
    raise HTTPException(504, "QQ 面板分页超过安全范围，请稍后重试。")


def _snapshot(resource: dict) -> dict:
    return {"resource": resource, "revision": digest(resource)}


def _panel_path(panel_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", panel_id):
        raise HTTPException(422, "面板 ID 无效。")
    return "/v2/panels/" + panel_id


def _receipt(app_id: str, operation_id: str) -> dict | None:
    return read_json_locked(_RECEIPTS, {}, strict=True).get(app_id + ":" + operation_id)


def _recover_exited(records: dict, app_id: str) -> None:
    """核对账号执行进程，将已退出执行器的未决操作保留为待确认。"""
    for key, record in records.items():
        if (
            key.startswith(app_id + ":")
            and record.get("state") == "running"
            and _owner_exited(record)
        ):
            record.update(
                state="uncertain",
                message="原执行进程已退出，请核对远端结果，不能自动重放。",
                updated_at=time.time(),
            )


def _pending_operations(app_id: str) -> list[dict]:
    """按账号找回未决操作，不依赖浏览器保存的操作标识。"""

    def update(records: dict) -> list[dict]:
        _recover_exited(records, app_id)
        return sorted(
            (
                deepcopy(value)
                for key, value in records.items()
                if key.startswith(app_id + ":")
                and value.get("state") in {"running", "uncertain"}
            ),
            key=lambda value: (
                value["state"] != "running",
                -value.get("updated_at", 0),
            ),
        )

    return mutate_json_locked(_RECEIPTS, {}, update, strict=True)


def _record(
    app_id: str, operation_id: str, record: dict, reserve: bool = False
) -> dict | None:
    key = app_id + ":" + operation_id

    def update(records: dict) -> dict | None:
        if reserve:
            _recover_exited(records, app_id)
        old = records.get(key)
        if old and old["input_hash"] != record["input_hash"]:
            raise ValueError("操作 ID 已绑定其他输入，请重新分析。")
        if reserve and old:
            return old
        if reserve:
            for other_key, pending in records.items():
                if (
                    other_key.startswith(app_id + ":")
                    and pending.get("state") == "running"
                ):
                    raise HTTPException(
                        409,
                        {
                            "message": "此账号存在未完成操作，请先查询结果。",
                            "operation": deepcopy(pending),
                        },
                    )
        records[key] = record
        completed = sorted(
            (
                k
                for k, v in records.items()
                if v.get("state") in {"confirmed", "failed"}
            ),
            key=lambda k: records[k].get("updated_at", 0),
        )
        for stale in completed[:-100]:
            records.pop(stale, None)

    return mutate_json_locked(_RECEIPTS, {}, update, strict=True)


def _targets(payload: dict, scope: str) -> dict:
    key = "user_openids" if scope == "c2c" else "group_openids"
    values = payload.get(key, [])
    if not isinstance(values, list) or not 1 <= len(values) <= 20:
        raise ValueError("每次应提供 1 至 20 个当前账号对应的官方 OpenID。")
    if any(not isinstance(v, str) for v in values):
        raise ValueError("OpenID 必须是文本。")
    if len(set(values)) != len(values) or any(
        not isinstance(v, str)
        or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", v)
        or v.isdecimal()
        for v in values
    ):
        raise ValueError("OpenID 无效或重复；不能填写 QQ 号、群号或其他 AppID 的标识。")
    other = "group_openids" if key == "user_openids" else "user_openids"
    if payload.get(other):
        raise ValueError("关联对象与面板场景不匹配。")
    return {key: values}


def _validate(action: str, payload: dict, before: dict) -> dict:
    value = deepcopy(payload)
    if action == "menu":
        return {"menu": validate_menu(value.get("menu", {}))}
    if action in {"create", "update"}:
        value["panel"] = validate_panel(value.get("panel", {}))
        if action == "update":
            return {"panel": value["panel"]}
        scope, target = value.get("scope"), value.get("target_type", "all")
        if (
            not isinstance(scope, str)
            or not isinstance(target, str)
            or scope not in {"c2c", "group", "channel", "dm"}
            or target
            not in {
                "all",
                "specific",
            }
        ):
            raise ValueError("面板场景或生效范围无效。")
        if target == "specific":
            if scope not in {"c2c", "group"}:
                raise ValueError("频道场景仅支持全局面板。")
            value.update(_targets(value, scope))
        elif value.get("user_openids") or value.get("group_openids"):
            raise ValueError("全局面板不能关联指定对象。")
        return value
    if action == "target":
        if before.get("target_type") != "specific" or before.get("scope") not in {
            "c2c",
            "group",
        }:
            raise ValueError("仅指定用户／群面板支持关联操作。")
        if not isinstance(value.get("op"), str) or value["op"] not in {"add", "del"}:
            raise ValueError("关联操作必须为 add 或 del。")
        return {"op": value["op"], **_targets(value, before["scope"])}
    if value:
        raise ValueError("删除面板不接受额外内容。")
    return {}


def _contains(actual: Any, expected: Any) -> bool:
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            key in actual and _contains(actual[key], value)
            for key, value in expected.items()
            if key != "version"
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(expected)
            and all(_contains(a, b) for a, b in zip(actual, expected))
        )
    return actual == expected


async def _execute(
    app_id: str, request: PresentationMutation, action: str, path: str
) -> dict:
    fingerprint = digest(
        {
            "action": action,
            "path": path,
            **request.model_dump(exclude={"recover_corrupt"}),
        }
    )
    async with _locks.setdefault(app_id, asyncio.Lock()):
        await asyncio.to_thread(_pending_operations, app_id)
        prior = await asyncio.to_thread(_receipt, app_id, request.operation_id)
        if prior:
            if prior["input_hash"] != fingerprint:
                raise HTTPException(409, "同一操作 ID 的输入不能改变。")
            if prior["state"] == "running":
                return {
                    **prior,
                    "state": "uncertain",
                    "message": "之前的执行结果待确认，请读取远端配置。",
                }
            return prior
        read_path = path.removesuffix("/target")
        before = (
            await _panel_list(app_id, request.payload.get("scope", ""))
            if action == "create"
            else await _call(app_id, "GET", read_path)
        )
        if digest(before) != request.expected_revision:
            raise HTTPException(
                409,
                {
                    "message": "远端配置已变化，草稿已保留，请重新读取并比较。",
                    **_snapshot(before),
                },
            )
        try:
            payload = _validate(action, request.payload, before)
        except (TypeError, ValueError) as error:
            raise HTTPException(422, str(error)) from error
        record = {
            "input_hash": fingerprint,
            "state": "running",
            "operation_id": request.operation_id,
            "updated_at": time.time(),
            "owner_pid": os.getpid(),
            "owner_birth": psutil.Process().create_time(),
            "action": action,
            "path": read_path,
        }
        try:
            previous = await asyncio.to_thread(
                _record, app_id, request.operation_id, record, True
            )
        except ValueError as error:
            raise HTTPException(409, str(error)) from error
        if previous:
            return previous
        method = {"create": "POST", "delete": "DELETE"}.get(action, "PUT")
        try:
            result = await _call(
                app_id, method, path, payload if action != "delete" else None
            )
        except asyncio.CancelledError:
            record.update(
                state="uncertain",
                message="执行被中断，请核对远端结果，不能自动重放。",
                updated_at=time.time(),
            )
            await asyncio.shield(
                asyncio.to_thread(_record, app_id, request.operation_id, record)
            )
            raise
        except HTTPException as error:
            record.update(
                state="failed"
                if error.status_code == 502
                and isinstance(error.detail, dict)
                and (error.detail.get("http_status") or 0) < 500
                else "uncertain",
                message="QQ 未接受修改。"
                if error.status_code == 502
                else "发布结果待确认，请重新读取。",
                diagnostic=error.detail,
            )
        else:
            try:
                verify_path = (
                    _panel_path(result["panel_id"]) if action == "create" else read_path
                )
                if action == "delete":
                    listing = await _panel_list(app_id, before.get("scope", ""))
                    panels = listing.get("panels", [])
                    confirmed = isinstance(panels, list) and not any(
                        p.get("panel_id") == path.rsplit("/", 1)[-1] for p in panels
                    )
                    resource = listing
                else:
                    resource = await _call(app_id, "GET", verify_path)
                    if action == "target":
                        key = (
                            "user_openids"
                            if "user_openids" in payload
                            else "group_openids"
                        )
                        original = set(before.get(key, []))
                        changed = set(payload[key])
                        targets = (
                            original | changed
                            if payload["op"] == "add"
                            else original - changed
                        )
                        expected = {**before, key: sorted(targets)}
                        confirmed = _contains(
                            {**resource, key: sorted(resource.get(key, []))}, expected
                        )
                    else:
                        expected = (
                            payload
                            if action in {"menu", "create"}
                            else {"panel": payload["panel"]}
                        )
                        actual = deepcopy(resource)
                        expected = deepcopy(expected)
                        if action == "create":
                            for key in ("user_openids", "group_openids"):
                                if key in expected:
                                    actual[key] = sorted(actual.get(key, []))
                                    expected[key] = sorted(expected[key])
                        confirmed = _contains(actual, expected)
                record.update(
                    state="confirmed" if confirmed else "uncertain",
                    **_snapshot(resource),
                    message="已发布并回读确认。"
                    if confirmed
                    else "已返回受理结果，但回读内容不一致。",
                )
            except (HTTPException, KeyError, TypeError):
                record.update(
                    state="uncertain",
                    message="QQ 已返回受理结果，但暂时无法确认最终配置。",
                    result=result,
                )
        record["updated_at"] = time.time()
        await asyncio.to_thread(_record, app_id, request.operation_id, record)
        return record


async def _run_operation(
    app_id: str, request: PresentationMutation, action: str, path: str
) -> dict:
    """保留受理后异常中断的收据，不把未知远端结果当作失败重试。"""
    try:
        return await _execute(app_id, request, action, path)
    except BaseException:
        record = await asyncio.to_thread(_receipt, app_id, request.operation_id)
        if record and record.get("state") == "running":
            record.update(
                state="uncertain",
                message="执行中断，远端结果待确认；请先重新读取。",
                updated_at=time.time(),
            )
            await asyncio.shield(
                asyncio.to_thread(_record, app_id, request.operation_id, record)
            )
        raise


def _owner_exited(record: dict) -> bool:
    """使用进程创建时间确认旧执行器已经退出。"""
    if not record.get("owner_pid") or not record.get("owner_birth"):
        return False
    try:
        return (
            psutil.Process(record["owner_pid"]).create_time() != record["owner_birth"]
        )
    except psutil.NoSuchProcess:
        return True
    except (psutil.AccessDenied, ValueError, OSError):
        return False


async def _mutate(
    app_id: str, request: PresentationMutation, action: str, path: str
) -> Result:
    _saved_bot(app_id)
    key = (app_id, request.operation_id)
    fingerprint = digest(
        {
            "action": action,
            "path": path,
            **request.model_dump(exclude={"recover_corrupt"}),
        }
    )
    existing = _tasks.get(key)
    if existing and getattr(existing, "input_hash", None) != fingerprint:
        raise HTTPException(409, "同一操作 ID 的输入不能改变。")
    if not existing:
        existing = asyncio.create_task(_run_operation(app_id, request, action, path))
        existing.input_hash = fingerprint
        _tasks[key] = existing

        def finished(task):
            _tasks.pop(key, None)
            if not task.cancelled():
                task.exception()

        existing.add_done_callback(finished)
    result = await asyncio.shield(existing)
    return Result.ok(result, info=result.get("message", "请查询操作结果。"))


@router.get("/{app_id}/menu")
async def get_menu(app_id: str, response: Response) -> Result:
    response.headers["Cache-Control"] = "no-store"
    return Result.ok(_snapshot(await _call(app_id, "GET", "/v2/menu")))


@router.put("/{app_id}/menu")
async def put_menu(app_id: str, request: PresentationMutation) -> Result:
    return await _mutate(app_id, request, "menu", "/v2/menu")


@router.get("/{app_id}/panels")
async def get_panels(
    app_id: str,
    response: Response,
    scope: Literal["c2c", "group", "channel", "dm"] = "c2c",
) -> Result:
    response.headers["Cache-Control"] = "no-store"
    return Result.ok(_snapshot(await _panel_list(app_id, scope)))


@router.post("/{app_id}/panels")
async def create_panel(app_id: str, request: PresentationMutation) -> Result:
    return await _mutate(app_id, request, "create", "/v2/panels")


@router.get("/{app_id}/panels/{panel_id}")
async def get_panel(app_id: str, panel_id: str, response: Response) -> Result:
    response.headers["Cache-Control"] = "no-store"
    return Result.ok(_snapshot(await _call(app_id, "GET", _panel_path(panel_id))))


@router.put("/{app_id}/panels/{panel_id}")
async def update_panel(
    app_id: str, panel_id: str, request: PresentationMutation
) -> Result:
    return await _mutate(app_id, request, "update", _panel_path(panel_id))


@router.delete("/{app_id}/panels/{panel_id}")
async def delete_panel(
    app_id: str, panel_id: str, request: PresentationMutation
) -> Result:
    return await _mutate(app_id, request, "delete", _panel_path(panel_id))


@router.put("/{app_id}/panels/{panel_id}/target")
async def update_targets(
    app_id: str, panel_id: str, request: PresentationMutation
) -> Result:
    return await _mutate(app_id, request, "target", _panel_path(panel_id) + "/target")


@router.get("/{app_id}/operations/{operation_id}")
async def get_operation(app_id: str, operation_id: str, response: Response) -> Result:
    _saved_bot(app_id)
    response.headers["Cache-Control"] = "no-store"
    result = await asyncio.to_thread(_receipt, app_id, operation_id)
    if not result and (app_id, operation_id) in _tasks:
        return Result.ok(
            {
                "operation_id": operation_id,
                "state": "running",
                "message": "正在核验远端基线，尚未提交修改。",
            }
        )
    if not result:
        raise HTTPException(404, "操作记录不存在，请先读取远端配置。")
    if result["state"] == "running" and (app_id, operation_id) not in _tasks:
        if _owner_exited(result):
            result = {
                **result,
                "state": "uncertain",
                "message": "原执行进程已退出，请核对远端结果，不能自动重放。",
            }
            await asyncio.to_thread(_record, app_id, operation_id, result)
        else:
            result = {
                **result,
                "message": "原执行进程尚未确认退出，请等待或检查原实例。",
            }
    return Result.ok(result)


@router.get("/{app_id}/operations")
async def get_operations(app_id: str, response: Response) -> Result:
    _saved_bot(app_id)
    response.headers["Cache-Control"] = "no-store"
    return Result.ok(
        {"operations": await asyncio.to_thread(_pending_operations, app_id)}
    )


@router.get("/{app_id}/cards")
async def get_cards(app_id: str, response: Response) -> Result:
    _saved_bot(app_id)
    response.headers["Cache-Control"] = "no-store"
    try:
        try:
            result = await asyncio.to_thread(read_cards, app_id)
        except (AtomicJsonRecordError, ValueError):
            result = await asyncio.to_thread(recover_cards, app_id)
    except (OSError, ValueError, TimeoutError) as error:
        raise HTTPException(
            503, "图文配置暂不可读取，请检查文件权限或稍后重试；未修改原文件。"
        ) from error
    return Result.ok(result)


@router.put("/{app_id}/cards")
async def put_cards(app_id: str, request: PresentationMutation) -> Result:
    _saved_bot(app_id)
    try:
        async with _locks.setdefault(app_id, asyncio.Lock()):
            result = await asyncio.to_thread(
                recover_cards if request.recover_corrupt else save_cards,
                app_id,
                request.expected_revision,
                request.payload,
            )
    except ValueError as error:
        raise HTTPException(
            409 if "其他页面" in str(error) else 422, str(error)
        ) from error
    except (AtomicJsonRecordError, OSError, TimeoutError) as error:
        raise HTTPException(
            503, "图文配置保存未完成，请重新读取；原文件及已有备份均保留。"
        ) from error
    return Result.ok(result, "已保存，后续事件立即使用新配置；未发送测试消息。")


async def drain_presentation_tasks() -> None:
    """关闭时等待已受理操作结束，超时保留待确认收据。"""
    tasks = list(_tasks.values())
    if not tasks:
        return
    _, pending = await asyncio.wait(tasks, timeout=10)
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
