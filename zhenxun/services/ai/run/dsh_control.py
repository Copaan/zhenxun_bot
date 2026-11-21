"""DSH 会话选择与工作区展示；不替换桌面 Agent 的模型或执行权限。"""

from enum import Enum
import re

from zhenxun.services.ai.run.dsh_desktop import (
    DesktopClient,
    DesktopSessionMissing,
    DshUnavailable,
)


class Foreground(Enum):
    IDLE = "idle"
    UNLOADED = "unloaded"
    MISSING = "missing"
    BUSY = "busy"

    @classmethod
    def read(cls, transport: str, data: dict):
        """按执行端协议核验前台活动；未加载不代表旧操作已停止。"""
        if not isinstance(data, dict) or type(data.get("running")) is not bool:
            raise DshUnavailable("无法确认会话状态，请刷新后重试。")
        if transport == "sdk":
            if type(data.get("loaded")) is not bool:
                raise DshUnavailable("无法确认 SDK 会话是否已加载。")
            if not data["loaded"]:
                if (
                    data["running"]
                    or data.get("confirmed") is not False
                    or data.get("queue", []) != []
                    or data.get("pendingInput", []) != []
                ):
                    raise DshUnavailable("SDK 前台状态不一致。")
                return cls.UNLOADED
            fields = ("queue", "pendingInput") if "pendingInput" in data else ("queue",)
        elif transport == "desktop":
            fields = ("queue", "pendingInput")
        else:
            raise DshUnavailable("未知的会话执行端。")
        if any(not isinstance(data.get(key), list) for key in fields):
            raise DshUnavailable("执行端未提供完整的队列状态。")
        if data["running"] or any(data[key] for key in fields):
            return cls.BUSY
        return cls.IDLE

    @classmethod
    async def observe(cls, client, transport, remote_id):
        args = {"sessionId": remote_id}
        try:
            return cls.read(transport, await client.rpc("session/state", args))
        except DesktopSessionMissing:
            if (
                transport == "desktop"
                and await client.rpc("session/projections", args) is None
            ):
                return cls.MISSING
            raise


class DshControl:
    @classmethod
    async def actions(cls, settings, item, client):
        """按本地登记和远端前台状态给出管理操作，写入时仍需复核。"""
        if client is None:
            return "offline", "宿主不可达，请先连接", []
        if not item:
            return "new", "尚未建立会话", ["new", "select"]
        flight = item.get("inflight")
        stop = ["stop"] if flight else []
        try:
            cls.check_local(item)
        except DshUnavailable:
            return "local_active", "有运行或排队输入", stop
        cls.verify_home(item, client)
        if item.get("created") or flight:
            activity = await Foreground.observe(
                client, settings.transport, item["remote_id"]
            )
            if activity is Foreground.BUSY:
                return "remote_busy", "正在执行或等待输入", stop
            if flight:
                return "previous_unknown", "旧结果待核验，可新建", ["new", *stop]
            if activity is Foreground.MISSING:
                return (
                    "missing",
                    "原会话不存在，可新建或选择其他会话",
                    ["new", "select"],
                )
        return (
            "idle",
            "可切换会话" if item.get("quarantined") else "空闲",
            ["new", "select", "reset", "archive"],
        )

    @classmethod
    async def inspect(cls, settings, item, mode, *, clients=None):
        from zhenxun.services.ai.core.exceptions import RunInterrupted

        client = None
        try:
            key = mode if settings.transport == "sdk" else "desktop"
            if clients is not None and key in clients:
                client = clients[key]
            else:
                if clients is not None:
                    clients[key] = None
                client = await cls.client(settings, mode)
                if clients is not None:
                    clients[key] = client
            return await cls.actions(settings, item, client)
        except (
            RunInterrupted,
            DshUnavailable,
            OSError,
            ValueError,
            TypeError,
            KeyError,
        ):
            return "unverified", "状态未确认，请检查连接", []
        finally:
            if client and clients is None:
                await client.close()

    @staticmethod
    def check_local(item, context=None):
        """切换前检查会话中其他运行，包括已准入的排队请求。"""
        from .session import session_manager

        if item and any(
            active is not context
            and active.session_id == item["local_id"]
            and task
            and not task.done()
            for active, task in session_manager.runs.values()
        ):
            raise DshUnavailable("当前会话仍有运行，请先停止或等待完成后切换。")

    @staticmethod
    def verify_home(item, client):
        key = "sdk_home" if item.get("transport") == "sdk" else "desktop_home"
        if item.get(key) and item[key] != str(client.home):
            raise DshUnavailable("数据目录已变化，未操作原会话。")
        if item.get("home_identity") and item[
            "home_identity"
        ] != DshControl.home_identity(client):
            raise DshUnavailable("原会话数据目录身份不符，未操作。")

    @staticmethod
    def home_identity(client):
        """识别当前宿主数据目录，不以相同路径代替目录身份。"""
        stat = client.home.stat()
        return [stat.st_dev, stat.st_ino] if stat.st_ino else None

    @classmethod
    async def recover(cls, context, binding, client, reason):
        """仅在本轮提交之前轮换托管会话，旧运行的收据原样归档。"""
        from .dsh import DshSessions

        records = DshSessions.for_binding(binding)
        cls.verify_home(binding, client)
        flight = binding.get("inflight") or {}
        info = context.run.operations.setdefault("dsh", {})
        if (
            not records.managed(binding)
            or binding["owner"] != records.owner(context, binding["mode"])
            or flight.get("run_id") == context.run.run_id
            or info.get("submission") != "not_submitted"
            or info.get("recovered")
            or (
                context.run.operations.get("deliveries")
                or context.run.operations.get("dsh_tools")
            )
        ):
            raise DshUnavailable("无法自动更换当前会话，请在管理页处理。")
        cls.check_local(binding, context)
        activity = await Foreground.observe(
            client, binding["transport"], binding["remote_id"]
        )
        if activity is Foreground.BUSY or (
            reason == "missing"
            and binding["transport"] == "desktop"
            and activity is not Foreground.MISSING
        ):
            raise DshUnavailable("旧会话仍在运行或等待输入，未切换。")
        check = context.state["_dsh_authorization_check"]
        if await check() != binding["mode"]:
            raise DshUnavailable("本轮权限已变化，未切换。")
        context.run.cancellation_token.raise_if_cancelled()
        cls.check_local(binding, context)
        records.rotate_remote(binding, reason)
        info["recovered"] = reason

    @staticmethod
    async def client(settings, mode="agent"):
        if settings.transport == "sdk":
            from .dsh_sdk import SdkClient

            return await SdkClient.open(settings, mode)
        return await DesktopClient.open(settings.desktop_url, settings.home)

    @staticmethod
    def describe(row: dict) -> dict:
        """从被动投影取得可展示的会话设置，不返回消息或凭据。"""
        values = (row.get("projections") or {}).get("values") or {}
        selection = values.get("modelSelection") or {}
        return {
            "remote_id": row["sessionId"],
            "title": values.get("title") or row["sessionId"],
            "cwd": row.get("cwd"),
            "preset": values.get("agentPreset"),
            "model": selection.get("next") or selection.get("lastUsed"),
            "plan": values.get("plan"),
            "permissions": values.get("permissions"),
            "running": bool(row.get("running")),
            "format": row.get("formatStatus"),
            "subagent": bool(values.get("subagent")),
            "revision": (row.get("projections") or {}).get("asOfSeq"),
        }

    @classmethod
    async def list(cls, settings, owner: str, *, mode: str = "agent") -> list[dict]:
        """列出可供当前所有者接入的桌面会话，排除其他用户已认领会话。"""
        from .dsh import DshSessions

        records = DshSessions.for_settings(settings)._read()
        if mode == "chat":
            items = {}
            for row in [*records["archives"], *records["owners"].values()]:
                if (
                    row["owner"] == owner
                    and row["mode"] == "chat"
                    and not row.get("native")
                ):
                    items[row["remote_id"]] = {
                        **{
                            key: row.get(key)
                            for key in (
                                "remote_id",
                                "model",
                                "cwd",
                                "plan",
                                "preset",
                                "created",
                            )
                        },
                        "native": False,
                        "title": row.get("title") or row["remote_id"],
                        "running": bool(row.get("inflight")),
                        "quarantined": bool(row.get("quarantined")),
                        "subagent": False,
                        "format": "current",
                    }
            return list(items.values())
        claims = {
            row["remote_id"]: row["owner"]
            for row in [*records["archives"], *records["owners"].values()]
        }
        client = await cls.client(settings)
        try:
            rows = (await client.rpc("session/list", {})).get("items", [])
            workspaces = (
                {} if settings.transport == "sdk" else await client.workspace_snapshot()
            )
            archived = set(workspaces.get("archivedSessionIds") or [])
            result = []
            for row in rows:
                session_id = row.get("remote_id", row.get("sessionId", ""))
                if (
                    not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", session_id)
                    or claims.get(session_id, owner) != owner
                ):
                    continue
                item = row if settings.transport == "sdk" else cls.describe(row)
                if item["subagent"] or item["format"] != "current":
                    continue
                item["archived"] = session_id in archived
                result.append(item)
            return result
        finally:
            await client.close()

    @classmethod
    async def choose(
        cls,
        caller,
        settings,
        remote_id: str | None,
        expected_revision: int | None,
        *,
        mode: str = "agent",
        previous: dict | None = None,
    ):
        """为指定超管显式新开或接入原会话；不复制历史、不更改原模型、模式或目录。"""
        from .dsh import DshSessions
        from .session import session_manager

        records = DshSessions.for_settings(settings)
        owner = (
            previous["owner"]
            if caller is None and previous
            else records.owner_key(caller, mode)
        )
        before = records._read()["owners"].get(owner)
        if previous is not None and before != previous:
            raise DshUnavailable("会话归属或运行收据已变化，请刷新。")
        revision = before["revision"] if before else 0
        if expected_revision is not None and revision != expected_revision:
            raise DshUnavailable("当前会话已变化，请刷新后重新选择。")

        cls.check_local(before)
        action = "select" if remote_id else "new"
        _, label, allowed = await cls.inspect(settings, before, mode)
        if action not in allowed:
            raise DshUnavailable(label)
        selected = None
        if remote_id:
            matches = [
                item
                for item in await cls.list(settings, owner, mode=mode)
                if item["remote_id"] == remote_id
            ]
            if len(matches) != 1:
                raise DshUnavailable("会话不存在、由其他用户使用或不能独立接入。")
            selected = matches[0]
            if selected["running"] or selected.get("quarantined"):
                raise DshUnavailable("目标会话正在执行，未接管或中断桌面任务。")
            if mode == "agent" or selected.get("created"):
                client = await cls.client(settings, mode)
                try:
                    state = await client.rpc("session/state", {"sessionId": remote_id})
                    if Foreground.read(settings.transport, state) is Foreground.BUSY:
                        raise DshUnavailable("目标聊天仍有执行，不能切换。")
                finally:
                    await client.close()
            if selected["preset"] in {"zhenxun-chat", "zhenxun-agent"}:
                selected["native"] = False
            else:
                selected["native"] = True
        if remote_id:
            _, label, allowed = await cls.inspect(settings, before, mode)
            if action not in allowed:
                raise DshUnavailable(label)
        cls.check_local(before)
        item = records.switch(caller, selected, revision, mode, previous=before)
        if before:
            session_manager.invalidate(before["local_id"])
        return item

    @classmethod
    async def release(cls, settings, item, *, archive=False):
        """核验前台空闲后解除关联，不取消已有任务。"""
        from .dsh import DshSessions
        from .session import session_manager

        records = DshSessions.for_binding(item)
        if records._read()["owners"].get(item["owner"]) != item:
            raise DshUnavailable("会话或运行已变化，未解除关联。")
        cls.check_local(item)
        if not item.get("created") and not item.get("inflight"):
            records.reset(item["local_id"], previous=item)
            session_manager.invalidate(item["local_id"])
            return
        client = await cls.client(settings, item["mode"])
        try:
            cls.verify_home(item, client)
            state = await client.rpc("session/state", {"sessionId": item["remote_id"]})
            activity = Foreground.read(settings.transport, state)
            if activity is Foreground.BUSY or item.get("inflight"):
                raise DshUnavailable("远端运行尚未确认停止，未解除关联。")
            cls.check_local(item)
            if archive and settings.transport == "desktop":
                await client.rpc(
                    "workspace/archiveSession", {"sessionId": item["remote_id"]}
                )
            cls.check_local(item)
            records.reset(item["local_id"], previous=item)
            session_manager.invalidate(item["local_id"])
        finally:
            await client.close()

    @classmethod
    async def stop_owned(cls, settings, item, run_id: str | None = None) -> dict:
        """依据持久收据查询并停止所属请求，不重新附着转发或重放输入。"""
        import asyncio

        from zhenxun.services.ai.core.exceptions import RunInterrupted

        from .dsh import DshSessions

        records = DshSessions.for_binding(item)
        result = {"state": "unknown", "foreground_only": bool(item.get("native"))}
        client = None

        def receipt(value):
            flight = value.get("inflight") or {}
            return (
                value["local_id"],
                value["remote_id"],
                value["revision"],
                flight.get("run_id"),
                flight.get("request_id"),
            )

        expected = receipt(item)
        try:
            records.refresh(item)
            if receipt(item) != expected:
                return {**result, "reason": "会话或运行已变化，未执行停止"}
            inflight = item.get("inflight") or {}
            if run_id and inflight.get("run_id") != run_id:
                return {**result, "reason": "运行已变化，未停止后续任务"}
            client = await cls.client(settings, item["mode"])
            cls.verify_home(item, client)
            records.refresh(item)
            if receipt(item) != expected:
                return {**result, "reason": "连接期间运行已变化，未执行停止"}
            if not inflight:
                state = await client.rpc(
                    "session/state", {"sessionId": item["remote_id"]}
                )
                activity = Foreground.read(settings.transport, state)
                if activity is Foreground.BUSY:
                    return {**result, "reason": "存在非本次转发的原生活动，未中断"}
                return {
                    **result,
                    "state": "foreground_idle"
                    if activity is Foreground.IDLE
                    else "unknown",
                }
            if settings.transport == "sdk":
                result = await client.call(
                    "stop_run",
                    {"run_id": inflight["run_id"], "session_id": item["remote_id"]},
                    timeout=10,
                )
            else:
                result["state"] = await client.stop(
                    item["remote_id"], inflight["request_id"]
                )
                if item.get("native") and result["state"] == "confirmed":
                    result["state"] = "foreground_confirmed"
            if result.get("state") in {
                "confirmed",
                "foreground_confirmed",
                "foreground_idle",
                "target_removed",
            }:
                records.refresh(item)
                if receipt(item) == expected:
                    records.update(
                        item,
                        inflight=inflight
                        if inflight.get("submission") == "unknown"
                        else None,
                        stop=result["state"],
                    )
            return result
        except asyncio.CancelledError:
            raise
        except (Exception, RunInterrupted) as error:
            return {**result, "reason": f"停止结果待核验（{type(error).__name__}）"}
        finally:
            if client:
                try:
                    await client.close()
                except Exception:
                    from zhenxun.services.log import logger

                    logger.warning("DSH 停止查询连接关闭失败", "AI")

    @classmethod
    async def parameter(cls, settings, item, parameter, value):
        """查询原生选择；SDK 仅在空闲时修改当前会话的模型参数。"""
        from .dsh import DshSessions

        rows = await cls.list(settings, item["owner"])
        selected = next((r for r in rows if r["remote_id"] == item["remote_id"]), None)
        if not selected:
            raise DshUnavailable("会话未找到，请刷新后选择。")
        if not value:
            return str(
                selected.get("preset")
                if parameter == "mode"
                else (selected.get("model") or {}).get("reasoningEffort")
                if parameter == "thinking"
                else selected.get("model")
            )
        if settings.transport != "sdk" or parameter == "mode":
            raise DshUnavailable(
                "此运行时未提供已核验的会话级修改接口；请在 DSH 原生界面处理，"
                "不会改默认值或作为 prompt 发送。"
            )
        if selected.get("running") or not selected.get("loaded"):
            raise DshUnavailable("仅能修改已加载且空闲的 SDK 会话。")
        model = dict(selected.get("model") or {})
        if not model:
            raise DshUnavailable("无法确认原模型选择。")
        if parameter == "model":
            from .dsh_sdk import SdkClient

            SdkClient.credentials(settings.model_copy(update={"sdk_model": value}))
            model["model"] = value
        else:
            if len(value) > 32 or not re.fullmatch(r"[a-zA-Z0-9_-]+", value):
                raise DshUnavailable("无效的思考等级。")
            model["reasoningEffort"] = value
        client = await cls.client(settings)
        try:
            await client.call(
                "control",
                {
                    "method": "prepare",
                    "args": {"sessionId": item["remote_id"], "model": model},
                },
            )
            confirmed = await client.call(
                "control",
                {"method": "describe", "args": {"sessionId": item["remote_id"]}},
            )
            if confirmed.get("model") != model:
                raise DshUnavailable("模型选择回读不一致，请重新查询。")
            DshSessions.for_binding(item).update(item, model=model)
            return "已修改当前 SDK 会话；其他会话与桌面默认值不变。"
        finally:
            await client.close()

    @staticmethod
    async def chat_workspace(client) -> dict:
        """普通聊天在同一空目录展示为真寻分组，历史仍按会话隔离。"""
        from .dsh import DSH_PATH

        path = DSH_PATH / "chat"
        path.mkdir(parents=True, exist_ok=True)
        result = await client.rpc("workspace/create", {"path": str(path.resolve())})
        workspace = result["workspace"]
        if workspace.get("title") != "真寻":
            changed = await client.rpc(
                "workspace/rename",
                {"workspaceId": workspace["workspaceId"], "title": "真寻"},
            )
            workspace = changed["workspace"]
        return workspace
