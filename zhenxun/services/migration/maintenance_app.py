from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
import json
from pathlib import Path
from threading import Lock
import time
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.staticfiles import StaticFiles
from jose import JWTError

from zhenxun.utils.network import is_private_client
from zhenxun.utils.passwords import verify_password
from zhenxun.utils.webui_session import (
    SESSION_SECONDS,
    SESSION_VERSION,
    decode_token,
    issue_token,
    matches_context,
    session_response,
)

from .errors import MigrationError
from .paths import contained_path
from .tasks import TaskStore


class _BodyLimit:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        total = 0

        async def bounded_receive():
            nonlocal total
            message = await receive()
            if message["type"] == "http.request":
                total += len(message.get("body", b""))
                if total > 16 * 1024:
                    raise HTTPException(413, "migration_management_body_limit")
            return message

        await self.app(scope, bounded_receive, send)


@dataclass(frozen=True)
class ManagementSnapshot:
    username: str
    password: str = field(repr=False)
    secret: str = field(repr=False)
    private_only: bool = True
    migration_subject: str = ""
    session_context: dict | None = field(default=None, repr=False)

    @classmethod
    def parse(cls, value: dict):
        if not isinstance(value, dict) or set(value) - {
            "username",
            "password",
            "secret",
            "private_only",
            "migration_subject",
            "session_context",
        }:
            raise MigrationError("migration_management_snapshot_invalid")
        for key in ("username", "password", "secret"):
            if not isinstance(value.get(key), str) or not 1 <= len(value[key]) <= 4096:
                raise MigrationError("migration_management_snapshot_invalid")
        if type(value.get("private_only", True)) is not bool:
            raise MigrationError("migration_management_snapshot_invalid")
        if (
            not isinstance(value.get("migration_subject", ""), str)
            or len(value.get("migration_subject", "")) > 128
        ):
            raise MigrationError("migration_management_snapshot_invalid")
        context = value.get("session_context")
        if context is not None and (
            not isinstance(context, dict)
            or set(context)
            != {
                "version",
                "task_id",
                "sid",
                "sub",
                "auth_source",
                "boot_id",
                "iat",
                "exp",
            }
            or context.get("version") != SESSION_VERSION
            or any(
                not isinstance(context.get(key), str)
                for key in ("task_id", "sid", "sub", "auth_source", "boot_id")
            )
            or not context["sid"]
            or not context["sub"]
            or len(context["task_id"]) != 32
            or any(type(context.get(key)) is not int for key in ("iat", "exp"))
            or context["exp"] - context["iat"] != SESSION_SECONDS
        ):
            raise MigrationError("migration_management_snapshot_invalid")
        return cls(**value)


def _origin(request: Request) -> None:
    try:
        origin = urlsplit(request.headers.get("origin", ""))
        actual = urlsplit(str(request.base_url))
        if (
            origin.scheme not in {"http", "https"}
            or origin.username
            or origin.password
            or origin.path
            or origin.query
            or origin.fragment
            or origin.hostname != actual.hostname
            or origin.scheme != actual.scheme
            or (origin.port or (443 if origin.scheme == "https" else 80))
            != (actual.port or (443 if actual.scheme == "https" else 80))
        ):
            raise ValueError
    except ValueError:
        raise HTTPException(403, "migration_origin_forbidden") from None


def create_maintenance_app(
    project: Path, snapshot: ManagementSnapshot, identity: dict, *, lifespan=None
):
    """Original-origin task access with no NoneBot, plugin or DB imports."""
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    app.add_middleware(_BodyLimit)
    store = TaskStore(project)
    bearer = OAuth2PasswordBearer(tokenUrl="/zhenxun/api/login", auto_error=False)
    attempts = OrderedDict()
    attempt_lock = Lock()

    def result(data):
        return {"suc": True, "code": 200, "data": data, "info": "", "warning": None}

    @app.middleware("http")
    async def private_access(request: Request, call_next):
        from fastapi.responses import JSONResponse

        if snapshot.private_only and (
            not request.client or not is_private_client(request.client.host)
        ):
            return JSONResponse(
                {"detail": "migration_private_access_required"}, status_code=403
            )
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        return response

    def authenticate(request: Request, token: str | None = Depends(bearer)) -> dict:
        try:
            if request.headers.get("x-migration-session") or request.headers.get(
                "x-setup-token"
            ):
                raise HTTPException(410, "migration_session_protocol_obsolete")
            if not token or len(token) > 16 * 1024:
                raise ValueError
            claims = decode_token(token, snapshot.secret)
            if matches_context(claims, snapshot.session_context):
                from .session import require_task_access

                task_id = snapshot.session_context["task_id"]
                if task_id != identity.get("task_id"):
                    raise ValueError
                require_task_access(task_id, request.method, request.url.path)
                claims["task_id"] = task_id
            elif (
                claims.get("sub") != snapshot.username
                or claims.get("auth_source", "password") != "password"
                or claims.get("maintenance_task_id") != identity.get("task_id")
            ):
                raise ValueError
            return claims
        except MigrationError as error:
            raise HTTPException(error.status, error.code) from None
        except (JWTError, TypeError, ValueError):
            raise HTTPException(401, "migration_login_required") from None

    @app.post("/zhenxun/api/login")
    def login(request: Request, payload: OAuth2PasswordRequestForm = Depends()):
        _origin(request)
        if len(payload.password) > 4096 or len(payload.username) > 4096:
            raise HTTPException(413, "migration_password_limit")
        peer = request.client.host if request.client else "unknown"
        now = time.time()
        with attempt_lock:
            count, until = attempts.get(peer, (0, now + 60))
            if until <= now:
                count, until = 0, now + 60
            if count >= 5:
                raise HTTPException(429, "migration_login_rate_limited")
            attempts[peer] = (count + 1, until)
            attempts.move_to_end(peer)
            while len(attempts) > 2048:
                attempts.popitem(last=False)
        if payload.username != snapshot.username or not verify_password(
            payload.password, snapshot.password
        ):
            raise HTTPException(401, "migration_login_required")
        with attempt_lock:
            attempts.pop(peer, None)
        token = issue_token(
            snapshot.secret,
            snapshot.username,
            claims={
                "auth_source": "password",
                "maintenance_task_id": identity["task_id"],
            },
        )
        return result(session_response(token, decode_token(token, snapshot.secret)))

    @app.post("/zhenxun/api/auth/activity")
    def session_activity(claims=Depends(authenticate), token=Depends(bearer)):
        return result(
            {
                **session_response(token, claims),
                "absolute_expires_in": max(0, int(claims["exp"] - time.time())),
            }
        )

    @app.get("/zhenxun/api/system/startup/status")
    def status():
        return result(
            {
                **identity,
                "session_auth_version": SESSION_VERSION,
                "state": "maintenance",
                "operating_mode": "maintenance",
                "accepts_bot_events": False,
                "maintenance": True,
            }
        )

    @app.get("/zhenxun/api/configure/status")
    def configuration_status():
        return result(
            {
                **identity,
                "state": "configured",
                "startup": status()["data"],
                "operating_mode": "maintenance",
                "maintenance": True,
                "accepts_bot_events": False,
            }
        )

    @app.get("/zhenxun/api/migration/tasks")
    def jobs(offset: int = 0, limit: int = 20, claims=Depends(authenticate)):
        try:
            if claims.get("task_id"):
                from .session import task_listing

                return result(
                    task_listing(store, claims["task_id"], offset=offset, limit=limit)
                )
            return result(store.list_jobs(offset=offset, limit=limit))
        except MigrationError as error:
            raise HTTPException(error.status, error.code) from None

    @app.get("/zhenxun/api/migration/capabilities")
    def capabilities(claims=Depends(authenticate)):
        from .service import capabilities as available

        return result(
            {**available(), "maintenance": True, "upload": False, "discovery": False}
        )

    @app.post("/zhenxun/api/migration/preflights/{identity}/confirm")
    async def candidate_cleanup(
        identity: str, request: Request, claims=Depends(authenticate)
    ):
        from .application import MigrationApplication

        _origin(request)
        raw = bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw) > 64 * 1024:
                raise HTTPException(413, "migration_private_input_limit")
        try:
            payload = json.loads(raw)
            if (
                set(payload) != {"private", "candidate_action"}
                or payload["candidate_action"] != "cleanup"
                or not isinstance(payload["private"], dict)
            ):
                raise MigrationError("migration_request_invalid")
            return result(
                await MigrationApplication(project).cleanup_candidate(
                    claims["sid"], identity, payload["private"], authorized_reclaim=True
                )
            )
        except MigrationError as error:
            raise HTTPException(error.status, error.public()) from None
        except (ValueError, TypeError):
            raise HTTPException(400, "migration_request_invalid") from None

    @app.get("/zhenxun/api/migration/discover")
    def discover(claims=Depends(authenticate)):
        return result({"items": []})

    @app.get("/zhenxun/api/migration/tasks/{task_id}")
    def job(task_id: str, claims=Depends(authenticate)):
        try:
            return result(store.public(store.read("jobs", task_id)))
        except MigrationError as error:
            raise HTTPException(error.status, error.code) from None

    @app.post("/zhenxun/api/migration/tasks/{task_id}/cancel")
    def cancel(task_id: str, request: Request, claims=Depends(authenticate)):
        _origin(request)
        try:
            return result(store.public(store.request_cancel(task_id)))
        except MigrationError as error:
            raise HTTPException(error.status, error.code) from None

    @app.post("/zhenxun/api/migration/tasks/{task_id}/credentials")
    async def credentials(task_id: str, request: Request, claims=Depends(authenticate)):
        import asyncio

        from .control import request_control

        _origin(request)
        try:
            payload = await request.json()
            if not isinstance(payload, dict) or set(payload) != {"database"}:
                raise MigrationError("migration_database_credentials_required")
            return result(
                await asyncio.to_thread(
                    request_control,
                    project,
                    "credentials",
                    {"task_id": task_id, **payload},
                )
            )
        except MigrationError as error:
            raise HTTPException(error.status, error.code) from None
        except ValueError:
            raise HTTPException(400, "migration_request_invalid") from None

    @app.get("/zhenxun/api/migration/tasks/{task_id}/recovery")
    def recovery(task_id: str, claims=Depends(authenticate)):
        from .service import recovery_requirements

        try:
            return result(recovery_requirements(store, task_id))
        except MigrationError as error:
            raise HTTPException(error.status, error.code) from None

    static = contained_path(project, "data/web_ui/public")
    if not (static / "index.html").is_file():
        static = contained_path(project, "zhenxun/builtin_plugins/web_ui/public")
    if (static / "index.html").is_file():
        for name in ("js", "css", "img", "fonts"):
            directory = static / name
            if directory.is_dir():
                app.mount(f"/{name}", StaticFiles(directory=directory), name=name)

        @app.get("/version.json")
        def version():
            return FileResponse(static / "version.json")

        @app.get("/")
        def index():
            entry = static / "maintenance.html"
            return FileResponse(entry if entry.is_file() else static / "index.html")

        @app.get("/maintenance.html")
        def maintenance_page():
            entry = contained_path(static, "maintenance.html", regular=True)
            return FileResponse(entry)

    else:

        @app.get("/")
        @app.get("/maintenance.html")
        def source_maintenance_page():
            return FileResponse(Path(__file__).with_name("maintenance_fallback.html"))

    return app
