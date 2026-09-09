from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
import secrets
from threading import Lock
import time
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.staticfiles import StaticFiles
from jose import JWTError, jwt

from zhenxun.utils.network import is_private_client
from zhenxun.utils.passwords import verify_password

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

    @classmethod
    def parse(cls, value: dict):
        if not isinstance(value, dict) or set(value) - {
            "username",
            "password",
            "secret",
            "private_only",
        }:
            raise MigrationError("migration_management_snapshot_invalid")
        for key in ("username", "password", "secret"):
            if not isinstance(value.get(key), str) or not 1 <= len(value[key]) <= 4096:
                raise MigrationError("migration_management_snapshot_invalid")
        if type(value.get("private_only", True)) is not bool:
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
            token = request.headers.get("x-migration-session") or token
            if not token or len(token) > 16 * 1024:
                raise ValueError
            token = token.removeprefix("Bearer ").removeprefix("bearer ")
            claims = jwt.decode(
                token,
                snapshot.secret,
                algorithms=["HS256"],
                options={"require_exp": True},
            )
            if claims.get("sub") != snapshot.username or not claims.get("sid"):
                raise ValueError
            if claims.get("auth_source") == "console":
                raise ValueError
            return claims
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
        token = jwt.encode(
            {
                "sub": snapshot.username,
                "sid": secrets.token_urlsafe(18),
                "iat": int(now),
                "exp": int(now) + 1800,
            },
            snapshot.secret,
            algorithm="HS256",
        )
        return result({"access_token": token, "token_type": "bearer"})

    @app.get("/zhenxun/api/system/startup/status")
    def status():
        return result(
            {
                **identity,
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
            return result(store.list_jobs(offset=offset, limit=limit))
        except MigrationError as error:
            raise HTTPException(error.status, error.code) from None

    @app.get("/zhenxun/api/migration/capabilities")
    def capabilities(claims=Depends(authenticate)):
        from .service import capabilities as available

        return result(
            {**available(), "maintenance": True, "upload": False, "discovery": False}
        )

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

    return app
