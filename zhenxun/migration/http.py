from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from threading import BoundedSemaphore
from urllib.parse import urlsplit

import anyio
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field, SecretStr
from starlette.concurrency import run_in_threadpool

from .application import MigrationApplication
from .archive import Limits
from .errors import MigrationError
from .paths import contained_path
from .service import capabilities, discover_packages, inspect_archive, inspect_package
from .tasks import UPLOAD_CHUNK, MigrationBudget, TaskStore


class UploadPayload(BaseModel):
    total: int = Field(gt=0, le=Limits().compressed)


class SealPayload(BaseModel):
    sha256: str = Field(min_length=64, max_length=64)


class InspectPayload(BaseModel):
    upload_id: str | None = Field(default=None, max_length=32)
    path: str | None = Field(default=None, max_length=2048)
    password: SecretStr | None = None
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=100)


def session_request(request: Request) -> Request:
    """Supply safe browser reads to the same strict Origin/session validator."""
    if request.method in {"GET", "HEAD"} and not request.headers.get("origin"):
        try:
            referer = urlsplit(request.headers.get("referer", ""))
            if (
                referer.scheme in {"http", "https"}
                and referer.netloc
                and not (referer.username or referer.password)
            ):
                headers = [
                    *request.scope["headers"],
                    (b"origin", f"{referer.scheme}://{referer.netloc}".encode("ascii")),
                ]
                return Request({**request.scope, "headers": headers})
        except (ValueError, UnicodeError):
            pass
    return request


def create_router(
    project: Path,
    *,
    authentication,
    session_dependency,
    result,
    first_authorization=None,
) -> APIRouter:
    """Shared HTTP surface; callers supply their existing administrator boundary."""
    router = APIRouter(prefix="/migration", dependencies=[authentication])
    store = TaskStore(project)
    application = MigrationApplication(project)
    work = BoundedSemaphore(1)

    @contextmanager
    def boundary(*, exclusive=False):
        acquired = False
        try:
            if exclusive:
                acquired = work.acquire(blocking=False)
                if not acquired:
                    raise MigrationError("migration_inspection_busy", status=409)
            yield
        except MigrationError as error:
            raise HTTPException(error.status, error.code) from None
        except TimeoutError:
            raise HTTPException(408, "migration_upload_timeout") from None
        except OSError:
            raise HTTPException(500, "migration_storage_failed") from None
        finally:
            if acquired:
                work.release()

    @router.get("/capabilities")
    def get_capabilities():
        active = store.active()
        maintenance = bool(
            active
            and active["stage"]
            in {
                "quiescing",
                "snapshotting",
                "applying",
                "verifying",
                "committing",
                "committed",
                "rolling_back",
                "awaiting_credentials",
                "recovery_required",
            }
        )
        return result(
            {
                **capabilities(),
                "upload": not maintenance,
                "discovery": not maintenance,
                "maintenance": maintenance,
            }
        )

    @router.get("/discover")
    def discover():
        with boundary():
            return result({"items": discover_packages(project)})

    @router.post("/uploads")
    def upload(payload: UploadPayload, session: str = Depends(session_dependency)):
        with boundary():
            return result(
                store.public(store.create_upload(session, total=payload.total))
            )

    @router.get("/uploads/{identity}")
    def upload_status(identity: str, session: str = Depends(session_dependency)):
        with boundary():
            return result(
                store.public(store.read("uploads", identity, session=session))
            )

    @router.put("/uploads/{identity}/chunks")
    async def chunk(
        identity: str,
        request: Request,
        offset: int = Query(ge=0),
        sha256: str = Query(min_length=64, max_length=64),
        session: str = Depends(session_dependency),
    ):
        with boundary(exclusive=True):
            # Authenticate the upload before reading an attacker-controlled body.
            store.read("uploads", identity, session=session)
            data = bytearray()
            with anyio.fail_after(120):
                async for value in request.stream():
                    if len(data) + len(value) > UPLOAD_CHUNK:
                        raise MigrationError("migration_upload_chunk_limit", status=413)
                    data.extend(value)
            value = await run_in_threadpool(
                store.append_chunk,
                identity,
                session,
                offset=offset,
                data=bytes(data),
                digest=sha256,
            )
            return result(store.public(value))

    @router.post("/uploads/{identity}/seal")
    def seal(
        identity: str, payload: SealPayload, session: str = Depends(session_dependency)
    ):
        with boundary(exclusive=True):
            budget = MigrationBudget.start(3600)
            return result(
                store.public(
                    store.seal_upload(
                        identity, session, payload.sha256, checkpoint=budget.checkpoint
                    )
                )
            )

    @router.post("/inspect")
    def inspect(payload: InspectPayload, session: str = Depends(session_dependency)):
        with boundary(exclusive=True):
            if bool(payload.path) == bool(payload.upload_id):
                raise MigrationError("migration_package_selection_required")
            password = (
                payload.password.get_secret_value().encode()
                if payload.password
                else None
            )
            if password and len(password) > 4096:
                raise MigrationError("migration_password_invalid")
            expected = None
            inspect_call = inspect_package
            if payload.upload_id:
                value = store.read("uploads", payload.upload_id, session=session)
                if value["stage"] != "sealed" or value["expires_at"] <= store.clock():
                    raise MigrationError("migration_upload_not_ready", status=409)
                path = contained_path(
                    store.path("uploads", payload.upload_id).parent,
                    "archive.zx.part",
                    regular=True,
                )
                expected = value["sha256"]
                inspect_call = inspect_archive
            else:
                allowed = {item["path"] for item in discover_packages(project)}
                if payload.path not in allowed:
                    raise MigrationError("migration_package_not_discovered", status=404)
                path = contained_path(project, payload.path, regular=True)
            inspected = inspect_call(
                path,
                password=password,
                offset=payload.offset,
                limit=payload.limit,
                budget=MigrationBudget.start(3600),
            )
            if expected is not None and inspected["sha256"] != expected:
                raise MigrationError("migration_archive_changed", status=409)
            return result(inspected)

    @router.get("/tasks")
    def jobs(
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=20, ge=1, le=100),
    ):
        with boundary():
            return result(store.list_jobs(offset=offset, limit=limit))

    @router.get("/tasks/{identity}")
    def job(identity: str):
        with boundary():
            return result(store.public(store.read("jobs", identity)))

    @router.post("/tasks/{identity}/cancel")
    def cancel(identity: str, session: str = Depends(session_dependency)):
        with boundary():
            # Administrators may cancel an offline task after logging in again.
            return result(store.public(store.request_cancel(identity)))

    @router.get("/tasks/{identity}/recovery")
    def recovery(identity: str):
        from .service import recovery_requirements

        with boundary():
            return result(recovery_requirements(store, identity))

    @router.post("/tasks/{identity}/credentials")
    async def credentials(
        identity: str, request: Request, session: str = Depends(session_dependency)
    ):
        import asyncio

        from .control import request_control

        with boundary():
            data = bytearray()
            with anyio.fail_after(120):
                async for value in request.stream():
                    if len(data) + len(value) > 16 * 1024:
                        raise MigrationError(
                            "migration_management_body_limit", status=413
                        )
                    data.extend(value)
            import json

            try:
                payload = json.loads(data)
            except (ValueError, UnicodeError):
                raise MigrationError("migration_request_invalid") from None
            if not isinstance(payload, dict) or set(payload) != {"database"}:
                raise MigrationError("migration_database_credentials_required")
            return result(
                await asyncio.to_thread(
                    request_control,
                    project,
                    "credentials",
                    {"task_id": identity, **payload},
                )
            )

    async def payload_for(request: Request) -> dict:
        import json

        data = bytearray()
        with anyio.fail_after(120):
            async for chunk in request.stream():
                if len(data) + len(chunk) > 64 * 1024:
                    raise MigrationError("migration_private_input_limit", status=413)
                data.extend(chunk)
        try:
            value = json.loads(data)
        except (ValueError, UnicodeError):
            raise MigrationError("migration_request_invalid") from None
        if not isinstance(value, dict):
            raise MigrationError("migration_request_invalid")
        return value

    @router.get("/export/preview")
    async def export_preview(
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=100),
        session: str = Depends(session_dependency),
    ):
        with boundary(exclusive=True):
            return result(await application.preview_export(offset=offset, limit=limit))

    @router.post("/export")
    async def export(request: Request, session: str = Depends(session_dependency)):
        from .discovery import CATEGORIES
        from .snapshot import ExportOptions

        with boundary(exclusive=True):
            payload = await payload_for(request)
            if set(payload) - {
                "categories",
                "confirm_secrets",
                "dependencies",
                "password",
            }:
                raise MigrationError("migration_export_options_invalid")
            if payload.get("confirm_secrets") is not True:
                raise MigrationError(
                    "migration_sensitive_export_confirmation_required", status=409
                )
            categories = payload.get("categories", sorted(CATEGORIES))
            if not isinstance(categories, list) or not all(
                isinstance(item, str) for item in categories
            ):
                raise MigrationError("migration_categories_invalid")
            return result(
                await application.export(
                    session,
                    ExportOptions(
                        categories=frozenset(categories),
                        plaintext_confirmed=True,
                        dependencies=payload.get("dependencies", True) is True,
                    ),
                    password=payload.get("password"),
                )
            )

    @router.post("/preflight")
    async def preflight(request: Request, session: str = Depends(session_dependency)):
        with boundary(exclusive=True):
            payload = await payload_for(request)
            if (
                set(payload) != {"upload_id", "options", "private"}
                or not isinstance(payload.get("options"), dict)
                or not isinstance(payload.get("private"), dict)
            ):
                raise MigrationError("migration_restore_options_invalid", status=409)
            # A bootstrap administrator is not a console migration grant.
            if payload["options"].get("first_deployment"):
                if first_authorization is None:
                    raise MigrationError(
                        "migration_console_authorization_required", status=409
                    )
                first_authorization(session)
            return result(await application.preflight(session, **payload))

    @router.post("/packages/register")
    async def register_package(
        request: Request, session: str = Depends(session_dependency)
    ):
        with boundary(exclusive=True):
            payload = await payload_for(request)
            if (
                set(payload) != {"path", "confirm_secrets"}
                or payload.get("confirm_secrets") is not True
            ):
                raise MigrationError("migration_sensitive_export_confirmation_required")
            if payload["path"] not in {
                item["path"] for item in discover_packages(project)
            }:
                raise MigrationError("migration_package_not_discovered", status=404)
            return result(
                await application.import_package(
                    session, contained_path(project, payload["path"], regular=True)
                )
            )

    @router.get("/preflights/{identity}")
    def preflight_details(
        identity: str,
        section: str = "files",
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=100),
        session: str = Depends(session_dependency),
    ):
        with boundary():
            return result(
                application.details(
                    session, identity, section=section, offset=offset, limit=limit
                )
            )

    @router.post("/preflights/{identity}/confirm")
    async def confirm(
        identity: str, request: Request, session: str = Depends(session_dependency)
    ):
        with boundary(exclusive=True):
            payload = await payload_for(request)
            if set(payload) != {"private", "replacement_confirmed"} or not isinstance(
                payload.get("private"), dict
            ):
                raise MigrationError(
                    "migration_destructive_confirmation_required", status=409
                )
            return result(await application.confirm(session, identity, **payload))

    @router.get("/tasks/{identity}/download")
    def download(identity: str, session: str = Depends(session_dependency)):
        from .assets import LeasedDownload

        with boundary():
            return LeasedDownload(store, identity)

    return router
