from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

import anyio
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field, SecretStr
from starlette.concurrency import run_in_threadpool

from .application import MigrationApplication
from .archive import Limits
from .errors import MigrationError
from .inspection import InspectionCoordinator
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
    archive_inspections = InspectionCoordinator(project, "archive")
    database_inspections = InspectionCoordinator(project, "database")

    @contextmanager
    def boundary():
        try:
            yield
        except MigrationError as error:
            raise HTTPException(error.status, error.public()) from None
        except TimeoutError:
            raise HTTPException(408, "migration_upload_timeout") from None
        except OSError:
            raise HTTPException(500, "migration_storage_failed") from None

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
        with boundary():
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
        with boundary():
            budget = MigrationBudget.start(3600)
            return result(
                store.public(
                    store.seal_upload(
                        identity, session, payload.sha256, checkpoint=budget.checkpoint
                    )
                )
            )

    @router.post("/inspect")
    async def inspect(
        payload: InspectPayload, session: str = Depends(session_dependency)
    ):
        with boundary():
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
            request = {
                "source": "upload" if payload.upload_id else "discovered",
                "upload_id": payload.upload_id,
                "path": payload.path,
                "offset": payload.offset,
                "limit": payload.limit,
                "password_fingerprint": hashlib.sha256(password or b"").hexdigest(),
            }
            key = hashlib.sha256(
                json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()

            async def run():
                inspected = await run_in_threadpool(
                    inspect_call,
                    path,
                    password=password,
                    offset=payload.offset,
                    limit=payload.limit,
                    budget=MigrationBudget.start(3600),
                )
                if expected is not None and inspected["sha256"] != expected:
                    raise MigrationError("migration_archive_changed", status=409)
                return inspected

            return result(
                await archive_inspections.submit(
                    session, key=key, request=request, runner=run
                )
            )

    @router.get("/inspections/{identity}")
    def inspection(identity: str, session: str = Depends(session_dependency)):
        with boundary():
            for coordinator in (archive_inspections, database_inspections):
                try:
                    return result(coordinator.get(identity, session))
                except MigrationError as error:
                    if error.code != "migration_record_not_found":
                        raise
            raise MigrationError("migration_record_not_found", status=404)

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
        with boundary():
            request = {"offset": offset, "limit": limit}
            key = hashlib.sha256(
                json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()

            async def run():
                return await application.preview_export(offset=offset, limit=limit)

            return result(
                await archive_inspections.submit(
                    session, key=key, request=request, runner=run
                )
            )

    @router.post("/export/connection-check")
    async def export_connection_check(session: str = Depends(session_dependency)):
        from zhenxun.configs.database import applied_database_connection
        from zhenxun.services.database_probe import probe_export_connection

        with boundary():
            try:
                fingerprint = applied_database_connection().fingerprint()
            except (OSError, ValueError):
                fingerprint = "unavailable"
            request = {"fingerprint": fingerprint}
            key = hashlib.sha256(
                json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()

            async def run():
                checked, _ = await probe_export_connection(project)
                diagnostic = (checked.get("native") or {}).get("diagnostic")
                return checked, diagnostic

            return result(
                await database_inspections.submit(
                    session, key=key, request=request, runner=run
                )
            )

    @router.post("/export")
    async def export(request: Request, session: str = Depends(session_dependency)):
        from .discovery import CATEGORIES
        from .snapshot import ExportOptions

        with boundary():
            payload = await payload_for(request)
            if set(payload) - {
                "categories",
                "confirm_secrets",
                "allow_forced_shutdown",
                "task_id",
                "dependencies",
                "password",
                "connection_fingerprint",
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
                        allow_forced_shutdown=payload.get(
                            "allow_forced_shutdown", False
                        ),
                    ),
                    password=payload.get("password"),
                    task_id=payload.get("task_id"),
                    connection_fingerprint=payload.get("connection_fingerprint"),
                )
            )

    @router.post("/preflight")
    async def preflight(request: Request, session: str = Depends(session_dependency)):
        with boundary():
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
        with boundary():
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
        with boundary():
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
