from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from .archive import Limits, require_space
from .errors import MigrationError
from .tasks import UPLOAD_CHUNK, MigrationBudget


class OnlineMigration:
    """CLI uses the existing authenticated API and its launcher handoff service."""

    def __init__(self, origin: str, management: dict, *, budget=None):
        parsed = urlsplit(origin)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise MigrationError("migration_api_origin_invalid")
        if (
            not isinstance(management, dict)
            or set(management) not in ({"username", "password"}, {"console_code"})
            or not all(
                isinstance(v, str) and 0 < len(v) <= 4096 for v in management.values()
            )
        ):
            raise MigrationError("migration_management_credentials_required")
        self.origin = origin.rstrip("/")
        self.management = dict(management)
        self.budget = budget or MigrationBudget.start()
        self.client = None

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            base_url=self.origin,
            trust_env=False,
            follow_redirects=False,
            headers={"Origin": self.origin},
        )
        try:
            bootstrap = "console_code" in self.management
            token = (
                await self.request(
                    "POST",
                    "/zhenxun/api/migration/bootstrap/session",
                    json={"code": self.management["console_code"]},
                )
                if bootstrap
                else await self.request(
                    "POST", "/zhenxun/api/login", data=self.management
                )
            )
            self.client.headers[
                "X-Migration-Session" if bootstrap else "Authorization"
            ] = f"{token['token_type']} {token['access_token']}"
            self.management.clear()
            return self
        except BaseException:
            self.management.clear()
            await self.client.aclose()
            raise

    async def __aexit__(self, *args):
        await self.client.aclose()

    async def request(self, method, path, **kwargs):
        import time

        self.budget.checkpoint()
        try:
            response = await self.client.request(
                method,
                path,
                timeout=max(0.001, min(3600, self.budget.deadline - time.monotonic())),
                **kwargs,
            )
            value = response.json()
            if response.is_error or not value.get("suc"):
                code = value.get("detail", "migration_api_failed")
                import re

                if not isinstance(code, str) or not re.fullmatch(
                    r"migration_[a-z0-9_]+", code
                ):
                    code = (
                        "migration_login_required"
                        if response.status_code == 401
                        else "migration_api_failed"
                    )
                raise MigrationError(code, status=response.status_code)
            self.budget.checkpoint()
            return value["data"]
        except (httpx.HTTPError, ValueError, KeyError):
            raise MigrationError("migration_api_connection_failed") from None

    async def restore(self, archive: Path, *, options, private):
        size = archive.stat().st_size
        if archive.suffix.casefold() != ".zx" or not 0 < size <= Limits().compressed:
            raise MigrationError("migration_archive_size_or_extension_invalid")
        prefix = "/zhenxun/api/migration"
        upload = await self.request("POST", prefix + "/uploads", json={"total": size})
        digest = hashlib.sha256()
        offset = 0
        with archive.open("rb") as source:
            while data := source.read(UPLOAD_CHUNK):
                self.budget.checkpoint()
                digest.update(data)
                await self.request(
                    "PUT",
                    f"{prefix}/uploads/{upload['id']}/chunks",
                    params={
                        "offset": offset,
                        "sha256": hashlib.sha256(data).hexdigest(),
                    },
                    content=data,
                )
                offset += len(data)
        await self.request(
            "POST",
            f"{prefix}/uploads/{upload['id']}/seal",
            json={"sha256": digest.hexdigest()},
        )
        preflight = await self.request(
            "POST",
            prefix + "/preflight",
            json={"upload_id": upload["id"], "options": options, "private": private},
        )
        return await self.request(
            "POST",
            f"{prefix}/preflights/{preflight['id']}/confirm",
            json={"private": private, "replacement_confirmed": True},
        )

    async def export(
        self, destination: Path, *, categories, dependencies, password=None
    ):
        prefix = "/zhenxun/api/migration"
        if destination.exists():
            raise MigrationError("migration_destination_exists")
        job = await self.request(
            "POST",
            prefix + "/export",
            json={
                "confirm_secrets": True,
                "categories": sorted(categories),
                "dependencies": dependencies,
                "password": password,
            },
        )
        while job["stage"] not in {
            "completed",
            "failed",
            "cancelled",
            "recovery_required",
        }:
            self.budget.checkpoint()
            await asyncio.sleep(0.5)
            job = await self.request("GET", f"{prefix}/tasks/{job['id']}")
        if job["stage"] != "completed":
            raise MigrationError(job.get("first_error") or "migration_export_failed")
        require_space(destination.parent, job["progress"]["size"])
        digest = hashlib.sha256()
        with destination.open("xb") as target:
            async with self.client.stream(
                "GET", f"{prefix}/tasks/{job['id']}/download", timeout=60
            ) as response:
                if response.status_code != 200:
                    raise MigrationError("migration_download_failed")
                total = 0
                async for data in response.aiter_bytes(1024 * 1024):
                    self.budget.checkpoint()
                    total += len(data)
                    if total > min(Limits().compressed, job["progress"]["size"]):
                        raise MigrationError("migration_archive_size_limit")
                    digest.update(data)
                    target.write(data)
        if (
            total != job["progress"]["size"]
            or digest.hexdigest() != job["progress"]["sha256"]
        ):
            raise MigrationError("migration_archive_hash_mismatch")
        return job
