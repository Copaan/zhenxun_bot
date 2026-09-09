from __future__ import annotations

import hashlib
import hmac
from pathlib import Path
import secrets
from threading import RLock
import time

from jose import JWTError, jwt

from zhenxun.utils.atomic_json import (
    mutate_json_locked,
    read_json_locked,
    write_json_locked,
)
from zhenxun.utils.passwords import hash_password

from .access import private_directory
from .errors import MigrationError
from .maintenance_app import ManagementSnapshot
from .paths import contained_path
from .tasks import TaskStore


def issue_console_grant(project: Path) -> str:
    """Local-console authorization, independent of initial configuration claims."""
    from .control import request_control

    state = request_control(project, "capabilities", {})
    if not state.get("launcher_connected"):
        raise MigrationError("migration_launcher_unavailable")
    directory = private_directory(contained_path(project, "migration/control"))
    owner = read_json_locked(directory / "owner.json", {})
    if owner.get("role") != "launcher" or not owner.get("identity"):
        raise MigrationError("migration_launcher_unavailable")
    code = secrets.token_urlsafe(32)
    write_json_locked(
        directory / "bootstrap-grant.json",
        {
            "launcher": owner["identity"],
            "sha256": hashlib.sha256(code.encode()).hexdigest(),
            "expires_at": time.time() + 900,
            "consumed": False,
        },
    )
    return code


class BootstrapAuthority:
    def __init__(self, project: Path, *, first_deployment):
        self.project = project
        self.first_deployment = first_deployment
        self.snapshot = None
        self.session = None
        self.launcher = None
        self.lock = RLock()

    def exchange(self, code: str) -> dict:
        with self.lock:
            if not self.first_deployment():
                raise MigrationError(
                    "migration_first_deployment_unavailable", status=409
                )
            if not isinstance(code, str) or not 32 <= len(code) <= 128:
                raise MigrationError(
                    "migration_console_authorization_invalid", status=403
                )
            directory = contained_path(self.project, "migration/control")
            owner = read_json_locked(directory / "owner.json", {})
            now = time.time()

            def consume(value):
                if (
                    not value
                    or value.get("consumed")
                    or value.get("expires_at", 0) <= now
                    or value.get("launcher") != owner.get("identity")
                    or owner.get("role") != "launcher"
                    or not hmac.compare_digest(
                        value.get("sha256", ""),
                        hashlib.sha256(code.encode()).hexdigest(),
                    )
                ):
                    raise MigrationError(
                        "migration_console_authorization_invalid", status=403
                    )
                value["consumed"] = True
                return value

            mutate_json_locked(directory / "bootstrap-grant.json", {}, consume)
            self.snapshot = ManagementSnapshot(
                "migration-console", hash_password(code), secrets.token_urlsafe(48)
            )
            self.session = secrets.token_urlsafe(32)
            self.launcher = owner["identity"]
            token = jwt.encode(
                {
                    "sub": self.snapshot.username,
                    "sid": self.session,
                    "scope": "migration",
                    "iat": int(now),
                    "exp": int(now) + 21600,
                },
                self.snapshot.secret,
                algorithm="HS256",
            )
            return {"access_token": token, "token_type": "bearer", "expires_in": 21600}

    def authenticate(self, token: str) -> str:
        with self.lock:
            try:
                if (
                    self.snapshot is None
                    or not self.first_deployment()
                    or len(token) > 16384
                ):
                    raise ValueError
                claims = jwt.decode(
                    token.removeprefix("Bearer ").removeprefix("bearer "),
                    self.snapshot.secret,
                    algorithms=["HS256"],
                    options={"require_exp": True},
                )
                owner = read_json_locked(
                    contained_path(self.project, "migration/control/owner.json"), {}
                )
                if (
                    claims.get("scope") != "migration"
                    or claims.get("sid") != self.session
                    or owner.get("identity") != self.launcher
                ):
                    raise ValueError
                job = TaskStore(self.project).active()
                if job and job.get("committed_at"):
                    raise ValueError
                return self.session
            except (JWTError, ValueError, TypeError):
                raise MigrationError("migration_login_required", status=401) from None

    def management(self, session):
        with self.lock:
            if (
                not self.snapshot
                or not self.session
                or not hmac.compare_digest(session, self.session)
                or not self.first_deployment()
            ):
                raise MigrationError(
                    "migration_console_authorization_required", status=403
                )
            return self.snapshot


_authorities = {}


def bootstrap_authority(project: Path) -> BootstrapAuthority:
    key = str(project.absolute())
    if key not in _authorities:

        def first():
            from zhenxun.services.startup import startup_coordinator

            return startup_coordinator.snapshot().get("operating_mode") == "setup_only"

        _authorities[key] = BootstrapAuthority(project, first_deployment=first)
    return _authorities[key]
