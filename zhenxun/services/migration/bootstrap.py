from __future__ import annotations

from pathlib import Path

from zhenxun.utils.passwords import is_password_hash
from zhenxun.utils.webui_session import AuthenticatedSession

from .errors import MigrationError
from .maintenance_app import ManagementSnapshot


class BootstrapAuthority:
    """Bind the existing operator session to first-deployment restoration."""

    def __init__(self, project: Path, *, first_deployment):
        self.project = project
        self.first_deployment = first_deployment

    def validate(self, session):
        if (
            not isinstance(session, AuthenticatedSession)
            or not session.first_deployment
        ):
            raise MigrationError("migration_setup_authorization_required", status=403)

    def management(self, session):
        self.validate(session)
        administrator = session.administrator
        if not administrator:
            raise MigrationError("migration_administrator_confirmation_required")
        return ManagementSnapshot(
            administrator["username"], administrator["password"], session.secret
        )

    def set_administrator(self, session, administrator):
        self.validate(session)
        if not is_password_hash(administrator["password"]):
            raise MigrationError("migration_administrator_confirmation_required")
        session.administrator = dict(administrator)


_authorities = {}


def bootstrap_authority(project: Path) -> BootstrapAuthority:
    key = str(project.absolute())
    if key not in _authorities:

        def first():
            from zhenxun.builtin_plugins.web_ui.api.configure.setup_access import (
                setup_access,
            )
            from zhenxun.services.startup import startup_coordinator

            return startup_coordinator.snapshot().get(
                "operating_mode"
            ) == "setup_only" and setup_access.state() not in {
                "configured",
                "restart_pending",
            }

        _authorities[key] = BootstrapAuthority(project, first_deployment=first)
    return _authorities[key]
