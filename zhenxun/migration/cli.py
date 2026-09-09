from __future__ import annotations

import argparse
import asyncio
import getpass
import json
from pathlib import Path
import sys

from .discovery import CATEGORIES
from .errors import MigrationError
from .service import capabilities, discover_packages, inspect_package
from .snapshot import ExportOptions, export_offline
from .tasks import TaskStore


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        # argparse's default error echoes unknown arguments, including values
        # accidentally supplied as passwords. Never reproduce them here.
        self.exit(2, '{"error":{"code":"migration_cli_arguments_invalid"}}\n')


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(prog="zx migration")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("capabilities")
    commands.add_parser("discover")
    commands.add_parser(
        "authorize", help="Print a one-time first-deployment migration grant"
    )
    export = commands.add_parser("export", help="Export a stopped instance")
    export.add_argument("destination", type=Path)
    export.add_argument("--category", action="append", choices=sorted(CATEGORIES))
    export.add_argument("--confirm-secrets", action="store_true")
    export.add_argument("--non-interactive", action="store_true")
    export.add_argument("--without-dependencies", action="store_true")
    export.add_argument("--encrypt", action="store_true")
    export.add_argument("--password-stdin", action="store_true")
    inspect = commands.add_parser("inspect", help="Verify a .zx without extracting")
    inspect.add_argument("archive", type=Path)
    inspect.add_argument("--encrypted", action="store_true")
    inspect.add_argument("--password-stdin", action="store_true")
    inspect.add_argument("--offset", type=int, default=0)
    inspect.add_argument("--limit", type=int, default=100)
    status = commands.add_parser("status")
    status.add_argument("task_id", nargs="?")
    cancel = commands.add_parser("cancel")
    cancel.add_argument("task_id")
    restore = commands.add_parser(
        "restore", help="Restore a stopped instance with initialization validation"
    )
    restore.add_argument("archive", type=Path)
    mode = restore.add_mutually_exclusive_group(required=True)
    mode.add_argument("--first-deployment", action="store_true")
    mode.add_argument("--replace", action="store_true")
    restore.add_argument("--trust-source", action="store_true")
    restore.add_argument("--confirm-replacement", action="store_true")
    restore.add_argument("--database-engine", choices=("sqlite", "mysql", "postgres"))
    restore.add_argument("--database-source-path")
    restore.add_argument("--database-target-path")
    restore.add_argument("--confirm-database-name")
    restore.add_argument("--private-stdin", action="store_true")
    restore.add_argument("--non-interactive", action="store_true")
    for command in (export, restore, status, cancel):
        command.add_argument(
            "--api-origin",
            help="Existing instance management origin for online launcher delegation",
        )
    for command in (export, status, cancel):
        command.add_argument("--private-stdin", action="store_true")
    return parser


def _private_input() -> dict:
    raw = sys.stdin.buffer.read(64 * 1024 + 1)
    if len(raw) > 64 * 1024:
        raise MigrationError("migration_private_input_limit")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise MigrationError("migration_private_input_invalid")
    return value


async def _remote(options, *, private=None, mapping=None):
    from .remote import OnlineMigration

    private = (
        private
        if private is not None
        else _private_input()
        if options.private_stdin
        else {}
    )
    management = private.pop("management", None)
    if management is None:
        if not sys.stdin.isatty() or getattr(options, "non_interactive", False):
            raise MigrationError("migration_management_credentials_required")
        management = {
            "username": input("Current administrator username: ").strip(),
            "password": getpass.getpass("Current administrator password: "),
        }
    async with OnlineMigration(options.api_origin, management) as remote:
        if options.command == "restore":
            return await remote.restore(
                options.archive,
                options={
                    "first_deployment": options.first_deployment,
                    "source_trusted": True,
                    "database": mapping,
                },
                private=private,
            )
        if options.command == "export":
            password = private.get("archive_password")
            if options.encrypt and not password:
                password = _password(stdin=options.password_stdin, prompt=True).decode()
            return await remote.export(
                options.destination.absolute(),
                categories=options.category or CATEGORIES,
                dependencies=not options.without_dependencies,
                password=password,
            )
        prefix = "/zhenxun/api/migration/tasks"
        if options.command == "cancel":
            return await remote.request("POST", f"{prefix}/{options.task_id}/cancel")
        return await remote.request(
            "GET", f"{prefix}/{options.task_id}" if options.task_id else prefix
        )


def _password(*, stdin: bool, prompt: bool) -> bytes | None:
    if stdin:
        value = sys.stdin.buffer.readline(4097)
        if len(value) > 4096:
            raise MigrationError("migration_password_limit")
        value = value.rstrip(b"\r\n")
        if not value:
            raise MigrationError("migration_password_required")
        return value
    if prompt:
        if not sys.stdin.isatty():
            raise MigrationError("migration_password_stdin_required")
        value = getpass.getpass("Migration archive password: ").encode()
        if not value or len(value) > 4096:
            raise MigrationError("migration_password_invalid")
        return value
    return None


def main(args: list[str], *, project: Path) -> int:
    options = _parser().parse_args(args)
    try:
        store = TaskStore(project)
        if options.command == "capabilities":
            result = capabilities()
        elif options.command == "authorize":
            from .bootstrap import issue_console_grant

            result = {"code": issue_console_grant(project), "expires_in": 900}
        elif options.command == "discover":
            result = {"items": discover_packages(project)}
        elif options.command == "inspect":
            result = inspect_package(
                options.archive.absolute(),
                password=_password(
                    stdin=options.password_stdin, prompt=options.encrypted
                ),
                offset=options.offset,
                limit=options.limit,
            )
        elif options.command == "export":
            if options.destination.suffix.casefold() != ".zx":
                raise MigrationError("migration_archive_extension_invalid")
            confirmed = options.confirm_secrets
            if not confirmed:
                if (
                    options.non_interactive
                    or not sys.stdin.isatty()
                    or options.password_stdin
                ):
                    raise MigrationError(
                        "migration_sensitive_export_confirmation_required"
                    )
                confirmed = (
                    input(
                        "The archive includes credentials, databases and plugin code. "
                        "Export this stopped instance? [y/N] "
                    )
                    .strip()
                    .casefold()
                    == "y"
                )
            if not confirmed:
                raise MigrationError("migration_cancelled")
            result = (
                asyncio.run(_remote(options))
                if options.api_origin
                else export_offline(
                    project,
                    options.destination.absolute(),
                    options=ExportOptions(
                        categories=frozenset(options.category or CATEGORIES),
                        plaintext_confirmed=confirmed,
                        dependencies=not options.without_dependencies,
                    ),
                    password=_password(
                        stdin=options.password_stdin, prompt=options.encrypt
                    ),
                )
            )
        elif options.command == "status":
            result = (
                asyncio.run(_remote(options))
                if options.api_origin
                else (
                    store.public(store.read("jobs", options.task_id))
                    if options.task_id
                    else store.list_jobs()
                )
            )
        elif options.command == "cancel":
            result = (
                asyncio.run(_remote(options))
                if options.api_origin
                else store.public(store.request_cancel(options.task_id))
            )
        elif options.command == "restore":
            import secrets

            from .application import MigrationApplication

            confirmed = options.trust_source and options.confirm_replacement
            if (
                not confirmed
                and not options.non_interactive
                and sys.stdin.isatty()
                and not options.private_stdin
            ):
                confirmed = (
                    input(
                        "Trust this package and replace its selected directories? "
                        "Plugin initialization may have irreversible external "
                        "effects. [y/N] "
                    )
                    .strip()
                    .casefold()
                    == "y"
                )
            if not confirmed:
                raise MigrationError("migration_restore_confirmation_required")
            if options.private_stdin:
                private = _private_input()
            else:
                if options.non_interactive or not sys.stdin.isatty():
                    raise MigrationError("migration_private_stdin_required")
                private = {
                    "configuration": {
                        "HOST": input("Confirmed listening address: ").strip(),
                        "PORT": input("Confirmed listening port: ").strip(),
                        "DB_URL": getpass.getpass("Confirmed target database URL: "),
                    }
                }
                private["administrator"] = {
                    "username": input("Confirmed administrator username: ").strip(),
                    "password": getpass.getpass("Confirmed administrator password: "),
                }
                if options.database_engine in {"mysql", "postgres"}:
                    private["database"] = {
                        "target_url": private["configuration"]["DB_URL"],
                        "candidate_url": getpass.getpass(
                            "Empty candidate database URL "
                            "(separate restricted account): "
                        ),
                    }
            mapping = None
            if options.database_engine:
                mapping = {
                    "engine": options.database_engine,
                    "source_path": options.database_source_path,
                    "confirmed_name": options.confirm_database_name,
                }
                if options.database_engine == "sqlite":
                    mapping["target_path"] = options.database_target_path
                if not all(mapping.values()):
                    raise MigrationError("migration_database_confirmation_required")
            result = asyncio.run(
                _remote(options, private=private, mapping=mapping)
                if options.api_origin
                else MigrationApplication(project).restore_offline(
                    secrets.token_urlsafe(32),
                    options.archive,
                    options={
                        "first_deployment": options.first_deployment,
                        "source_trusted": True,
                        "database": mapping,
                    },
                    private=private,
                )
            )
        else:
            raise MigrationError(
                "migration_maintenance_validation_unavailable", status=409
            )
        sys.stdout.write(json.dumps(result, ensure_ascii=True) + "\n")
        return 0
    except MigrationError as error:
        sys.stderr.write(
            json.dumps({"error": error.public()}, ensure_ascii=True) + "\n"
        )
        return 1
    except (OSError, ValueError, KeyboardInterrupt, EOFError):
        sys.stderr.write('{"error":{"code":"migration_cli_operation_failed"}}\n')
        return 1
