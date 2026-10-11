"""Effective MySQL grants for the objects used by native migration tools."""

from __future__ import annotations

from dataclasses import dataclass, field
import re

from .errors import MigrationError

CAPABILITY_POLICY_VERSION = 3
MYSQL_RESTORE_PRIVILEGES = {
    "CREATE",
    "ALTER",
    "DROP",
    "INDEX",
    "INSERT",
    "UPDATE",
    "DELETE",
    "REFERENCES",
    "SELECT",
    "LOCK TABLES",
}
_IDENTIFIER = r"(?:`(?:``|[^`])*`|\*)"
_ACCOUNT = r"`(?:``|[^`])*`@`(?:``|[^`])*`"
_SCOPE = re.compile(rf"^({_IDENTIFIER})\.({_IDENTIFIER})$")
_GRANT = re.compile(r"^(GRANT|REVOKE) (.+?) ON (.+?) (?:TO|FROM) .+$")


def _identifier(value):
    return value[1:-1].replace("``", "`") if value.startswith("`") else value


def _privileges(value):
    # Column grants do not confer whole-table access. Split outside column lists.
    items = re.split(r",\s*(?![^()]*\))", value)
    result = set()
    for item in items:
        if "(" in item:
            if not re.fullmatch(r"[A-Z ]+\s*\((?:`(?:``|[^`])*`\s*,?\s*)+\)", item):
                raise MigrationError("migration_database_capability_unconfirmed")
            continue
        if not re.fullmatch(r"[A-Z_ ]+", item):
            raise MigrationError("migration_database_capability_unconfirmed")
        result.add(item)
    if "ALL PRIVILEGES" in result:
        result |= MYSQL_RESTORE_PRIVILEGES | {
            "PROCESS",
            "TRIGGER",
            "EVENT",
            "CREATE ROUTINE",
            "ALTER ROUTINE",
            "EXECUTE",
            "CREATE VIEW",
            "SHOW VIEW",
            "RELOAD",
        }
    return result


def scope_matches(pattern, database, *, literal, ignore_case=False):
    """Match MySQL database grants, respecting partial_revokes and escaping."""
    if literal:
        return (
            pattern.casefold() == database.casefold()
            if ignore_case
            else pattern == database
        )
    parts = []
    escaped = False
    for character in pattern:
        if escaped:
            parts.append(re.escape(character))
            escaped = False
        elif character == "\\":
            escaped = True
        else:
            parts.append({"%": ".*", "_": "."}.get(character, re.escape(character)))
    if escaped:
        raise MigrationError("migration_database_capability_unconfirmed")
    return (
        re.fullmatch("".join(parts), database, re.IGNORECASE if ignore_case else 0)
        is not None
    )


@dataclass
class MySQLCapabilities:
    global_privileges: set[str] = field(default_factory=set)
    database_privileges: set[str] = field(default_factory=set)
    table_privileges: dict[str, set[str]] = field(default_factory=dict)
    routine_privileges: dict[str, set[str]] = field(default_factory=dict)
    revoked: set[str] = field(default_factory=set)
    sources: set[str] = field(default_factory=set)
    ignore_case: bool = False

    @classmethod
    def parse(cls, text, database, *, literal, ignore_case=False):
        result = cls(ignore_case=ignore_case)
        if not text.strip():
            raise MigrationError("migration_database_capability_unconfirmed")
        for line in text.splitlines():
            match = _GRANT.fullmatch(line)
            if match is None:
                # SHOW GRANTS also reports role membership; enabled roles are
                # expanded by SHOW GRANTS USING, not inferred from membership.
                if re.fullmatch(
                    rf"GRANT {_ACCOUNT}(?:,\s*{_ACCOUNT})* TO .+",
                    line,
                ):
                    continue
                raise MigrationError("migration_database_capability_unconfirmed")
            action, privileges, scope = match.groups()
            if action == "GRANT" and scope.startswith(("PROCEDURE ", "FUNCTION ")):
                kind, object_scope = scope.split(" ", 1)
                routine = _SCOPE.fullmatch(object_scope)
                if routine and _privileges(privileges) <= {
                    "EXECUTE",
                    "ALTER ROUTINE",
                    "GRANT OPTION",
                }:
                    db, name = map(_identifier, routine.groups())
                    if scope_matches(
                        db, database, literal=True, ignore_case=ignore_case
                    ):
                        key = kind + ":" + name.casefold()
                        result.routine_privileges.setdefault(key, set()).update(
                            _privileges(privileges)
                        )
                        result.sources.add("routine")
                    continue
                raise MigrationError("migration_database_capability_unconfirmed")
            if (
                action == "GRANT"
                and privileges == "PROXY"
                and re.fullmatch(_ACCOUNT, scope)
            ):
                # Proxy delegation does not grant object privileges to CURRENT_USER.
                continue
            parsed_scope = _SCOPE.fullmatch(scope)
            if parsed_scope is None:
                raise MigrationError("migration_database_capability_unconfirmed")
            db_token, table_token = parsed_scope.groups()
            db, table = map(_identifier, (db_token, table_token))
            values = _privileges(privileges)
            if action == "REVOKE":
                if not literal or table_token != "*" or db_token == "*":
                    raise MigrationError("migration_database_capability_unconfirmed")
                if scope_matches(db, database, literal=True, ignore_case=ignore_case):
                    result.revoked |= values
                    result.sources.add("partial_revoke")
            elif db_token == "*" and table_token == "*":
                result.global_privileges |= values
                result.sources.add("global")
            elif table_token == "*":
                if scope_matches(
                    db, database, literal=literal, ignore_case=ignore_case
                ):
                    result.database_privileges |= values
                    result.sources.add("database")
            elif scope_matches(db, database, literal=True, ignore_case=ignore_case):
                key = table.casefold() if ignore_case else table
                result.table_privileges.setdefault(key, set()).update(values)
                result.sources.add("table")
        return result

    def effective(self, table=None):
        values = (self.global_privileges - self.revoked) | self.database_privileges
        if table is not None:
            values |= self.table_privileges.get(
                table.casefold() if self.ignore_case else table, set()
            )
        return values - {"ALL PRIVILEGES", "USAGE"}

    def effective_routine(self, kind, name):
        return self.effective() | self.routine_privileges.get(
            kind.upper() + ":" + name.casefold(), set()
        )

    @property
    def inventory_visible(self):
        return bool(self.effective() & MYSQL_RESTORE_PRIVILEGES)
