from contextvars import ContextVar

CURRENT_PLATFORM_SCOPE: ContextVar[str] = ContextVar(
    "zhenxun_platform_scope", default=""
)

LEGACY_IDENTITY_TABLES = frozenset(
    {
        "ban_console",
        "group_console",
        "level_users",
        "sign_users",
        "user_console",
        "user_gold_log",
        "user_props",
        "user_props_log",
    }
)


class UnsafeLegacyIdentityWrite(RuntimeError):
    pass


def guard_legacy_identity_write(table: str) -> None:
    if CURRENT_PLATFORM_SCOPE.get() == "qq_api" and table in LEGACY_IDENTITY_TABLES:
        raise UnsafeLegacyIdentityWrite(
            f"QQ official identity cannot write legacy table {table}"
        )


__all__ = [
    "CURRENT_PLATFORM_SCOPE",
    "LEGACY_IDENTITY_TABLES",
    "UnsafeLegacyIdentityWrite",
    "guard_legacy_identity_write",
]
