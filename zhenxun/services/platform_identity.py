from contextlib import contextmanager
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
        "sign_log",
        "mahiro_bank",
        "mahiro_bank_log",
    }
)


class UnsafeLegacyIdentityWrite(RuntimeError):
    pass


_business_keys: ContextVar[frozenset[str]] = ContextVar(
    "business_write_keys", default=frozenset()
)
BUSINESS_TABLES = frozenset(
    {
        "user_console",
        "sign_users",
        "user_gold_log",
        "user_props",
        "user_props_log",
        "sign_log",
        "mahiro_bank",
        "mahiro_bank_log",
    }
)


@contextmanager
def business_write_scope(keys):
    """Internal provisioning capability, restricted to explicit business keys."""
    token = _business_keys.set(frozenset(map(str, keys)))
    try:
        yield
    finally:
        _business_keys.reset(token)


def guard_legacy_identity_write(table: str, user_ids=None) -> None:
    if CURRENT_PLATFORM_SCOPE.get() == "qq_api" and table in LEGACY_IDENTITY_TABLES:
        from zhenxun.services.business_identity import current_business_identity

        identity = current_business_identity()
        allowed = _business_keys.get()
        if identity is not None:
            from zhenxun.services.asset_transaction import locked_asset_keys

            allowed = allowed | (locked_asset_keys() & {identity.storage_key})
        if (
            table in BUSINESS_TABLES
            and user_ids
            and set(map(str, user_ids)).issubset(allowed)
        ):
            return
        raise UnsafeLegacyIdentityWrite(
            f"QQ official identity cannot write legacy table {table}"
        )


__all__ = [
    "CURRENT_PLATFORM_SCOPE",
    "LEGACY_IDENTITY_TABLES",
    "UnsafeLegacyIdentityWrite",
    "guard_legacy_identity_write",
]
