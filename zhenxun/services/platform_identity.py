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


NAMESPACED_TABLES = frozenset({"ban_console", "group_console", "level_users"})


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


def is_isolated_scope(scope: str) -> bool:
    """判断平台作用域是否需要与 QQ 号身份隔离存储

    参数:
        scope: 平台作用域，空字符串表示无事件上下文

    返回:
        bool: 除 qq_client 与无上下文外均为 True
    """
    return bool(scope) and scope != "qq_client"


def is_namespaced_scope(scope: str) -> bool:
    """判断平台作用域是否使用 `作用域:id` 前缀存储封禁、群组与管理员等级

    参数:
        scope: 平台作用域

    返回:
        bool: 除 qq_client、qq_api 与无上下文外均为 True
    """
    return is_isolated_scope(scope) and scope != "qq_api"


def _scoped(value, scope: str | None, infix: str):
    scope = CURRENT_PLATFORM_SCOPE.get() if scope is None else scope
    if not value or not is_namespaced_scope(scope):
        return value
    value = str(value)
    return value if value.startswith(f"{scope}:") else f"{scope}:{infix}{value}"


def scoped_user_id(user_id, scope: str | None = None):
    """转换为平台作用域下持久化使用的用户 id

    参数:
        user_id: 适配器中的用户 id，已带前缀时原样返回
        scope: 平台作用域，默认取当前事件的作用域

    返回:
        前缀作用域返回 `作用域:id`，其余原样返回
    """
    return _scoped(user_id, scope, "")


def scoped_group_id(group_id, scope: str | None = None):
    """转换为平台作用域下持久化使用的群组 id

    参数:
        group_id: 适配器中的群组 id，已带前缀时原样返回
        scope: 平台作用域，默认取当前事件的作用域

    返回:
        前缀作用域返回 `作用域:group:id`，其余原样返回
    """
    return _scoped(group_id, scope, "group:")


def scope_owns(*ids, scope: str | None = None) -> bool:
    """判断记录 id 是否属于当前平台作用域

    参数:
        ids: 记录中的用户/群组 id
        scope: 平台作用域，默认取当前事件的作用域

    返回:
        bool: 非前缀作用域恒为 True；前缀作用域要求非空 id 均带本作用域前缀
    """
    scope = CURRENT_PLATFORM_SCOPE.get() if scope is None else scope
    if not is_namespaced_scope(scope):
        return True
    keys = [str(i) for i in ids if i]
    return bool(keys) and all(key.startswith(f"{scope}:") for key in keys)


@contextmanager
def business_write_scope(keys):
    """Internal provisioning capability, restricted to explicit business keys."""
    token = _business_keys.set(frozenset(map(str, keys)))
    try:
        yield
    finally:
        _business_keys.reset(token)


def guard_legacy_identity_write(table: str, user_ids=None, group_ids=None) -> None:
    scope = CURRENT_PLATFORM_SCOPE.get()
    if is_isolated_scope(scope) and table in LEGACY_IDENTITY_TABLES:
        if (
            table in NAMESPACED_TABLES
            and is_namespaced_scope(scope)
            and (user_ids or group_ids)
            and scope_owns(*(user_ids or ()), *(group_ids or ()), scope=scope)
        ):
            return
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
            f"Platform scope {scope} cannot write legacy table {table}"
        )


__all__ = [
    "CURRENT_PLATFORM_SCOPE",
    "LEGACY_IDENTITY_TABLES",
    "NAMESPACED_TABLES",
    "UnsafeLegacyIdentityWrite",
    "guard_legacy_identity_write",
    "is_isolated_scope",
    "is_namespaced_scope",
    "scope_owns",
    "scoped_group_id",
    "scoped_user_id",
]
