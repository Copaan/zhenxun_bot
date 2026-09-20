"""Compatibility alias for the relocated NoneBot store services."""

from zhenxun._compat import alias_module, install_module_aliases

alias_module(__name__, "zhenxun.services.nonebot_store")

install_module_aliases(
    {
        f"{__name__}.{name}": f"zhenxun.services.nonebot_store.{name}"
        for name in ("dependencies", "orm_migration", "registry", "runtime", "storage")
    },
    entrypoints={f"{__name__}.orm_migration": "main"},
)
