"""Compatibility alias for the relocated archive worker service."""

from zhenxun._compat import alias_module

_implementation = alias_module(
    __name__, "zhenxun.services.plugin_store.plugin_archive_worker"
)

if __name__ == "__main__":
    _implementation.main()
