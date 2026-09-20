"""Compatibility alias for the relocated ORM migration module."""

from zhenxun._compat import alias_module

_implementation = alias_module(__name__, "zhenxun.services.nonebot_store.orm_migration")

if __name__ == "__main__":
    raise SystemExit(_implementation.main())
