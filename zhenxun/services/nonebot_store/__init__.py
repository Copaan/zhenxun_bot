"""Managed NoneBot Registry services.

Importing this package may load the service implementation and its data helpers, but it
must not initialize NoneBot, open a database connection, start a scheduler, or load a
library plugin. The launcher relies on those side-effect boundaries before
``nonebot.init()``.
"""

from .runtime import activate_current_generation, load_managed_plugins

__all__ = ["activate_current_generation", "load_managed_plugins"]
