"""Managed NoneBot Registry plugin support.

This package intentionally has no NoneBot or service-package imports so the launcher can
use the generation and transaction helpers before ``nonebot.init()``.
"""

from .runtime import activate_current_generation, load_managed_plugins

__all__ = ["activate_current_generation", "load_managed_plugins"]
