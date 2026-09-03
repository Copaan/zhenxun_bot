from __future__ import annotations

from typing import Any

from .providers import RuntimeProviderSnapshot, capture_runtime_providers


class HookKernel:
    """Structural hook entrypoint installed immediately after NoneBot init."""

    def __init__(self) -> None:
        self.installed = False

    def install(self, runtime_manager: Any) -> None:
        runtime_manager.install()
        self.installed = bool(runtime_manager.enabled)

    def uninstall(self, runtime_manager: Any) -> None:
        runtime_manager.uninstall()
        self.installed = False

    def snapshot(self, driver: Any | None = None) -> RuntimeProviderSnapshot:
        return capture_runtime_providers(driver)


hook_kernel = HookKernel()

__all__ = ["HookKernel", "hook_kernel"]
