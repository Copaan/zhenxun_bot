"""Small helpers for keeping relocated modules import-compatible."""

from collections.abc import Iterable
from importlib import import_module
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec
import sys
from types import ModuleType


def alias_module(legacy_name: str, implementation_name: str) -> ModuleType:
    """Map a legacy module name to one canonical module object."""

    implementation = import_module(implementation_name)
    sys.modules[legacy_name] = implementation
    parent_name, _, attribute = legacy_name.rpartition(".")
    parent = sys.modules.get(parent_name)
    if parent is not None:
        setattr(parent, attribute, implementation)
    return implementation


class _AliasLoader(Loader):
    """Load a legacy name from one canonical module."""

    def __init__(self, implementation_name: str, entrypoint: str | None) -> None:
        self._implementation_name = implementation_name
        self._entrypoint = entrypoint

    def create_module(self, _spec: ModuleSpec) -> ModuleType:
        return import_module(self._implementation_name)

    def exec_module(self, _module: ModuleType) -> None:
        return None

    def get_code(self, _fullname: str):
        if self._entrypoint is None:
            return None
        source = (
            f"from {self._implementation_name} import {self._entrypoint}\n"
            f"raise SystemExit({self._entrypoint}())\n"
        )
        return compile(source, f"<{self._implementation_name} -m>", "exec")


class _AliasFinder(MetaPathFinder):
    def __init__(self) -> None:
        self.aliases: dict[str, tuple[str, str | None]] = {}

    def find_spec(
        self, fullname: str, _path: Iterable[str] | None, _target: ModuleType | None
    ) -> ModuleSpec | None:
        target = self.aliases.get(fullname)
        if target is None:
            return None
        implementation_name, entrypoint = target
        return ModuleSpec(
            fullname,
            _AliasLoader(implementation_name, entrypoint),
            origin=f"alias:{implementation_name}",
        )


_alias_finder = _AliasFinder()


def install_module_aliases(
    aliases: dict[str, str], *, entrypoints: dict[str, str] | None = None
) -> None:
    """Install lazy aliases without importing every relocated child module."""

    entrypoints = entrypoints or {}
    _alias_finder.aliases.update(
        {
            legacy_name: (implementation_name, entrypoints.get(legacy_name))
            for legacy_name, implementation_name in aliases.items()
        }
    )
    if _alias_finder not in sys.meta_path:
        sys.meta_path.insert(0, _alias_finder)
