"""Task-bound export admission for verified stopped processes."""

from .errors import MigrationError


def export_snapshot_mode(shutdown: dict, options: dict) -> str:
    """Admit clean shutdown or an explicitly authorized crash-consistent export."""
    if not shutdown.get("process_tree_released") or shutdown.get("unresolved_roles"):
        raise MigrationError("migration_shutdown_unconfirmed")
    if shutdown.get("result") == "confirmed" and not shutdown.get("forced"):
        return "clean_shutdown"
    identity = shutdown.get("identity") or {}
    if (
        options.get("allow_forced_shutdown") is not True
        or shutdown.get("process_identity_verified") is not True
        or not all(
            identity.get(key)
            for key in ("boot_id", "startup_id", "launcher_boot_id", "pid")
        )
    ):
        raise MigrationError("migration_shutdown_unconfirmed")
    return "forced_stop"
