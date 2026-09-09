from __future__ import annotations

import os
from pathlib import Path
import re
import sys
import traceback

import psutil

from zhenxun.utils.atomic_json import write_json_locked

from .control import request_control
from .errors import MigrationError
from .lease import DelegatedLease
from .phases import execute_phase
from .tasks import TaskStore


def main(args: list[str]) -> int:
    request = None
    result_path = None
    try:
        if len(args) != 1 or not re.fullmatch(r"[a-f0-9]{32}", args[0]):
            raise MigrationError("migration_phase_identity_invalid")
        project = Path.cwd()
        launcher = os.environ.get("ZHENXUN_LAUNCHER_PID", "")
        identity = os.environ.get("ZHENXUN_INSTANCE_LEASE_ID", "")
        if not launcher.isdecimal():
            raise MigrationError("migration_launcher_identity_mismatch")
        lease = DelegatedLease(project, int(launcher), identity)
        request = request_control(project, "phase_input", {"invocation_id": args[0]})
        if request["invocation_id"] != args[0] or request[
            "launcher_boot_id"
        ] != os.environ.get("ZHENXUN_LAUNCHER_BOOT_ID"):
            raise MigrationError("migration_phase_identity_mismatch")
        lease.management_peers = tuple(request.get("management_peers", ()))
        result_path = (
            TaskStore(project).path("jobs", request["job_id"]).parent
            / "phases"
            / args[0]
            / "result.json"
        )
        receipt = {
            key: request[key]
            for key in (
                "job_id",
                "phase",
                "invocation_id",
                "launcher_boot_id",
                "job_revision",
            )
        }
        receipt.update(pid=os.getpid(), created_at=psutil.Process().create_time())
        try:
            result = execute_phase(project, request, lease=lease)
        except BaseException as error:
            code = (
                error.code
                if isinstance(error, MigrationError)
                else "migration_phase_failed"
            )
            write_json_locked(
                result_path,
                {
                    **receipt,
                    "state": "failed",
                    "error_code": code,
                    "error_type": type(error).__name__,
                    "failure_frames": [
                        {
                            "file": Path(frame.filename).name,
                            "line": frame.lineno,
                            "function": frame.name,
                        }
                        for frame in traceback.extract_tb(error.__traceback__)[-6:]
                    ],
                },
            )
            return 1
        write_json_locked(
            result_path, {**receipt, "state": "completed", "result": result}
        )
        return 0
    except BaseException:
        # No raw request, exception or credential is written to subprocess output.
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
