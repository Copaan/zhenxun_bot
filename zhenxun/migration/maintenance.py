from __future__ import annotations

import asyncio
from dataclasses import asdict
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from zhenxun.services.lifecycle.diagnostics import merge_terminal_receipt
from zhenxun.utils.atomic_json import read_json_locked
from zhenxun.utils.network import internal_connect_host

from .errors import MigrationError
from .maintenance_app import ManagementSnapshot


class MaintenanceProcess:
    def __init__(self, service, job_id: str, snapshot: ManagementSnapshot):
        self.service, self.job_id = service, job_id
        self._snapshot = snapshot
        self.process = None
        self.startup_id = uuid.uuid4().hex
        self._network = None
        self._deadline = 0.0
        self._keep_available = False

    def input_for(self, payload: dict, *, peer_pid: int) -> dict:
        self.service.lease.require_held()
        process = self.process
        handle = self.service.supervisor._handles.get(process.pid) if process else None
        if (
            handle is None
            or process.poll() is not None
            or payload.get("startup_id") != self.startup_id
            or peer_pid
            not in {item.pid for item in handle._live_processes(discover=True)}
            or time.monotonic() >= self._deadline
        ):
            raise MigrationError("migration_management_identity_unconfirmed")
        return {
            "job_id": self.job_id,
            "startup_id": self.startup_id,
            "launcher_boot_id": self.service.supervisor.boot_id,
            "management": asdict(self._snapshot),
            "network": asdict(self._network),
            "remaining_seconds": self._deadline - time.monotonic(),
            "keep_available": self._keep_available,
        }

    async def start(self, network, *, budget, keep_available=False):
        from zhenxun.configs.webui_tls import certificate_sha256
        from zhenxun.services.listener_health import health_response

        self._network = network
        self._deadline = budget.deadline
        self._keep_available = keep_available
        self.service._maintenance = self

        def spawn():
            return subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "zhenxun.migration.maintenance_worker",
                    self.startup_id,
                ],
                cwd=self.service.lease.project,
                env={
                    **os.environ,
                    "PYTHONPATH": os.pathsep.join(
                        filter(
                            None,
                            (
                                str(Path(__file__).resolve().parents[2]),
                                os.getenv("PYTHONPATH"),
                            ),
                        )
                    ),
                    "ZHENXUN_LAUNCHER_PID": str(os.getpid()),
                    "ZHENXUN_LAUNCHER_BOOT_ID": self.service.supervisor.boot_id,
                    "ZHENXUN_INSTANCE_LEASE_ID": self.service.lease.identity,
                },
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                if os.name == "nt"
                else 0,
            )

        self.process = await self.service.supervisor.start_process(
            "migration_management", spawn, startup_id=self.startup_id
        )
        host = internal_connect_host(network.host)
        host = f"[{host}]" if ":" in host else host
        address = (
            f"{network.scheme}://{host}:{network.port}/zhenxun/api/configure/status"
        )
        pin = certificate_sha256(network.certfile) if network.enabled else None

        def read():
            try:
                with health_response(address, fingerprint=pin, timeout=1) as response:
                    return json.loads(response.read(64 * 1024)).get("data", {})
            except (OSError, ValueError):
                return None

        ready_deadline = min(budget.deadline, time.monotonic() + 30)
        try:
            while time.monotonic() < ready_deadline:
                budget.checkpoint()
                if self.process.poll() is not None:
                    raise MigrationError("migration_management_exited")
                if self.service.supervisor.shutdown_deadline is not None:
                    raise MigrationError("migration_launcher_stopping")
                state = await asyncio.to_thread(read)
                if state and (
                    state.get("startup_id") == self.startup_id
                    and state.get("launcher_boot_id") == self.service.supervisor.boot_id
                    and state.get("task_id") == self.job_id
                    and state.get("operating_mode") == "maintenance"
                ):
                    handle = self.service.supervisor._handles[self.process.pid]
                    if handle.bind_runtime(
                        runtime_pid=state.get("pid"),
                        worker_boot_id=state.get("boot_id"),
                        operating_mode="maintenance",
                    ):
                        return state
                await asyncio.sleep(0.05)
            raise MigrationError("migration_management_startup_timeout")
        except BaseException:
            await self.close()
            raise

    async def close(self):
        if self.process is None:
            return
        process, self.process = self.process, None
        handle = self.service.supervisor._handles.get(process.pid)
        await self.service.supervisor.stop_process(process)
        self.service._maintenance = None
        if (
            handle is None
            or handle._live_processes(discover=True)
            or any(
                item["stage"] in {"terminate", "kill"} for item in handle.stop_stages
            )
        ):
            raise MigrationError("migration_management_close_unconfirmed")
        state_path = (
            self.service.store.path("jobs", self.job_id).parent
            / f"management-{self.startup_id}.json"
        )
        state = read_json_locked(state_path, {})
        receipt = merge_terminal_receipt(state, state_path).get("terminal_shutdown", {})
        identity = receipt.get("identity", {})
        if (
            receipt.get("result") != "confirmed"
            or identity.get("startup_id") != self.startup_id
            or identity.get("launcher_boot_id") != self.service.supervisor.boot_id
            or identity.get("pid") != handle.runtime_pid
            or identity.get("boot_id") != handle.worker_boot_id
        ):
            raise MigrationError("migration_management_close_unconfirmed")
