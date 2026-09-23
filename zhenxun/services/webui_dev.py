"""Launcher-owned Vue CLI development server."""

from __future__ import annotations

import asyncio
from functools import partial
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess

import aiohttp

PREFIX = "/__webui_dev__/"


def worker_endpoint() -> tuple[str, str] | None:
    """Read the private loopback endpoint handed to this worker by its launcher."""
    port = os.getenv("ZHENXUN_WEBUI_DEV_PORT")
    token = os.getenv("ZHENXUN_WEBUI_DEV_TOKEN")
    if not port or not token:
        return None
    if not port.isdecimal() or not 0 < int(port) < 65536:
        raise ValueError("Invalid launcher WebUI development port")
    return f"http://127.0.0.1:{port}", token


class WebUIDevServer:
    """Keep one supervised frontend process across ordinary worker restarts."""

    def __init__(self, source: Path, supervisor):
        self.source = source.resolve()
        self.supervisor = supervisor
        self.node = shutil.which("node")
        self.cli = self.source / "node_modules/@vue/cli-service/bin/vue-cli-service.js"
        if not self.node or not self.cli.is_file():
            raise RuntimeError(
                f"WebUI development requires Node and installed Vue CLI: {self.source}"
            )
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
        self.token = secrets.token_urlsafe(32)
        self.process: subprocess.Popen | None = None

    def worker_environment(self) -> dict[str, str]:
        return {
            "ZHENXUN_WEBUI_DEV_PORT": str(self.port),
            "ZHENXUN_WEBUI_DEV_TOKEN": self.token,
        }

    async def start(self) -> None:
        if self.process is not None:
            return
        env = os.environ.copy()
        env["PATH"] = os.pathsep.join(
            (
                str(self.source / "node_modules/.bin"),
                str(Path(self.node).parent),
                env.get("PATH", ""),
            )
        )
        env.update(
            WEBUI_DEV_MANAGED="1",
            WEBUI_DEV_HOST="127.0.0.1",
            WEBUI_DEV_PORT=str(self.port),
            WEBUI_DEV_TOKEN=self.token,
            BROWSER="none",
            NODE_ENV="development",
        )
        self.process = await self.supervisor.start_process(
            "webui_dev",
            partial(
                subprocess.Popen,
                [
                    self.node,
                    str(self.cli),
                    "serve",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(self.port),
                ],
                cwd=self.source,
                env=env,
                creationflags=(
                    subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
                ),
            ),
            ready=self._listening,
        )

    async def _listening(self, process: subprocess.Popen) -> None:
        deadline = asyncio.get_running_loop().time() + 15
        async with aiohttp.ClientSession(
            trust_env=False,
            cookie_jar=aiohttp.DummyCookieJar(),
            timeout=aiohttp.ClientTimeout(total=1),
        ) as client:
            while asyncio.get_running_loop().time() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f"WebUI dev server exited: {process.returncode}")
                try:
                    async with client.get(
                        f"http://127.0.0.1:{self.port}{PREFIX}status",
                        headers={"X-Zhenxun-Dev-Token": self.token},
                    ) as response:
                        if response.status == 200:
                            return
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    pass
                await asyncio.sleep(0.1)
        raise TimeoutError("WebUI dev server did not open its listener")

    async def stop(self) -> None:
        if self.process is not None:
            await self.supervisor.stop_process(self.process)
            self.process = None
