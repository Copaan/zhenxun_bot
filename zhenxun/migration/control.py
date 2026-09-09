from __future__ import annotations

from collections.abc import Callable
import json
import math
import os
from pathlib import Path
import re
import socket
import stat
import struct
import threading
import time
from typing import ClassVar
import uuid

import psutil

from zhenxun.utils.atomic_json import read_json_locked

from .errors import MigrationError
from .lease import InstanceLease
from .paths import contained_path

MAX_MESSAGE = 64 * 1024
REQUEST_TIMEOUT = 5.0
OPERATIONS = frozenset(
    {
        "capabilities",
        "export",
        "restore",
        "status",
        "cancel",
        "credentials",
        "phase_input",
        "maintenance_input",
        "validation_input",
        "validation_state",
    }
)


def _remaining(deadline: float, stopped: threading.Event | None = None) -> float:
    if stopped is not None and stopped.is_set():
        raise MigrationError("migration_control_stopped")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise MigrationError("migration_control_timeout")
    return remaining


def _encode(value: dict) -> bytes:
    try:
        data = json.dumps(value, ensure_ascii=True, allow_nan=False).encode()
    except (ValueError, TypeError, RecursionError):
        raise MigrationError("migration_control_message_invalid") from None
    if len(data) > MAX_MESSAGE:
        raise MigrationError("migration_control_message_limit")
    return data


def _decode(data: bytes) -> dict:
    if len(data) > MAX_MESSAGE:
        raise MigrationError("migration_control_message_limit")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise MigrationError("migration_control_message_invalid")
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError("non-finite JSON number")

    try:
        value = json.loads(
            data, object_pairs_hook=unique, parse_constant=invalid_constant
        )
    except (ValueError, TypeError, RecursionError):
        raise MigrationError("migration_control_message_invalid") from None
    if not isinstance(value, dict):
        raise MigrationError("migration_control_message_invalid")
    pending = [(value, 0)]
    count = 0
    while pending:
        item, depth = pending.pop()
        count += 1
        if count > 4096 or depth > 16:
            raise MigrationError("migration_control_message_limit")
        if isinstance(item, dict):
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
        elif isinstance(item, float) and not math.isfinite(item):
            raise MigrationError("migration_control_message_invalid")
    return value


def _address(project: Path, identity: str) -> str:
    if len(identity) != 32 or any(c not in "0123456789abcdef" for c in identity):
        raise MigrationError("migration_launcher_identity_mismatch")
    if os.name == "nt":
        return rf"\\.\pipe\zhenxun-migration-{identity}"
    parent = contained_path(project, "migration/control")
    path = parent / "launcher.sock"
    if path.is_symlink():
        raise MigrationError("migration_control_address_conflict")
    if path.exists():
        info = path.lstat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
            raise MigrationError("migration_control_address_conflict")
    return str(path)


class _Pipe:
    def __init__(self, handle: int):
        self.handle = handle

    @staticmethod
    def wait(overlapped, deadline, stopped=None):
        import _winapi

        try:
            while True:
                remaining = _remaining(deadline, stopped)
                result = _winapi.WaitForMultipleObjects(
                    [overlapped.event], False, max(1, int(min(remaining, 0.05) * 1000))
                )
                if result == _winapi.WAIT_OBJECT_0:
                    return overlapped.GetOverlappedResult(False)
        except BaseException:
            overlapped.cancel()
            raise

    def receive(self, deadline, stopped=None):
        import _winapi

        overlapped, _ = _winapi.ReadFile(self.handle, MAX_MESSAGE + 1, overlapped=True)
        size, error = self.wait(overlapped, deadline, stopped)
        if error == _winapi.ERROR_MORE_DATA or size > MAX_MESSAGE:
            raise MigrationError("migration_control_message_limit")
        if error or not size:
            raise MigrationError("migration_control_disconnected")
        return bytes(overlapped.getbuffer())

    def send(self, data, deadline, stopped=None):
        import _winapi

        overlapped, _ = _winapi.WriteFile(self.handle, data, overlapped=True)
        size, error = self.wait(overlapped, deadline, stopped)
        if error or size != len(data):
            raise MigrationError("migration_control_disconnected")

    def close(self):
        import _winapi

        if self.handle is not None:
            _winapi.CloseHandle(self.handle)
            self.handle = None


def _new_pipe(address, *, first):
    import ctypes
    from ctypes import wintypes

    from .access import windows_private_descriptor

    class SecurityAttributes(ctypes.Structure):
        _fields_: ClassVar = [
            ("length", wintypes.DWORD),
            ("descriptor", ctypes.c_void_p),
            ("inherit", wintypes.BOOL),
        ]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateNamedPipeW.argtypes = (
        [wintypes.LPCWSTR] + [wintypes.DWORD] * 6 + [ctypes.POINTER(SecurityAttributes)]
    )
    kernel.CreateNamedPipeW.restype = wintypes.HANDLE
    with windows_private_descriptor() as descriptor:
        attributes = SecurityAttributes(
            ctypes.sizeof(SecurityAttributes), descriptor, False
        )
        handle = kernel.CreateNamedPipeW(
            address,
            0x40000003 | (0x80000 if first else 0),
            4 | 2 | 8,
            2,
            MAX_MESSAGE,
            MAX_MESSAGE,
            1000,
            ctypes.byref(attributes),
        )
    if handle == ctypes.c_void_p(-1).value:
        raise MigrationError("migration_control_listen_failed")
    return _Pipe(handle)


def _pipe_peer(handle, *, server: bool) -> int:
    import ctypes
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    function = (
        kernel.GetNamedPipeServerProcessId
        if server
        else kernel.GetNamedPipeClientProcessId
    )
    function.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.ULONG)]
    pid = wintypes.ULONG()
    if not function(handle, ctypes.byref(pid)):
        raise MigrationError("migration_control_peer_unconfirmed")
    return pid.value


class _Socket:
    def __init__(self, connection: socket.socket):
        self.connection = connection

    def _read(self, size, deadline, stopped):
        data = bytearray()
        while len(data) < size:
            self.connection.settimeout(min(_remaining(deadline, stopped), 0.1))
            try:
                chunk = self.connection.recv(size - len(data))
            except TimeoutError:
                continue
            if not chunk:
                raise MigrationError("migration_control_disconnected")
            data.extend(chunk)
        return bytes(data)

    def receive(self, deadline, stopped=None):
        size = struct.unpack("!I", self._read(4, deadline, stopped))[0]
        if not 0 < size <= MAX_MESSAGE:
            raise MigrationError("migration_control_message_limit")
        return self._read(size, deadline, stopped)

    def send(self, data, deadline, stopped=None):
        packet = memoryview(struct.pack("!I", len(data)) + data)
        while packet:
            self.connection.settimeout(min(_remaining(deadline, stopped), 0.1))
            try:
                size = self.connection.send(packet)
            except TimeoutError:
                continue
            if not size:
                raise MigrationError("migration_control_disconnected")
            packet = packet[size:]

    def close(self):
        self.connection.close()


def _unix_peer(connection: socket.socket) -> tuple[int, int]:
    if not hasattr(socket, "SO_PEERCRED"):
        raise MigrationError("migration_control_peer_unavailable")
    pid, uid, _ = struct.unpack(
        "3i", connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
    )
    return pid, uid


class LocalControlServer:
    """Bounded local command transport; the owner supplies task dispatch only."""

    def __init__(
        self,
        lease: InstanceLease,
        handler: Callable[..., dict],
        *,
        with_peer: bool = False,
    ):
        lease.require_held()
        if lease.role != "launcher":
            raise MigrationError("migration_control_requires_launcher")
        self.lease, self.handler = lease, handler
        self.with_peer = with_peer
        self.address = _address(lease.project, lease.identity)
        self.stopped = threading.Event()
        self.thread = None
        self.listener = None
        self.failure = None

    def start(self):
        self.lease.require_held()
        if self.thread is not None:
            raise MigrationError("migration_control_already_started")
        if os.name == "nt":
            self.listener = _new_pipe(self.address, first=True)
        else:
            if not hasattr(socket, "SO_PEERCRED"):
                raise MigrationError("migration_control_peer_unavailable")
            path = Path(self.address)
            # The exclusive instance lease owns this fixed local socket path.
            if path.exists():
                if (
                    not stat.S_ISSOCK(path.lstat().st_mode)
                    or path.stat().st_uid != os.getuid()
                ):
                    raise MigrationError("migration_control_address_conflict")
                path.unlink()
            self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                self.listener.bind(self.address)
                path.chmod(0o600)
                self.listener.listen(4)
                self.listener.settimeout(0.1)
            except BaseException:
                self.listener.close()
                raise
        self.thread = threading.Thread(
            target=self._serve, name="zx-migration-control", daemon=False
        )
        try:
            self.thread.start()
        except BaseException:
            self.listener.close()
            self.thread = None
            raise
        return self

    def _accept(self):
        if os.name == "nt":
            import _winapi

            pipe = self.listener
            overlapped = _winapi.ConnectNamedPipe(pipe.handle, overlapped=True)
            _Pipe.wait(overlapped, time.monotonic() + 86400, self.stopped)
            try:
                self.listener = _new_pipe(self.address, first=False)
                pipe.peer_pid = _pipe_peer(pipe.handle, server=False)
            except BaseException:
                pipe.close()
                raise
            return pipe
        while True:
            _remaining(time.monotonic() + 1, self.stopped)
            try:
                connection, _ = self.listener.accept()
            except TimeoutError:
                continue
            try:
                pid, uid = _unix_peer(connection)
                if uid != os.getuid():
                    raise MigrationError("migration_control_peer_forbidden")
                wrapped = _Socket(connection)
                wrapped.peer_pid = pid
                return wrapped
            except BaseException:
                connection.close()
                raise

    def _serve(self):
        try:
            while not self.stopped.is_set():
                connection = None
                # Listener failures are terminal, not a tight accept/retry loop.
                try:
                    connection = self._accept()
                except (MigrationError, OSError):
                    if self.stopped.is_set():
                        break
                    raise
                try:
                    deadline = time.monotonic() + REQUEST_TIMEOUT
                    request = _decode(connection.receive(deadline, self.stopped))
                    self.lease.require_held()
                    nonce = request.get("nonce")
                    if (
                        type(request.get("schema")) is not int
                        or request.get("schema") != 1
                        or request.get("launcher") != self.lease.identity
                        or not isinstance(nonce, str)
                        or len(nonce) != 32
                        or not isinstance(request.get("operation"), str)
                        or request.get("operation") not in OPERATIONS
                        or not isinstance(request.get("payload"), dict)
                    ):
                        raise MigrationError("migration_control_request_invalid")
                    response = {
                        "schema": 1,
                        "launcher": self.lease.identity,
                        "nonce": nonce,
                    }
                    try:
                        response["result"] = self.handler(
                            request["operation"],
                            request["payload"],
                            **(
                                {"peer_pid": connection.peer_pid}
                                if self.with_peer
                                else {}
                            ),
                        )
                    except Exception as error:
                        response["error"] = (
                            error.code
                            if isinstance(error, MigrationError)
                            and re.fullmatch(r"migration_[a-z0-9_]{1,100}", error.code)
                            else "migration_control_dispatch_failed"
                        )
                    connection.send(_encode(response), deadline, self.stopped)
                except (MigrationError, OSError, ValueError):
                    if self.stopped.is_set():
                        break
                finally:
                    if connection is not None:
                        connection.close()
        except BaseException:
            self.failure = "migration_control_server_failed"
        finally:
            if self.listener is not None:
                self.listener.close()
            if os.name != "nt":
                Path(self.address).unlink(missing_ok=True)

    def close(self, *, deadline: float):
        self.stopped.set()
        if self.thread is not None:
            self.thread.join(max(0, deadline - time.monotonic()))
            if self.thread.is_alive():
                raise MigrationError("migration_control_close_unconfirmed")
        elif self.listener is not None:
            self.listener.close()


def request_control(
    project: Path, operation: str, payload: dict, *, timeout: float = REQUEST_TIMEOUT
) -> dict:
    if (
        not isinstance(operation, str)
        or operation not in OPERATIONS
        or not 0 < timeout <= 30
    ):
        raise MigrationError("migration_control_request_invalid")
    root = contained_path(project, "migration/control")
    if not root.exists():
        raise MigrationError("migration_launcher_unavailable")
    owner = read_json_locked(root / "owner.json", None)
    if not isinstance(owner, dict) or owner.get("role") != "launcher":
        raise MigrationError("migration_launcher_unavailable")
    if (
        type(owner.get("pid")) is not int
        or owner["pid"] <= 0
        or type(owner.get("created_at")) not in {int, float}
        or not isinstance(owner.get("identity"), str)
    ):
        raise MigrationError("migration_launcher_identity_mismatch")
    try:
        if psutil.Process(owner["pid"]).create_time() != owner["created_at"]:
            raise MigrationError("migration_launcher_identity_mismatch")
    except (psutil.Error, KeyError):
        raise MigrationError("migration_launcher_unavailable") from None
    address = _address(project, owner["identity"])
    deadline = time.monotonic() + timeout
    nonce = uuid.uuid4().hex
    data = _encode(
        {
            "schema": 1,
            "launcher": owner["identity"],
            "nonce": nonce,
            "operation": operation,
            "payload": payload,
        }
    )
    connection = None
    try:
        if os.name == "nt":
            import _winapi

            _winapi.WaitNamedPipe(address, max(1, int(_remaining(deadline) * 1000)))
            handle = _winapi.CreateFile(
                address,
                _winapi.GENERIC_READ | _winapi.GENERIC_WRITE,
                0,
                0,
                _winapi.OPEN_EXISTING,
                _winapi.FILE_FLAG_OVERLAPPED,
                0,
            )
            connection = _Pipe(handle)
            _winapi.SetNamedPipeHandleState(
                handle, _winapi.PIPE_READMODE_MESSAGE, None, None
            )
            peer = _pipe_peer(handle, server=True)
        else:
            raw = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            connection = _Socket(raw)
            raw.settimeout(_remaining(deadline))
            raw.connect(address)
            peer, uid = _unix_peer(raw)
            if uid != os.getuid():
                raise MigrationError("migration_control_peer_forbidden")
        if (
            peer != owner["pid"]
            or psutil.Process(peer).create_time() != owner["created_at"]
        ):
            raise MigrationError("migration_launcher_identity_mismatch")
        connection.send(data, deadline)
        response = _decode(connection.receive(deadline))
        if (
            response.get("schema") != 1
            or response.get("launcher") != owner["identity"]
            or response.get("nonce") != nonce
        ):
            raise MigrationError("migration_control_response_invalid")
        if response.get("error"):
            raise MigrationError(response["error"])
        if not isinstance(response.get("result"), dict):
            raise MigrationError("migration_control_response_invalid")
        return response["result"]
    except (OSError, psutil.Error):
        raise MigrationError("migration_control_transport_failed") from None
    finally:
        if connection is not None:
            connection.close()
