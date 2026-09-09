from __future__ import annotations

import asyncio
from asyncio.sslproto import SSLProtocol
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import re
import socket
import ssl
import sys
from threading import RLock
import time
from typing import Any
import uuid
import weakref

_UVICORN_HTTP_PREFIX = "uvicorn.protocols.http."
_UVICORN_WEBSOCKET_PREFIX = "uvicorn.protocols.websockets."
_AIOHTTP_SERVER_PREFIX = "aiohttp.web_protocol"
_DEBUG_INTERVAL_SECONDS = 30.0
_MAX_CONTEXT_OBJECTS = 64
_SAFE_SYMBOL = re.compile(r"^[A-Za-z0-9_.<>-]{1,160}$")
_RELATED_ATTRIBUTES = (
    "__self__",
    "_protocol",
    "protocol",
    "_app_protocol",
    "_app_transport",
    "_ssl_protocol",
    "_transport",
)


@dataclass
class _InboundTLSOwner:
    loop: weakref.ReferenceType[Any]
    server: weakref.ReferenceType[Any]
    socket: weakref.ReferenceType[Any]
    recovery_transport: weakref.ReferenceType[Any] | None = None
    recovered: bool = False


class TransportRuntime:
    """Own event-loop reset handling and Uvicorn's outer transport lifetime."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._previous_handler: Callable[..., Any] | None = None
        self._installed_handler: Callable[..., Any] | None = None
        self._counts: Counter[str] = Counter()
        self._last_debug: dict[str, float] = {}
        self._last_unclassified_reset: dict[str, Any] | None = None
        self._lifespan_stopped = False
        self._uvicorn_server_type: type[Any] | None = None
        self._uvicorn_original_handle_exit: Callable[..., Any] | None = None
        self._uvicorn_wrapped_handle_exit: Callable[..., Any] | None = None
        self._shutdown_task: asyncio.Task[None] | None = None
        self._tls_transports: weakref.WeakSet[Any] = weakref.WeakSet()
        self._tls_owners: weakref.WeakKeyDictionary[SSLProtocol, _InboundTLSOwner] = (
            weakref.WeakKeyDictionary()
        )
        self._original_ssl_factory: Callable[..., Any] | None = None
        self._wrapped_ssl_factory: Callable[..., Any] | None = None
        self._ssl_factory_was_local = False
        self._lock = RLock()

    def install(self) -> None:
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._loop is loop and self._installed_handler is not None:
                return
            if self._loop is not None:
                self.restore()
            self._loop = loop
            self._previous_handler = loop.get_exception_handler()
            self._installed_handler = self._handle_exception
            self._lifespan_stopped = False
            try:
                loop.set_exception_handler(self._installed_handler)
            except Exception:
                self.restore()
                raise
            original = getattr(loop, "_make_ssl_transport", None)
            if callable(original):
                self._original_ssl_factory = original
                self._ssl_factory_was_local = "_make_ssl_transport" in getattr(
                    loop, "__dict__", {}
                )

                def track_ssl(*args, **kwargs):
                    transport = original(*args, **kwargs)
                    protocol = self._safe_getattr(transport, "_ssl_protocol")
                    app_protocol = self._safe_getattr(protocol, "_app_protocol")
                    if (
                        kwargs.get("server_side")
                        and kwargs.get("server") is not None
                        and type(app_protocol).__module__.startswith(
                            "uvicorn.protocols."
                        )
                    ):
                        self._tls_transports.add(transport)
                        raw_socket = (
                            args[0]
                            if args
                            else kwargs.get("rawsock", kwargs.get("sock"))
                        )
                        server = kwargs["server"]
                        if (
                            isinstance(protocol, SSLProtocol)
                            and protocol._loop is loop
                            and isinstance(server, asyncio.Server)
                            and server.get_loop() is loop
                            and isinstance(raw_socket, socket.socket)
                        ):
                            # SSL connection_lost clears both app/underlying links
                            # before Proactor closes the socket and detaches server.
                            self._tls_owners[protocol] = _InboundTLSOwner(
                                weakref.ref(loop),
                                weakref.ref(server),
                                weakref.ref(raw_socket),
                            )
                    return transport

                self._wrapped_ssl_factory = track_ssl
                try:
                    loop._make_ssl_transport = track_ssl
                except Exception:
                    self.restore()
                    raise

    def retain_until_loop_close(self) -> None:
        """Keep the proxy installed while Uvicorn tears transports down."""
        with self._lock:
            self._lifespan_stopped = True

    def restore(self) -> None:
        restore_error: Exception | None = None
        with self._lock:
            loop = self._loop
            previous = self._previous_handler
            installed = self._installed_handler
            self._loop = None
            self._previous_handler = None
            self._installed_handler = None
            if (
                loop is not None
                and self._wrapped_ssl_factory is not None
                and getattr(loop, "_make_ssl_transport", None)
                is self._wrapped_ssl_factory
            ):
                try:
                    if self._ssl_factory_was_local:
                        loop._make_ssl_transport = self._original_ssl_factory
                    else:
                        del loop._make_ssl_transport
                except Exception as error:
                    restore_error = error
            self._wrapped_ssl_factory = self._original_ssl_factory = None
            self._tls_transports.clear()
            self._tls_owners.clear()
        if (
            loop is not None
            and not loop.is_closed()
            and loop.get_exception_handler() is installed
        ):
            loop.set_exception_handler(previous)
        if restore_error is not None:
            raise restore_error

    def record(self, metric: str, amount: int = 1) -> None:
        with self._lock:
            self._counts[metric] += amount

    def record_diagnostic(self, code: str) -> None:
        self.record(code)
        now = time.monotonic()
        with self._lock:
            if (
                now - self._last_debug.get(code, float("-inf"))
                < _DEBUG_INTERVAL_SECONDS
            ):
                return
            self._last_debug[code] = now
        from zhenxun.services.log import logger

        logger.info(f"入站连接诊断 | code={code}", "WebUi")

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            counts = dict(self._counts)
            installed = self._installed_handler is not None
            lifespan_stopped = self._lifespan_stopped
            bridge_installed = self._uvicorn_wrapped_handle_exit is not None
            last_unclassified = (
                dict(self._last_unclassified_reset)
                if self._last_unclassified_reset
                else None
            )
        return {
            "exception_proxy_installed": installed,
            "lifespan_stopped": lifespan_stopped,
            "signal_bridge_installed": bridge_installed,
            "http_reset_count": counts.get("http_reset", 0),
            "websocket_reset_count": counts.get("websocket_reset", 0),
            "proactor_close_reset_count": counts.get("proactor_close_reset", 0),
            "tls_close_recovery_count": counts.get("tls_close_recovery", 0),
            "tls_close_recovery_failed_count": counts.get(
                "tls_close_recovery_failed", 0
            ),
            "unclassified_windows_reset_count": counts.get(
                "unclassified_windows_reset", 0
            ),
            "websocket_send_failure_count": counts.get("websocket_send_failure", 0),
            "cooperative_close_count": counts.get("cooperative_close", 0),
            "signal_shutdown_count": counts.get("signal_shutdown", 0),
            "predrained_connection_count": counts.get("predrained_connection", 0),
            "tls_drain_abort_count": counts.get("tls_drain_abort", 0),
            "tracked_inbound_tls_transports": len(self._tls_transports),
            "last_unclassified_reset": last_unclassified,
            "inbound_diagnostics": {
                key: value
                for key, value in counts.items()
                if key.startswith(("onebot_", "tls_"))
            },
        }

    def _handle_exception(
        self, loop: asyncio.AbstractEventLoop, context: dict[str, Any]
    ) -> None:
        error = context.get("exception")
        callback = self._safe_getattr(
            context.get("handle"), "_callback"
        ) or context.get("callback")
        if self._safe_getattr(callback, "__name__") == "_call_connection_lost":
            if self._recover_proactor_tls_close(loop, callback, error):
                self.record("proactor_close_reset")
                self._debug_disconnect("proactor_close_reset")
            else:
                self._delegate(loop, context)
            return
        channel = self._uvicorn_channel(context)
        if channel and isinstance(error, ssl.SSLError):
            self.record_diagnostic("tls_transport_failed")
        if channel and self._is_windows_connection_reset(error):
            metric = f"{channel}_reset"
            self.record(metric)
            self._debug_disconnect(metric)
            return
        if self._is_windows_connection_reset(error):
            self.record("unclassified_windows_reset")
            self._record_unclassified_reset(context)
        self._delegate(loop, context)

    def install_uvicorn_signal_bridge(self) -> bool:
        """Start inbound connection drain before Uvicorn enters shutdown."""
        try:
            import uvicorn

            server_type = uvicorn.Server
            original = server_type.handle_exit
        except (AttributeError, ImportError):
            return False
        with self._lock:
            if self._uvicorn_wrapped_handle_exit is not None:
                return self._uvicorn_server_type is server_type

            def wrapped(server: Any, sig: int, frame: Any) -> Any:
                from zhenxun.services.lifecycle import lifecycle_kernel

                lifecycle_kernel.request_shutdown()
                self._begin_signal_shutdown(server)
                return original(server, sig, frame)

            wrapped.__name__ = getattr(original, "__name__", "handle_exit")
            wrapped.__qualname__ = getattr(
                original, "__qualname__", "Server.handle_exit"
            )
            setattr(wrapped, "__zhenxun_transport_bridge__", True)
            server_type.handle_exit = wrapped
            self._uvicorn_server_type = server_type
            self._uvicorn_original_handle_exit = original
            self._uvicorn_wrapped_handle_exit = wrapped
        return True

    def restore_uvicorn_signal_bridge(self) -> None:
        with self._lock:
            server_type = self._uvicorn_server_type
            original = self._uvicorn_original_handle_exit
            wrapped = self._uvicorn_wrapped_handle_exit
            self._uvicorn_server_type = None
            self._uvicorn_original_handle_exit = None
            self._uvicorn_wrapped_handle_exit = None
            self._shutdown_task = None
        if (
            server_type is not None
            and original is not None
            and wrapped is not None
            and getattr(server_type, "handle_exit", None) is wrapped
        ):
            server_type.handle_exit = original

    def _begin_signal_shutdown(self, server: Any) -> None:
        self.record("signal_shutdown")
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        servers = tuple(
            listener
            for listener in (self._safe_getattr(server, "servers") or ())
            if isinstance(listener, asyncio.Server) and listener.get_loop() is loop
        )
        for listener in servers:
            listener.close()
        server_state = self._safe_getattr(server, "server_state")
        connections = tuple(self._safe_getattr(server_state, "connections") or ())
        tls_transports = []
        # connection_lost may remove a protocol from Uvicorn before the SSL
        # transport releases its asyncio.Server reference.
        for transport in tuple(self._tls_transports):
            protocol = self._safe_getattr(transport, "_ssl_protocol")
            if protocol is not None:
                tls_transports.append((transport, protocol))
        for connection in connections:
            if not type(connection).__module__.startswith("uvicorn.protocols."):
                continue
            transport = self._safe_getattr(connection, "transport")
            ssl_protocol = self._safe_getattr(transport, "_ssl_protocol")
            if ssl_protocol is not None:
                if not any(item[0] is transport for item in tls_transports):
                    tls_transports.append((transport, ssl_protocol))
        with self._lock:
            task = self._shutdown_task
            if task is not None and not task.done():
                return
            self._shutdown_task = loop.create_task(
                self._quiesce_webui_connections(
                    server_state,
                    tls_transports,
                    servers,
                ),
                name="webui-transport-signal-quiesce",
            )
            self._shutdown_task.add_done_callback(self._observe_shutdown_task)

    def _observe_shutdown_task(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and task.exception() is not None:
            self.record_diagnostic("tls_shutdown_cleanup_failed")

    async def _quiesce_webui_connections(
        self, server_state=None, tls_transports=(), servers=()
    ) -> None:
        from zhenxun.services.lifecycle import lifecycle_kernel

        deadline = time.monotonic() + lifecycle_kernel.shutdown_remaining(2.0)
        try:
            security = sys.modules.get("zhenxun.builtin_plugins.web_ui.security")
            quiesce = getattr(security, "quiesce_authenticated_websockets", None)
            remaining = max(0.0, deadline - time.monotonic())
            if callable(quiesce) and remaining > 0:
                await quiesce(timeout=min(0.75, remaining))
        except Exception:
            pass
        if server_state is None:
            return
        loop = asyncio.get_running_loop()
        servers = tuple(
            listener
            for listener in servers
            if isinstance(listener, asyncio.Server) and listener.get_loop() is loop
        )
        aborted = set()
        # Accept completions and active responses may finish after the signal.
        # Refresh ownership on every pass without extending the drain deadline.
        while servers:
            current = dict(tls_transports)
            current.update(
                (transport, self._safe_getattr(transport, "_ssl_protocol"))
                for transport in tuple(self._tls_transports)
            )
            for transport, ssl_protocol in current.items():
                if not isinstance(ssl_protocol, SSLProtocol):
                    continue
                owner = self._tls_owners.get(ssl_protocol)
                if owner is None or owner.loop() is not loop:
                    continue
                underlying = self._safe_getattr(ssl_protocol, "_transport")
                owner_server = owner.server()
                if (
                    underlying is None
                    or underlying in aborted
                    or owner_server not in servers
                    or self._safe_getattr(underlying, "_loop") is not loop
                    or self._safe_getattr(underlying, "_server") is not owner_server
                    or owner.socket() is None
                    or self._safe_getattr(underlying, "_sock") is not owner.socket()
                    or owner_server.is_serving()
                ):
                    continue
                app_protocol = self._safe_getattr(ssl_protocol, "_app_protocol")
                if transport.is_closing() or (
                    app_protocol not in server_state.connections
                    and not server_state.tasks
                ):
                    underlying.abort()
                    aborted.add(underlying)
                    self.record("tls_drain_abort")
            if all(listener._active_count == 0 for listener in servers):
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.record_diagnostic("tls_shutdown_drain_incomplete")
                return
            await asyncio.sleep(min(0.05, remaining))

    def _delegate(
        self, loop: asyncio.AbstractEventLoop, context: dict[str, Any]
    ) -> None:
        with self._lock:
            previous = self._previous_handler
        if previous is not None:
            previous(loop, context)
        else:
            loop.default_exception_handler(context)

    @staticmethod
    def _is_windows_connection_reset(error: Any) -> bool:
        return isinstance(error, ConnectionResetError) or (
            isinstance(error, OSError) and getattr(error, "winerror", None) == 10054
        )

    @classmethod
    def _context_objects(cls, context: dict[str, Any]) -> list[Any]:
        objects: list[Any] = [
            context.get("protocol"),
            context.get("transport"),
            context.get("handle"),
            context.get("callback"),
        ]
        objects.append(cls._safe_getattr(context.get("handle"), "_callback"))
        result: list[Any] = []
        visited: set[int] = set()
        while objects and len(result) < _MAX_CONTEXT_OBJECTS:
            current = objects.pop()
            if current is None or id(current) in visited:
                continue
            visited.add(id(current))
            result.append(current)
            objects.extend(
                cls._safe_getattr(current, attribute)
                for attribute in _RELATED_ATTRIBUTES
            )
        return result

    @staticmethod
    def _safe_getattr(value: Any, attribute: str) -> Any:
        try:
            return getattr(value, attribute, None)
        except Exception:
            return None

    @staticmethod
    def _safe_symbol(value: Any) -> str:
        text = str(value or "unknown")
        return text if _SAFE_SYMBOL.fullmatch(text) else "[redacted]"

    @classmethod
    def _uvicorn_channel(cls, context: dict[str, Any]) -> str | None:
        for current in cls._context_objects(context):
            module = str(
                getattr(current, "__module__", "")
                or getattr(type(current), "__module__", "")
            )
            if module.startswith(_UVICORN_WEBSOCKET_PREFIX):
                return "websocket"
            if module.startswith(_UVICORN_HTTP_PREFIX):
                return "http"
            if module.startswith(_AIOHTTP_SERVER_PREFIX):
                return "http"
        return None

    def _recover_proactor_tls_close(self, loop, callback, error) -> bool:
        if (
            sys.platform != "win32"
            or loop is not self._loop
            or not isinstance(error, OSError)
            or getattr(error, "winerror", None) != 10054
        ):
            return False
        from asyncio.proactor_events import _ProactorSocketTransport

        transport = self._safe_getattr(callback, "__self__")
        if (
            type(transport) is not _ProactorSocketTransport
            or self._safe_getattr(callback, "__func__")
            is not _ProactorSocketTransport._call_connection_lost
            or transport._loop is not loop
            or not transport.is_closing()
        ):
            return False
        protocol = transport._protocol
        if not isinstance(protocol, SSLProtocol):
            return False
        owner = self._tls_owners.get(protocol)
        if owner is None or owner.loop() is not loop:
            return False
        trace = error.__traceback__
        while trace is not None:
            if (
                trace.tb_frame.f_code
                is _ProactorSocketTransport._call_connection_lost.__code__
                and trace.tb_frame.f_locals.get("self") is transport
            ):
                break
            trace = trace.tb_next
        if trace is None:
            return False
        if owner.recovery_transport is not None:
            return bool(
                owner.recovery_transport() is transport
                and owner.recovered
                and transport._called_connection_lost
                and transport._sock is None
                and transport._server is None
            )
        server, raw_socket = owner.server(), owner.socket()
        if (
            server is None
            or server.get_loop() is not loop
            or transport._server is not server
            or raw_socket is None
            or transport._sock is not raw_socket
            or transport._called_connection_lost
            or not isinstance(protocol, SSLProtocol)
            or protocol._loop is not loop
            or protocol._transport is not None
            or protocol._app_protocol is not None
            or protocol._app_transport is not None
        ):
            return False
        # Do not repeat protocol.connection_lost or socket.shutdown. A failed
        # detach may have mutated accounting, so never retry an uncertain repair.
        owner.recovery_transport = weakref.ref(transport)
        try:
            raw_socket.close()
            transport._sock = None
            server._detach()
            transport._server = None
            transport._called_connection_lost = True
        except Exception as recovery_error:
            self.record("tls_close_recovery_failed")
            self._delegate(
                loop,
                {
                    "message": "Inbound TLS close recovery failed",
                    "exception": recovery_error,
                    "transport": transport,
                },
            )
            return False
        owner.recovered = True
        self.record("tls_close_recovery")
        return True

    def _record_unclassified_reset(self, context: dict[str, Any]) -> None:
        objects = self._context_objects(context)
        modules = sorted(
            {
                self._safe_symbol(
                    getattr(item, "__module__", "")
                    or getattr(type(item), "__module__", "unknown")
                )
                for item in objects
                if item is not None
            }
        )[:12]
        callback = self._safe_getattr(
            context.get("handle"), "_callback"
        ) or context.get("callback")
        diagnostic = {
            "diagnostic_id": f"transport-{uuid.uuid4().hex[:12]}",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "callback_module": self._safe_symbol(
                getattr(callback, "__module__", "unknown")
            ),
            "callback_name": self._safe_symbol(
                getattr(callback, "__qualname__", None)
                or getattr(callback, "__name__", "unknown")
            ),
            "module_chain": modules,
        }
        with self._lock:
            self._last_unclassified_reset = diagnostic
        self._log_unclassified_reset(diagnostic)

    def _log_unclassified_reset(self, diagnostic: dict[str, Any]) -> None:
        now = time.monotonic()
        metric = "unclassified_windows_reset"
        with self._lock:
            last = self._last_debug.get(metric, 0.0)
            if now - last < _DEBUG_INTERVAL_SECONDS:
                return
            self._last_debug[metric] = now
        try:
            from zhenxun.services.log import logger

            logger.warning(
                "未分类Windows连接重置，将交给原异常处理器 | "
                f"diagnostic_id={diagnostic['diagnostic_id']} | "
                f"callback={diagnostic['callback_module']}:"
                f"{diagnostic['callback_name']}",
                "WebUi",
            )
        except Exception:
            pass

    def _debug_disconnect(self, metric: str) -> None:
        now = time.monotonic()
        with self._lock:
            last = self._last_debug.get(metric, 0.0)
            if now - last < _DEBUG_INTERVAL_SECONDS:
                return
            self._last_debug[metric] = now
        try:
            from zhenxun.services.log import logger

            logger.debug(
                f"已回收连接关闭阶段重置 | category={metric.removesuffix('_reset')}",
                "WebUi",
            )
        except Exception:
            pass


transport_runtime = TransportRuntime()

__all__ = ["TransportRuntime", "transport_runtime"]
