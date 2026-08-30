from __future__ import annotations

from collections.abc import Callable
import logging
import os
from threading import Lock

_UVICORN_READY_PREFIX = "Uvicorn running on "
_UVICORN_STARTUP_FAILURE_PREFIX = "Application startup failed"


class _ReadyLogHandler(logging.Handler):
    def __init__(self, owner: UvicornReadyBanner) -> None:
        super().__init__(logging.INFO)
        self._owner = owner

    def emit(self, record: logging.LogRecord) -> None:
        self._owner.handle(self, record)


class UvicornReadyBanner:
    """Run one callback after Uvicorn has bound the worker's listening socket."""

    def __init__(
        self,
        record_logger_name: str = "uvicorn.error",
        handler_logger_name: str = "uvicorn",
    ) -> None:
        self._record_logger_name = record_logger_name
        self._handler_logger_name = handler_logger_name
        self._lock = Lock()
        self._handler: _ReadyLogHandler | None = None
        self._callback: Callable[[], None] | None = None
        self._emitted = False

    def arm(self, callback: Callable[[], None]) -> bool:
        with self._lock:
            if self._handler is not None or self._emitted:
                return False
            handler = _ReadyLogHandler(self)
            self._handler = handler
            self._callback = callback
            logging.getLogger(self._handler_logger_name).addHandler(handler)
            return True

    def handle(self, handler: _ReadyLogHandler, record: logging.LogRecord) -> None:
        if record.process != os.getpid() or record.name != self._record_logger_name:
            return
        message = record.getMessage()
        is_ready = message.startswith(_UVICORN_READY_PREFIX)
        is_startup_failure = message.startswith(_UVICORN_STARTUP_FAILURE_PREFIX)
        if not is_ready and not is_startup_failure:
            return

        with self._lock:
            if handler is not self._handler or self._emitted:
                return
            logging.getLogger(self._handler_logger_name).removeHandler(handler)
            handler.close()
            callback = self._callback
            self._handler = None
            self._callback = None
            self._emitted = is_ready

        if is_ready and callback is not None:
            callback()

    def reset(self) -> None:
        with self._lock:
            handler = self._handler
            self._handler = None
            self._callback = None
            self._emitted = False
            if handler is not None:
                logging.getLogger(self._handler_logger_name).removeHandler(handler)
                handler.close()


webui_ready_banner = UvicornReadyBanner()


__all__ = ["UvicornReadyBanner", "webui_ready_banner"]
