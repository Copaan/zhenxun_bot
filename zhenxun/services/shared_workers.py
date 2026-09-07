import threading
from weakref import WeakSet

_threads = WeakSet()
_lock = threading.Lock()


def register_shared_worker(thread):
    with _lock:
        _threads.add(thread)


def shared_worker_ids():
    with _lock:
        return {id(thread) for thread in _threads if thread.is_alive()}


def shared_worker_threads():
    with _lock:
        return tuple(thread for thread in _threads if thread.is_alive())
