"""Resource snapshots collected by an existing managed worker, never on reads."""

import os
import sys
import time

import psutil


class ResourceSampler:
    def __init__(self):
        self.process = None
        self.children = {}
        self.last_heavy = 0.0
        self.heavy = {}

    def sample(self):
        started = time.monotonic()
        if self.process is None:
            self.process = psutil.Process(os.getpid())
        process = self.process
        try:
            with process.oneshot():
                result = {
                    "pid": process.pid,
                    "created_at": process.create_time(),
                    "rss_bytes": process.memory_info().rss,
                    "threads": process.num_threads(),
                    "cpu_percent": process.cpu_percent(None),
                    "sampled_at": time.time(),
                }
                if hasattr(process, "num_handles"):
                    result["handles"] = process.num_handles()
            children = {}
            child_rss = 0
            for child in process.children(recursive=True):
                try:
                    identity = child.pid, child.create_time()
                    child = self.children.get(identity, child)
                    child_rss += child.memory_info().rss
                    children[identity] = child
                except psutil.Error:
                    continue
            self.children = children
            result.update(children=len(children), children_rss_bytes=child_rss)
            if started - self.last_heavy >= 30:
                self.heavy = {
                    "modules": len(sys.modules),
                    "available_memory_bytes": psutil.virtual_memory().available,
                }
                self.last_heavy = started
            result.update(self.heavy)
            result["sample_duration_ms"] = (time.monotonic() - started) * 1000
            return result
        except psutil.Error as error:
            return {"sample_error": type(error).__name__, "sampled_at": time.time()}
