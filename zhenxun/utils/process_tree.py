"""Discover descendants only through verified immediate parent generations."""

import psutil


def verified_descendants(root: psutil.Process) -> list[psutil.Process]:
    found: list[psutil.Process] = []
    pending = [(root, root.create_time())]
    seen = {(root.pid, root.create_time())}
    while pending:
        parent, expected = pending.pop()
        try:
            if psutil.Process(parent.pid).create_time() != expected:
                continue
            # Recursive psutil discovery compares all birth times with the
            # root. Windows can retain a PPID whose PID now names a younger
            # intermediate process, falsely joining unrelated process trees.
            children = parent.children(recursive=False)
            for child in children:
                try:
                    created = child.create_time()
                    if (
                        created < expected
                        or child.ppid() != parent.pid
                        or not child.is_running()
                        or psutil.Process(parent.pid).create_time() != expected
                    ):
                        continue
                    identity = (child.pid, created)
                    if identity in seen:
                        continue
                    seen.add(identity)
                    found.append(child)
                    pending.append((child, created))
                except (psutil.NoSuchProcess, psutil.ZombieProcess):
                    continue
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
    return found
