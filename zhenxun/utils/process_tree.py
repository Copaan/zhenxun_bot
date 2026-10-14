"""Discover descendants only through verified immediate parent generations."""

import contextlib

import psutil


def _children_by_parent() -> dict[int, list[int]]:
    """读取一次系统进程表，按父进程分组

    返回:
        dict[int, list[int]]: 父进程 PID 到子进程 PID 列表的映射
    """
    # psutil 在 Windows 上每次 children()/ppid() 都会遍历整张进程表，
    # 这里只取一次快照，让整棵树的发现只付出一次遍历成本。
    snapshot = getattr(psutil, "_ppid_map", None)
    if snapshot is not None:
        pairs = snapshot().items()
    else:
        pairs = (
            (item.info["pid"], item.info["ppid"])
            for item in psutil.process_iter(["pid", "ppid"])
        )
    result: dict[int, list[int]] = {}
    for pid, ppid in pairs:
        if pid != ppid:
            result.setdefault(ppid, []).append(pid)
    return result


def verified_descendants(root: psutil.Process) -> list[psutil.Process]:
    """按代校验出生时间，查找进程的全部后代

    参数:
        root: 根进程

    返回:
        list[psutil.Process]: 仍在运行且身份可信的后代进程
    """
    children_by_parent = _children_by_parent()
    found: list[psutil.Process] = []
    pending = [(root, root.create_time())]
    seen = {(root.pid, root.create_time())}
    while pending:
        parent, expected = pending.pop()
        try:
            if psutil.Process(parent.pid).create_time() != expected:
                continue
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
        # Recursive psutil discovery compares all birth times with the
        # root. Windows can retain a PPID whose PID now names a younger
        # intermediate process, falsely joining unrelated process trees.
        for pid in children_by_parent.get(parent.pid, ()):
            try:
                child = psutil.Process(pid)
                created = child.create_time()
                if created < expected or not child.is_running():
                    continue
                identity = (child.pid, created)
                if identity in seen:
                    continue
                seen.add(identity)
                found.append(child)
                pending.append((child, created))
            except (psutil.NoSuchProcess, psutil.ZombieProcess):
                continue
    return found


def kill_tree(
    processes: list[psutil.Process], timeout: float = 5
) -> list[psutil.Process]:
    """逆序结束进程（先后代、后根进程）并等待退出

    参数:
        processes: 根进程在前、后代在后的进程列表
        timeout: 等待退出的秒数

    返回:
        list[psutil.Process]: 超时后仍存活的进程
    """
    for item in reversed(processes):
        with contextlib.suppress(psutil.NoSuchProcess):
            if item.is_running():
                item.kill()
    return psutil.wait_procs(processes, timeout=timeout)[1]
