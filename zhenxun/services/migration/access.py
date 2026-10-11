from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path

from .errors import MigrationError
from .paths import contained_path


def private_directory(path: Path) -> Path:
    path = contained_path(path.parent, path.name)
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if os.name != "nt":
        path.chmod(0o700)
        return path
    import ctypes
    from ctypes import wintypes

    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    advapi.SetFileSecurityW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        ctypes.c_void_p,
    ]
    with windows_private_descriptor(inherit=True) as descriptor:
        if not advapi.SetFileSecurityW(str(path), 0x80000004, descriptor):
            raise MigrationError("migration_private_acl_failed")
    return path


@contextmanager
def windows_private_descriptor(*, inherit: bool = False):
    import ctypes
    from ctypes import wintypes

    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    pointer = ctypes.c_void_p
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.LocalFree.argtypes = [pointer]
    kernel.LocalFree.restype = pointer
    advapi.OpenProcessToken.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.HANDLE),
    ]
    advapi.GetTokenInformation.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        pointer,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
    ]
    advapi.ConvertSidToStringSidW.argtypes = [pointer, ctypes.POINTER(wintypes.LPWSTR)]
    advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        ctypes.POINTER(pointer),
        pointer,
    ]
    token = wintypes.HANDLE()
    sid_string = wintypes.LPWSTR()
    descriptor = pointer()
    try:
        if not advapi.OpenProcessToken(
            kernel.GetCurrentProcess(), 8, ctypes.byref(token)
        ):
            raise MigrationError("migration_private_acl_failed")
        length = wintypes.DWORD()
        advapi.GetTokenInformation(token, 1, None, 0, ctypes.byref(length))
        buffer = ctypes.create_string_buffer(length.value)
        if not advapi.GetTokenInformation(
            token, 1, buffer, length, ctypes.byref(length)
        ):
            raise MigrationError("migration_private_acl_failed")
        sid = ctypes.cast(buffer, ctypes.POINTER(pointer))[0]
        if not advapi.ConvertSidToStringSidW(sid, ctypes.byref(sid_string)):
            raise MigrationError("migration_private_acl_failed")
        sddl = (
            "D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)" + f"(A;OICI;FA;;;{sid_string.value})"
            if inherit
            else f"D:P(A;;GA;;;{sid_string.value})"
        )
        if not advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl, 1, ctypes.byref(descriptor), None
        ):
            raise MigrationError("migration_private_acl_failed")
        yield descriptor
    finally:
        if descriptor:
            kernel.LocalFree(descriptor)
        if sid_string:
            kernel.LocalFree(ctypes.cast(sid_string, pointer))
        if token:
            kernel.CloseHandle(token)
