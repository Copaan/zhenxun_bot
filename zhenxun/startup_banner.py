"""Small terminal-only startup mark; no application or image-library imports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import importlib.metadata
import os
import platform
import shutil
import sys
from typing import TextIO
import unicodedata

# Quantized from the supplied swirl artwork. Spaces preserve the console background.
_PALETTE = (
    (250, 128, 116),
    (251, 151, 141),
    (245, 157, 166),
    (249, 183, 186),
    (251, 205, 195),
    (255, 218, 186),
    (255, 238, 181),
    (253, 254, 166),
)
_GRID = (
    "                                                                ",
    "                            1111111                             ",
    "                     1234566666666666654321                     ",
    "                 134666666555555555556666666531                 ",
    "              136666555555666666666666555555666641              ",
    "            1466655566666666666666666666666666666763            ",
    "          15665555555555556666666666666666666666666772          ",
    "         36655666666666666666556666666666666666666667771        ",
    "       15656665544444444445556666666666666666666777777771       ",
    "      1666544333333333333334445666666666666667777777777771      ",
    "      66443333333333333333444444456666666677777777777777771     ",
    "     453333333333333333334444555555666777777777777777788887     ",
    "    1533333333333333333344443111113467777777777777888888888     ",
    "    33333333333333333334431         157777777777888888888881    ",
    "    333333333333333334432             6777778888888888888881    ",
    "    333333333333333343211             177888888888888888888     ",
    "    333333333333333431111             288888888888888888881     ",
    "    333333333333334311111             88888888888888888881      ",
    "    2333333333333441111111          27888888888888888882        ",
    "    13333333333344311111111         168888888888888841          ",
    "     23333333334442111111111            1122222211              ",
    "      333333334444311111111111                                  ",
    "       33333344444411111111112211                               ",
    "        343344444442111111111112221111            11122         ",
    "         144444444441111111111111222222221111112222231          ",
    "          1344444445411111111111122222222222222222221           ",
    "            135554445521111111111222222222222222211             ",
    "               1455555542111111112222222222222211               ",
    "                  1345555432111111112222222111                  ",
    "                      1134454443222221111                       ",
    "                                                                ",
    "                                                                ",
)
_GLYPHS = "@%##**++"
_BASIC_COLORS = (91, 91, 95, 95, 97, 93, 93, 93)
_shown = False


def _clean(value: object) -> str:
    return " ".join(
        "".join(
            c for c in str(value) if not unicodedata.category(c).startswith("C")
        ).split()
    )


def _width(text: str) -> int:
    return sum(
        0
        if unicodedata.combining(c)
        else 2
        if unicodedata.east_asian_width(c) in {"W", "F"}
        else 1
        for c in text
    )


def _wrap(text: str, width: int) -> list[str]:
    result, row = [], ""
    for char in _clean(text):
        if _width(char) > width:
            continue
        if _width(row + char) > width:
            result.append(row)
            row = ""
        row += char
    return [*result, row] if row else result


def startup_information(role: str) -> list[str]:
    try:
        version = importlib.metadata.version("zhenxun-bot")
    except importlib.metadata.PackageNotFoundError:
        version = "unknown"
    return [
        f"ZHENXUN  v{version}",
        "State: starting",
        f"Python: {platform.python_version()}",
        f"System: {platform.system()} / {platform.machine()}",
        f"Process: {role} / PID {os.getpid()}",
        f"Started: {datetime.now().astimezone():%Y-%m-%d %H:%M:%S %z}",
    ]


def render_banner(
    columns: int, lines: int, color: str = "none", information: list[str] | None = None
) -> str:
    usable = max(0, columns - 1)
    if usable == 0 or lines <= 0:
        return ""
    information = information or ["ZHENXUN", "State: starting"]
    height = min(16, lines // 3, (columns - 2) // 2)
    width = min(64, height * 2)
    if height < 6 or usable - width - 3 < 30:
        text_rows = [row for text in information for row in _wrap(text, usable)]
        return "\n".join(text_rows[: max(1, lines - 1)]) + "\n"
    text_rows = [row for text in information for row in _wrap(text, usable - width - 3)]
    rows = []
    for y in range(height):
        row = ""
        previous = None
        for x in range(width):
            source = _GRID[min(31, int((y + 0.5) * 32 / height))]
            cell = source[min(63, int((x + 0.5) * 64 / width))]
            if cell == " ":
                row += " "
                continue
            index = int(cell) - 1
            if color != "none" and index != previous:
                if color == "truecolor":
                    red, green, blue = _PALETTE[index]
                    row += f"\x1b[38;2;{red};{green};{blue}m"
                else:
                    row += f"\x1b[{_BASIC_COLORS[index]}m"
                previous = index
            row += _GLYPHS[index]
        if previous is not None:
            row += "\x1b[0m"
        rows.append(
            (row + "   " + (text_rows[y] if y < len(text_rows) else "")).rstrip()
        )
    rows.extend(" " * (width + 3) + row for row in text_rows[height:])
    return "\n".join(rows[: max(1, lines - 1)]) + "\n"


def ready_summary(status: dict) -> str:
    counts = (status.get("load_plan") or {}).get("status_counts")
    plugins = (
        f"loaded={counts.get('loaded', 0)} failed={counts.get('failed', 0)} "
        f"pending={counts.get('pending', 0)}"
        if counts is not None
        else "not observed"
    )
    warmup = (status.get("stages") or {}).get("warmup", {}).get("state", "not_started")
    rows = [
        f"Runtime: {_clean(status.get('operating_mode', 'unknown'))} / "
        f"{_clean(status.get('state', 'unknown'))} / "
        f"{float(status.get('elapsed_ms', 0)) / 1000:.2f}s",
        f"Plugins: {plugins} | Warmup: {_clean(warmup)}",
    ]
    reasons = status.get("degraded_reasons") or []
    if reasons:
        first = reasons[0]
        rows.append(
            f"Diagnostic: {_clean(first.get('source_id', 'unknown'))} / "
            f"{_clean(first.get('diagnostic_id', 'unknown'))}"
        )
    return "\n".join(rows) + "\n"


def _enable_windows_color(stream: TextIO):
    import ctypes
    from ctypes import wintypes
    import msvcrt

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetConsoleMode.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel.GetConsoleMode.restype = wintypes.BOOL
    kernel.SetConsoleMode.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.SetConsoleMode.restype = wintypes.BOOL
    handle = msvcrt.get_osfhandle(stream.fileno())
    mode = wintypes.DWORD()
    if not kernel.GetConsoleMode(handle, ctypes.byref(mode)):
        return None
    original = mode.value
    if not kernel.SetConsoleMode(handle, original | 0x0004):
        return None

    def restore() -> None:
        if original != original | 0x0004:
            kernel.SetConsoleMode(handle, original)

    return restore


@contextmanager
def _color_mode(stream: TextIO):
    restore = None
    mode = "none"
    if "NO_COLOR" not in os.environ and os.getenv("TERM") != "dumb":
        try:
            if os.name == "nt":
                restore = _enable_windows_color(stream)
                if restore is not None:
                    mode = "truecolor"
            elif os.getenv("COLORTERM", "").lower() in {"truecolor", "24bit"}:
                mode = "truecolor"
            elif os.getenv("TERM", "").startswith(
                ("xterm", "screen", "tmux", "linux", "vt", "rxvt", "ansi")
            ):
                mode = "basic"
        except (OSError, ValueError, AttributeError):
            pass
    try:
        yield mode
    finally:
        if restore is not None:
            restore()


def show_startup_banner(role: str = "launcher") -> None:
    global _shown
    if _shown or os.getenv("ZHENXUN_STARTUP_BANNER", "").strip() == "0":
        return
    stream = sys.stdout
    try:
        if stream is None or not stream.isatty():
            return
        _shown = True
        try:
            size = os.get_terminal_size(stream.fileno())
        except (OSError, ValueError, AttributeError):
            size = shutil.get_terminal_size(fallback=(80, 24))
        with _color_mode(stream) as color:
            try:
                stream.write(
                    render_banner(
                        size.columns, size.lines, color, startup_information(role)
                    )
                )
                stream.flush()
            except (OSError, ValueError):
                if color != "none":
                    stream.write("\x1b[0m")
                    stream.flush()
    except Exception:
        # A decorative banner must never prevent the launcher from starting.
        return
