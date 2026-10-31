import asyncio
from dataclasses import dataclass
import io
import json
import posixpath
import re
import tarfile
import time
from typing import Any
import uuid

WORKDIR = "/workspace"
"""容器内的工作目录"""
OUTPUT_LIMIT = 8000
"""单路输出保留的最大字符数"""
FILE_LIMIT = 1024 * 1024
"""读取文件的最大字节数"""
SCREEN_LIMIT = 20000
"""终端缓冲区保留的最大字符数"""
_ANSI = re.compile(r"(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]")
LANGUAGES: dict[str, tuple[str, list[str]]] = {
    "python": ("py", ["python3"]),
    "bash": ("sh", ["bash"]),
    "javascript": ("js", ["node"]),
}
"""语言到（文件后缀，执行命令）的对照表"""
_LIST_SCRIPT = (
    "import json, os, sys\n"
    "items = []\n"
    "entries = sorted(os.scandir(sys.argv[1]), key=lambda e: (e.is_file(), e.name))\n"
    "for e in entries[:500]:\n"
    "    try:\n"
    "        s = e.stat(follow_symlinks=False)\n"
    "    except OSError:\n"
    "        continue\n"
    "    kind = 'link' if e.is_symlink() else 'dir' if e.is_dir() else 'file'\n"
    "    items.append([e.name, kind, s.st_size, int(s.st_mtime)])\n"
    "open(sys.argv[2], 'w').write(json.dumps(items))\n"
)
"""列目录脚本：参数为目录与结果文件，最多 500 项，目录在前"""
_STAT_SCRIPT = (
    "import os, sys\n"
    "p, limit, head = sys.argv[1], int(sys.argv[2]), sys.argv[3]\n"
    "if os.path.isdir(p):\n"
    "    raise IsADirectoryError(p)\n"
    "if not os.path.isfile(p):\n"
    "    raise FileNotFoundError(p)\n"
    "if os.path.getsize(p) > limit:\n"
    "    with open(p, 'rb') as src, open(head, 'wb') as dst:\n"
    "        dst.write(src.read(limit))\n"
    "    print(head)\n"
    "else:\n"
    "    print(p)\n"
)
"""读文件前的检查脚本：参数为文件、字节上限与截断副本路径，超过上限时复制开头部分，
输出实际要读取的文件"""
_KEEP_BYTES = OUTPUT_LIMIT * 4
"""收集命令输出时首尾各保留的字节数，超出部分在读取时丢弃"""
_ALIASES = {
    "py": "python",
    "python3": "python",
    "sh": "bash",
    "shell": "bash",
    "js": "javascript",
    "node": "javascript",
    "nodejs": "javascript",
}


def normalize_language(language: str) -> str | None:
    """规范化语言名称

    参数:
        language: 模型给出的语言名称

    返回:
        str | None: 支持的语言名称，不支持时为 None
    """
    name = language.strip().lower()
    name = _ALIASES.get(name, name)
    return name if name in LANGUAGES else None


def resolve_path(path: str) -> str:
    """把相对路径解析到工作目录下

    参数:
        path: 容器内路径，相对路径以工作目录为基准

    返回:
        str: 规范化后的绝对路径
    """
    return posixpath.normpath(posixpath.join(WORKDIR, path.replace("\\", "/")))


def _append(buf: bytearray, data: bytes):
    """追加命令输出，超过首尾保留量时丢弃中间部分"""
    buf.extend(data)
    if len(buf) > _KEEP_BYTES * 2 + 1024:
        buf[_KEEP_BYTES : len(buf) - _KEEP_BYTES] = b"\n...\n"


def _clip(text: str, limit: int = OUTPUT_LIMIT) -> str:
    """截去过长文本的中间部分"""
    if len(text) <= limit:
        return text
    half = limit // 2
    return f"{text[:half]}\n...（省略 {len(text) - limit} 字符）...\n{text[-half:]}"


@dataclass
class ExecResult:
    """容器内命令的执行结果"""

    stdout: str = ""
    stderr: str = ""
    exit_code: int = -1
    timeout: bool = False

    def text(self) -> str:
        """整理为交给模型的文本

        返回:
            str: 退出码与输出
        """
        parts = [f"退出码: {self.exit_code}"]
        if self.timeout:
            parts.append("执行超时，进程已被终止。")
        if self.stdout:
            parts.append(f"标准输出:\n{self.stdout}")
        if self.stderr:
            parts.append(f"标准错误:\n{self.stderr}")
        if not self.stdout and not self.stderr:
            parts.append("（无输出）")
        return "\n".join(parts)


class Terminal:
    """容器内的交互终端"""

    def __init__(self, container: Any):
        self.container = container
        self.buffer = ""
        self.last_input = time.time()
        self._pid_file = f"/tmp/.zx_term_{uuid.uuid4().hex[:12]}.pid"
        self._stream: Any = None
        self._task: asyncio.Task | None = None

    async def start(self, command: str):
        """启动终端并运行命令

        参数:
            command: shell 命令
        """
        script = f'echo $$ > {self._pid_file}; exec sh -c "$1"'
        exec_inst = await self.container.exec(
            cmd=["sh", "-c", script, "sh", command],
            tty=True,
            stdin=True,
            stdout=True,
            stderr=True,
            workdir=WORKDIR,
        )
        self._stream = exec_inst.start(detach=False)
        await self._stream.__aenter__()
        self._task = asyncio.create_task(self._read_loop())

    async def _read_loop(self):
        """持续读取终端输出到缓冲区"""
        try:
            while self._stream:
                msg = await self._stream.read_out()
                if msg is None:
                    break
                self.buffer += _ANSI.sub("", msg.data.decode("utf-8", "replace"))
                if len(self.buffer) > SCREEN_LIMIT:
                    self.buffer = self.buffer[-SCREEN_LIMIT:]
        except Exception:
            pass

    @property
    def running(self) -> bool:
        """终端中的程序是否仍在运行"""
        return self._task is not None and not self._task.done()

    def screen(self, lines: int = 50) -> str:
        """读取终端最近的输出

        参数:
            lines: 保留的行数

        返回:
            str: 终端画面
        """
        text = "\n".join(self.buffer.split("\n")[-lines:]).strip()
        return text if self.running else f"{text}\n（程序已结束）".strip()

    async def send(self, text: str):
        """向终端写入输入

        参数:
            text: 输入内容
        """
        self.last_input = time.time()
        if self._stream:
            await self._stream.write_in(text.encode("utf-8"))

    async def interrupt(self):
        """向终端发送 Ctrl+C"""
        await self.send("\x03")

    async def close(self):
        """关闭终端并结束其中的进程组（断开连接不会结束 exec 进程）"""
        running = self.running
        if self._task:
            self._task.cancel()
        if self._stream:
            stream, self._stream = self._stream, None
            try:
                await stream.close()
            except Exception:
                pass
        if not running:
            return
        script = (
            f"p=$(cat {self._pid_file} 2>/dev/null) && "
            f'{{ kill -9 -"$p" || kill -9 "$p"; }} 2>/dev/null; rm -f {self._pid_file}'
        )
        try:
            exec_inst = await self.container.exec(cmd=["sh", "-c", script])
            await asyncio.wait_for(exec_inst.start(detach=True), 10)
        except Exception:
            pass


class DockerSandbox:
    """一个会话独占的沙箱容器"""

    def __init__(self, key: str, container: Any, image: str):
        self.key = key
        self.container = container
        self.image = image
        self.created_at = time.time()
        self.last_active = self.created_at
        self.terminal: Terminal | None = None
        self.running = 0
        """正在执行的命令数"""

    def busy(self, idle: float) -> bool:
        """是否仍在使用：有命令在执行，或 idle 秒内有过操作或终端输入

        参数:
            idle: 闲置秒数

        返回:
            bool: 是否在使用
        """
        last = max(self.last_active, self.terminal.last_input if self.terminal else 0)
        return self.running > 0 or time.time() - last < idle

    async def alive(self) -> bool:
        """容器是否在运行

        返回:
            bool: 是否在运行
        """
        try:
            info = await self.container.show()
        except Exception:
            return False
        return bool(info.get("State", {}).get("Running"))

    async def exec(
        self, command: list[str], timeout: float, workdir: str = WORKDIR
    ) -> ExecResult:
        """在容器中执行命令，超时后终止进程

        参数:
            command: 命令参数列表
            timeout: 超时秒数
            workdir: 工作目录

        返回:
            ExecResult: 执行结果
        """
        self.last_active = time.time()
        seconds = max(1, int(timeout))
        exec_inst = await self.container.exec(
            cmd=["timeout", "-k", "3", str(seconds), *command],
            stdout=True,
            stderr=True,
            workdir=workdir,
        )
        out, err = bytearray(), bytearray()

        async def read():
            async with exec_inst.start(detach=False) as stream:
                while (msg := await stream.read_out()) is not None:
                    _append(out if msg.stream == 1 else err, msg.data)

        timed_out = False
        self.running += 1
        try:
            await asyncio.wait_for(read(), timeout=seconds + 10)
        except asyncio.TimeoutError:
            timed_out = True
        finally:
            self.running -= 1
            self.last_active = time.time()
        info = await exec_inst.inspect()
        code = info.get("ExitCode")
        code = -1 if code is None else int(code)
        return ExecResult(
            stdout=_clip(out.decode("utf-8", "replace").strip()),
            stderr=_clip(err.decode("utf-8", "replace").strip()),
            exit_code=code,
            timeout=timed_out or code in (124, 137),
        )

    async def run_code(self, language: str, code: str, timeout: float) -> ExecResult:
        """把代码写入临时文件后执行

        参数:
            language: 已规范化的语言名称
            code: 代码
            timeout: 超时秒数

        返回:
            ExecResult: 执行结果
        """
        suffix, command = LANGUAGES[language]
        path = f"/tmp/zx_run/{uuid.uuid4().hex[:12]}.{suffix}"
        await self.write_file(path, code.encode("utf-8"))
        return await self.exec([*command, path], timeout)

    async def read_file(self, path: str, checked: bool = False) -> bytes:
        """读取容器内文件，先在容器内检查类型与大小，超过上限的文件只读取开头

        参数:
            path: 文件路径
            checked: 已确认是不超过上限的文件，跳过检查

        返回:
            bytes: 文件内容，最多 FILE_LIMIT 字节

        异常:
            IsADirectoryError: 路径是目录
            FileNotFoundError: 文件不存在
        """
        self.last_active = time.time()
        target = resolve_path(path)
        if not checked:
            head = f"/tmp/.zx_head_{uuid.uuid4().hex[:12]}"
            result = await self.exec(
                ["python3", "-c", _STAT_SCRIPT, target, str(FILE_LIMIT), head], 30
            )
            if result.exit_code != 0:
                error = result.stderr.splitlines()[-1] if result.stderr else path
                if "IsADirectoryError" in error:
                    raise IsADirectoryError(path)
                raise FileNotFoundError(error)
            target = result.stdout.strip() or target
        tar = await self.container.get_archive(target)
        members = tar.getmembers()
        member = members[0] if members else None
        if member is None or not member.isfile():
            raise IsADirectoryError(path)
        file = tar.extractfile(member)
        return file.read(FILE_LIMIT) if file else b""

    async def list_dir(self, path: str) -> list[dict[str, Any]]:
        """列出容器内目录

        参数:
            path: 目录路径

        返回:
            list[dict[str, Any]]: 条目（name、type 为 dir / file / link、size、mtime），
                目录在前，最多 500 项
        """
        output = f"/tmp/.zx_list_{uuid.uuid4().hex[:12]}.json"
        result = await self.exec(
            ["python3", "-c", _LIST_SCRIPT, resolve_path(path), output], 15
        )
        if result.exit_code != 0:
            error = result.stderr.splitlines()[-1] if result.stderr else path
            if "NotADirectoryError" in error:
                raise NotADirectoryError(path)
            raise FileNotFoundError(error)
        items = json.loads(await self.read_file(output, checked=True) or b"[]")
        return [
            {"name": name, "type": kind, "size": size, "mtime": mtime}
            for name, kind, size, mtime in items
        ]

    async def write_file(self, path: str, data: bytes):
        """写入容器内文件，自动创建上级目录

        参数:
            path: 文件路径
            data: 文件内容
        """
        target = resolve_path(path)
        folder, name = posixpath.split(target)
        result = await self.exec(["mkdir", "-p", folder], 30)
        if result.exit_code != 0:
            raise OSError(result.stderr or f"无法创建目录 {folder}")
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            info.mtime = int(time.time())
            tar.addfile(info, io.BytesIO(data))
        await self.container.put_archive(folder, buf.getvalue())

    async def open_terminal(self, command: str) -> Terminal:
        """关闭旧终端并启动新的交互终端

        参数:
            command: shell 命令

        返回:
            Terminal: 新终端
        """
        self.last_active = time.time()
        if self.terminal:
            await self.terminal.close()
        self.terminal = Terminal(self.container)
        await self.terminal.start(command)
        return self.terminal

    async def close(self):
        """关闭终端并删除容器"""
        if self.terminal:
            await self.terminal.close()
            self.terminal = None
        try:
            await self.container.delete(force=True)
        except Exception:
            pass
