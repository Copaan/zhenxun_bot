import asyncio
import hashlib
import time
from typing import Any, ClassVar

from zhenxun.services.ai.config import get_llm_config
from zhenxun.services.ai.config.models import SandboxSettings
from zhenxun.services.ai.utils.logger import log_sandbox as logger
from zhenxun.utils.lifespan import LifespanManager

from .docker import DockerSandbox

LABEL = "zhenxun_component"
"""标记沙箱容器的标签名"""
TEST_CODE = (
    "import platform, sys; " "print('ok', sys.version.split()[0], platform.machine())"
)
"""测试运行使用的代码"""


class SandboxError(Exception):
    """沙箱不可用或操作失败"""


def _docker_error(e: Exception) -> str:
    """提取 Docker 异常中的可读信息"""
    message = getattr(e, "message", None) or str(e) or type(e).__name__
    return str(message).strip()[:300]


class SandboxManager:
    """Agent 代码沙箱管理：Docker 检测、镜像安装、会话容器与闲置清理"""

    _client: ClassVar[Any] = None
    _sessions: ClassVar[dict[str, DockerSandbox]] = {}
    _locks: ClassVar[dict[str, asyncio.Lock]] = {}
    _lifespan: ClassVar[LifespanManager | None] = None
    _ready: ClassVar[tuple[float, str, bool, str]] = (0.0, "", False, "")
    _pull: ClassVar[dict[str, Any]] = {"status": "idle"}
    _pull_task: ClassVar[asyncio.Task | None] = None

    @classmethod
    def settings(cls) -> SandboxSettings:
        """当前沙箱配置

        返回:
            SandboxSettings: 沙箱配置
        """
        return get_llm_config().sandbox

    @classmethod
    async def client(cls) -> Any:
        """获取可用的 Docker 客户端

        返回:
            Any: aiodocker.Docker
        """
        if cls._client is not None:
            try:
                await asyncio.wait_for(cls._client.version(), timeout=5)
                return cls._client
            except Exception:
                await cls._close_client()
        import aiodocker

        try:
            client = aiodocker.Docker()
        except Exception as e:
            raise SandboxError(f"未找到 Docker：{_docker_error(e)}") from e
        try:
            await asyncio.wait_for(client.version(), timeout=5)
        except Exception as e:
            await client.close()
            raise SandboxError(f"无法连接 Docker：{_docker_error(e)}") from e
        cls._client = client
        for sandbox in cls._sessions.values():
            sandbox.container = client.containers.container(sandbox.container.id)
        return client

    @classmethod
    async def _close_client(cls):
        """关闭 Docker 客户端"""
        client, cls._client = cls._client, None
        if client is not None:
            try:
                await client.close()
            except Exception:
                pass

    @classmethod
    async def _image(cls, client: Any, image: str) -> dict[str, Any] | None:
        """查询本地镜像

        参数:
            client: Docker 客户端
            image: 镜像名

        返回:
            dict | None: 镜像大小与创建时间，未安装时为 None
        """
        try:
            info = await client.images.inspect(image)
        except Exception as e:
            if getattr(e, "status", None) == 404:
                return None
            raise
        return {"size": info.get("Size", 0), "created": info.get("Created", "")}

    @classmethod
    async def status(cls) -> dict[str, Any]:
        """沙箱状态：Docker、镜像、安装进度与运行中的会话

        返回:
            dict[str, Any]: 状态信息
        """
        settings = cls.settings()
        data: dict[str, Any] = {
            "enabled": settings.enable_sandbox,
            "docker": {"ok": False, "version": "", "error": ""},
            "image": {"name": settings.docker_image, "installed": False},
            "pull": dict(cls._pull),
            "sessions": await cls.sessions(),
        }
        try:
            client = await cls.client()
            version = await client.version()
            data["docker"].update(
                ok=True,
                version=version.get("Version", ""),
                os=f"{version.get('Os', '')}/{version.get('Arch', '')}",
            )
            image = await cls._image(client, settings.docker_image)
            if image:
                data["image"].update(installed=True, **image)
        except Exception as e:
            data["docker"]["error"] = _docker_error(e)
        ready, reason = cls._judge(data)
        cls._ready = (time.time(), settings.docker_image, ready, reason)
        data.update(ready=ready, reason=reason)
        return data

    @classmethod
    def _judge(cls, data: dict[str, Any]) -> tuple[bool, str]:
        """根据状态判断沙箱能否使用"""
        if not data["enabled"]:
            return False, "沙箱未启用"
        if not data["docker"]["ok"]:
            return False, "Docker 不可用"
        if not data["image"]["installed"]:
            return False, f"镜像 {data['image']['name']} 未安装"
        return True, ""

    @classmethod
    async def ready(cls) -> tuple[bool, str]:
        """沙箱能否使用，结果缓存 60 秒

        返回:
            tuple[bool, str]: 是否可用，不可用的原因
        """
        settings = cls.settings()
        if not settings.enable_sandbox:
            return False, "沙箱未启用"
        checked, image, ready, reason = cls._ready
        if image == settings.docker_image and time.time() - checked < 60:
            return ready, reason
        data = await cls.status()
        return data["ready"], data["reason"]

    @classmethod
    def pull(cls) -> bool:
        """在后台拉取配置的镜像

        返回:
            bool: 是否开始拉取，已在拉取时为 False
        """
        if cls._pull_task and not cls._pull_task.done():
            return False
        image = cls.settings().docker_image
        cls._pull = {"status": "running", "image": image, "message": "准备拉取"}
        cls._pull_task = asyncio.create_task(cls._run_pull(image))
        return True

    @classmethod
    async def _run_pull(cls, image: str):
        """拉取镜像并记录进度

        参数:
            image: 镜像名
        """
        layers: dict[str, tuple[int, int]] = {}
        started = time.time()
        try:
            client = await cls.client()
            async for item in client.images.pull(image, stream=True):
                if item.get("error"):
                    raise SandboxError(item["error"])
                detail = item.get("progressDetail") or {}
                if item.get("id") and detail.get("total"):
                    layers[item["id"]] = (detail.get("current", 0), detail["total"])
                elif item.get("id") and item.get("status") in {
                    "Pull complete",
                    "Already exists",
                }:
                    total = layers.get(item["id"], (0, 0))[1]
                    layers[item["id"]] = (total, total)
                current = sum(c for c, _ in layers.values())
                total = sum(t for _, t in layers.values())
                cls._pull.update(
                    message=item.get("status", ""),
                    current=current,
                    total=total,
                    layers=len(layers),
                )
            cls._pull.update(status="done", message="安装完成")
            logger.info(f"沙箱镜像 {image} 拉取完成，用时 {time.time() - started:.0f}s")
        except Exception as e:
            cls._pull.update(status="error", message=_docker_error(e))
            logger.warning(f"沙箱镜像 {image} 拉取失败: {_docker_error(e)}")
        finally:
            cls._pull["duration"] = round(time.time() - started, 1)
            cls._ready = (0.0, "", False, "")

    @classmethod
    def _container_config(cls, settings: SandboxSettings, key: str) -> dict[str, Any]:
        """生成沙箱容器配置：资源上限、网络开关，不带宿主环境变量与挂载

        参数:
            settings: 沙箱配置
            key: 会话标识

        返回:
            dict[str, Any]: 容器创建参数
        """
        memory = settings.memory_mb * 1024 * 1024
        return {
            "Image": settings.docker_image,
            "Cmd": ["sleep", "2147483647"],
            "WorkingDir": "/workspace",
            "Env": ["HOME=/workspace", "LANG=C.UTF-8", "PYTHONUNBUFFERED=1"],
            "Labels": {LABEL: "sandbox", "zhenxun_session": key[:200]},
            "HostConfig": {
                "NanoCpus": int(settings.cpus * 1e9),
                "Memory": memory,
                "MemorySwap": memory,
                "PidsLimit": 256,
                "NetworkMode": "bridge" if settings.network else "none",
                "SecurityOpt": ["no-new-privileges"],
                "CapDrop": ["NET_RAW", "MKNOD"],
                "Init": True,
            },
        }

    @classmethod
    async def _create(cls, key: str) -> DockerSandbox:
        """为会话创建并启动容器

        参数:
            key: 会话标识

        返回:
            DockerSandbox: 沙箱
        """
        settings = cls.settings()
        client = await cls.client()
        if not await cls._image(client, settings.docker_image):
            raise SandboxError(f"镜像 {settings.docker_image} 未安装")
        digest = hashlib.sha1(key.encode()).hexdigest()[:12]
        name = f"zx-sandbox-{digest}-{int(time.time())}"
        try:
            container = await client.containers.run(
                config=cls._container_config(settings, key), name=name
            )
        except Exception as e:
            raise SandboxError(f"创建沙箱容器失败：{_docker_error(e)}") from e
        logger.info(f"已为会话 {key} 创建沙箱容器 {name}")
        return DockerSandbox(key, container, settings.docker_image)

    @classmethod
    async def get(cls, key: str) -> DockerSandbox:
        """获取会话的沙箱，不存在或已停止时新建

        参数:
            key: 会话标识（会话 id 已包含 Bot、群和用户）

        返回:
            DockerSandbox: 沙箱
        """
        ready, reason = await cls.ready()
        if not ready:
            raise SandboxError(reason)
        lock = cls._locks.setdefault(key, asyncio.Lock())
        async with lock:
            sandbox = cls._sessions.get(key)
            if sandbox and not await sandbox.alive():
                await cls.close(key)
                sandbox = None
            if sandbox is None:
                sandbox = await cls._create(key)
                cls._sessions[key] = sandbox
            sandbox.last_active = time.time()
            await cls._keep(key)
            return sandbox

    @classmethod
    def find(cls, key: str) -> DockerSandbox | None:
        """已存在的会话沙箱，不会新建

        参数:
            key: 会话标识

        返回:
            DockerSandbox | None: 沙箱
        """
        return cls._sessions.get(key)

    @classmethod
    async def _keep(cls, key: str):
        """登记或刷新会话的闲置清理时间，到期时仍在使用则续期"""
        ttl = float(cls.settings().cleanup_timeout)
        if cls._lifespan is None:
            cls._lifespan = LifespanManager()

        def busy() -> bool:
            sandbox = cls._sessions.get(key)
            return sandbox is not None and sandbox.busy(ttl)

        await cls._lifespan.register(
            key, ttl=ttl, cleanup_callback=cls.close, is_busy_callback=busy
        )

    @classmethod
    async def sessions(cls) -> list[dict[str, Any]]:
        """运行中的会话沙箱

        返回:
            list[dict[str, Any]]: 会话、镜像、创建与最近使用时间
        """
        return [
            {
                "key": key,
                "image": sandbox.image,
                "created_at": int(sandbox.created_at),
                "last_active": int(sandbox.last_active),
                "terminal": bool(sandbox.terminal and sandbox.terminal.running),
            }
            for key, sandbox in cls._sessions.items()
        ]

    @classmethod
    async def close(cls, key: str) -> bool:
        """删除会话的沙箱容器

        参数:
            key: 会话标识

        返回:
            bool: 是否存在并已删除
        """
        sandbox = cls._sessions.pop(key, None)
        if (lock := cls._locks.get(key)) and not lock.locked():
            del cls._locks[key]
        if cls._lifespan is not None:
            await cls._lifespan.unregister(key)
        if sandbox is None:
            return False
        await sandbox.close()
        logger.info(f"已删除会话 {key} 的沙箱容器")
        return True

    @classmethod
    async def close_all(cls) -> int:
        """删除全部会话沙箱

        返回:
            int: 删除的数量
        """
        keys = list(cls._sessions)
        for key in keys:
            await cls.close(key)
        return len(keys)

    @classmethod
    async def test(cls) -> dict[str, Any]:
        """用临时容器按当前配置运行一段 Python 代码

        返回:
            dict[str, Any]: 是否成功、输出与耗时
        """
        started = time.time()
        sandbox = None
        try:
            sandbox = await cls._create("test")
            result = await sandbox.run_code("python", TEST_CODE, 30)
            ok = result.exit_code == 0 and result.stdout.startswith("ok")
            output = result.text()
        except Exception as e:
            ok, output = False, _docker_error(e)
        finally:
            if sandbox:
                await sandbox.close()
        return {"ok": ok, "output": output, "duration": round(time.time() - started, 2)}

    @classmethod
    async def prune(cls):
        """删除上次运行遗留的沙箱容器"""
        try:
            client = await cls.client()
            containers = await client.containers.list(
                all=True, filters={"label": [f"{LABEL}=sandbox"]}
            )
        except Exception:
            return
        for container in containers:
            try:
                await container.delete(force=True)
            except Exception:
                pass
        if containers:
            logger.info(f"已清理 {len(containers)} 个遗留的沙箱容器")

    @classmethod
    async def shutdown(cls):
        """关闭时删除全部沙箱容器并断开 Docker"""
        await cls.close_all()
        if cls._pull_task and not cls._pull_task.done():
            cls._pull_task.cancel()
        if cls._lifespan is not None:
            await cls._lifespan.stop()
            cls._lifespan = None
        await cls._close_client()
