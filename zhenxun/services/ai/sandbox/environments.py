from abc import ABC, abstractmethod
import shlex
from typing import TYPE_CHECKING, Any, ClassVar

from zhenxun.services.ai.utils.logger import log_sandbox as logger

if TYPE_CHECKING:
    from .drivers.base import BaseSandboxSession
    from .models import SandboxBlueprint


class BaseProvisioner(ABC):
    """环境配置器基类，负责在沙箱内热安装依赖"""

    @property
    @abstractmethod
    def name(self) -> str:
        """配置器名称"""
        pass

    @abstractmethod
    async def install(self, session: "BaseSandboxSession", blueprint: Any) -> bool:
        """执行安装逻辑，成功返回 True"""
        pass

    @abstractmethod
    async def scan_and_setup_workspace(
        self, session: "BaseSandboxSession", workspace_dir: str
    ) -> bool:
        """扫描指定工作区（检测文件如 requirements.txt）并自动配置环境"""
        pass


class ProvisionerRegistry:
    """环境配置器注册中心"""

    _provisioners: ClassVar[dict[str, BaseProvisioner]] = {}

    @classmethod
    def register(cls, provisioner: BaseProvisioner) -> None:
        """向注册中心注册一个环境配置器"""
        if provisioner.name in cls._provisioners:
            logger.warning(
                f"[ProvisionerRegistry] 覆盖已存在的配置器: {provisioner.name}"
            )
        cls._provisioners[provisioner.name] = provisioner
        logger.debug(f"[ProvisionerRegistry] 成功注册环境配置器: {provisioner.name}")

    @classmethod
    def get(cls, name: str) -> BaseProvisioner | None:
        """获取指定名称的环境配置器"""
        return cls._provisioners.get(name)

    @classmethod
    def get_all(cls) -> dict[str, BaseProvisioner]:
        """获取所有已注册的环境配置器列表"""
        return cls._provisioners.copy()


class UnifiedManifestProvisioner(BaseProvisioner):
    """
    统一环境清单装配器。
    单向执行：系统依赖(apt) -> Python依赖(uv) -> 二进制检查(bins) -> 自定义脚本。
    """

    @property
    def name(self) -> str:
        """返回统一环境清单装配器的名称"""
        return "unified_manifest"

    async def install(
        self, session: "BaseSandboxSession", blueprint: "SandboxBlueprint"
    ) -> bool:
        """根据 SandboxBlueprint 环境清单指纹装配沙箱依赖环境"""
        target_hash = blueprint.calculate_hash()

        if session.get_meta("env_hash") == target_hash:
            return True

        hash_file = shlex.quote(f"{session.workspace_path}/.zx_env_hash")
        hash_check = await session.run_process(f"cat {hash_file}")
        if hash_check.exit_code == 0 and hash_check.stdout.strip() == target_hash:
            logger.debug(
                f"[Sandbox] ⚡ 环境指纹 ({target_hash[:8]}) "
                "匹配成功，跳过所有依赖安装环节！"
            )
            session._meta["env_hash"] = target_hash
            return True

        for step in blueprint.setup_steps:
            await step.apply(session)

        await session.run_process(f"echo {shlex.quote(target_hash)} > {hash_file}")
        session._meta["env_hash"] = target_hash
        logger.debug(f"[Sandbox] 环境装配完毕，已写入指纹快照: {target_hash[:8]}")

        return True

    async def scan_and_setup_workspace(
        self, session: "BaseSandboxSession", workspace_dir: str
    ) -> bool:
        """扫描项目工作区，根据 requirements.txt 或 package.json 自动安装所需依赖"""
        directory = shlex.quote(workspace_dir)
        ok = True
        check = await session.run_process(f"test -f {directory}/requirements.txt")
        if check.exit_code == 0:
            check_uv = await session.run_process("command -v uv")
            res = await session.run_process(
                "uv pip install -r requirements.txt"
                if check_uv.exit_code == 0
                else "pip install -q -r requirements.txt",
                cwd=workspace_dir,
                timeout=300,
            )
            if res.exit_code != 0:
                logger.warning(
                    f"[UnifiedProvisioner] Python 依赖安装失败: {res.stderr}"
                )
                ok = False

        check_npm = await session.run_process(f"test -f {directory}/package.json")
        if check_npm.exit_code == 0:
            logger.info("[UnifiedProvisioner] 发现 package.json，正在安装 npm 依赖...")
            res = await session.run_process(
                "npm install --no-fund --no-audit --loglevel=error",
                cwd=workspace_dir,
                timeout=300,
            )
            if res.exit_code != 0:
                logger.warning(f"[UnifiedProvisioner] npm 依赖安装失败: {res.stderr}")
                ok = False
        return ok


ProvisionerRegistry.register(UnifiedManifestProvisioner())
