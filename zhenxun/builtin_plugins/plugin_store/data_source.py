import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
import os
from pathlib import Path
import shutil
import tempfile
import time
from typing import Any, ClassVar

import ujson as json

from zhenxun.builtin_plugins.plugin_store.models import StorePluginInfo
from zhenxun.models.plugin_info import PluginInfo
from zhenxun.plugin_store_coordinator import coordinated_store_operation
from zhenxun.services.cache.bounded_ttl import BoundedTTLCache
from zhenxun.services.log import logger
from zhenxun.services.plugin_init import PluginInitManager
from zhenxun.utils.enum import PluginType
from zhenxun.utils.image_utils import BuildImage, ImageTemplate, RowStyle
from zhenxun.utils.manager.virtual_env_package_manager import VirtualEnvPackageManager
from zhenxun.utils.repo_utils import RepoFileManager
from zhenxun.utils.repo_utils.models import RepoFileInfo, RepoType
from zhenxun.utils.utils import is_number, win_on_rm_error

from .config import (
    BASE_PATH,
    DEFAULT_GITHUB_URL,
    EXTRA_GITHUB_URL,
    LOG_COMMAND,
)
from .exceptions import PluginStoreException

_PLUGIN_STORE_DATA_CACHE = BoundedTTLCache[
    str, tuple[list[StorePluginInfo], list[StorePluginInfo]]
](
    "PLUGIN_STORE_DATA",
    ttl_seconds=5 * 60,
    max_items=1,
)
_PLUGIN_STORE_DATA_LOCK = asyncio.Lock()
_PLUGIN_STORE_GENERATION = 0


@dataclass(frozen=True, slots=True)
class StoreInstallResult:
    dependency_plan: dict[str, Any] = field(default_factory=dict)

    @property
    def dependency_changes(self) -> bool:
        changes = self.dependency_plan.get("package_changes", {})
        return bool(changes.get("added") or changes.get("changed"))

    @property
    def source_build_required(self) -> bool:
        return bool(self.dependency_plan.get("source_build_required"))


def row_style(column: str, text: str) -> RowStyle:
    """被动技能文本风格

    参数:
        column: 表头
        text: 文本内容

    返回:
        RowStyle: RowStyle
    """
    style = RowStyle()
    if column == "-" and text == "已安装":
        style.font_color = "#67C23A"
    return style


class StoreManager:
    _catalog_warnings: ClassVar[list[dict[str, Any]]] = []
    _catalog_health: ClassVar[dict[str, dict[str, Any]]] = {}
    _catalog_health_checked_at: ClassVar[float] = 0.0
    _SOURCE_NAMES: ClassVar[dict[RepoType, str]] = {
        RepoType.ALIYUN: "阿里云",
        RepoType.GITHUB: "GitHub",
    }
    _BINARY_EXTENSIONS: ClassVar[frozenset[str]] = frozenset(
        {
            ".7z",
            ".avi",
            ".bin",
            ".bmp",
            ".class",
            ".dat",
            ".db",
            ".dll",
            ".doc",
            ".docx",
            ".dylib",
            ".eot",
            ".exe",
            ".flv",
            ".gif",
            ".gz",
            ".ico",
            ".jpeg",
            ".jpg",
            ".mov",
            ".mp3",
            ".mp4",
            ".otf",
            ".pdf",
            ".png",
            ".ppt",
            ".pptx",
            ".pyc",
            ".rar",
            ".so",
            ".svg",
            ".tar",
            ".tif",
            ".tiff",
            ".ttf",
            ".webp",
            ".wmv",
            ".woff",
            ".woff2",
            ".xls",
            ".xlsx",
            ".xz",
            ".zip",
        }
    )

    @classmethod
    def _resolve_local_plugin_path(
        cls, plugin_info: StorePluginInfo, *, is_external: bool
    ) -> Path:
        """将商店插件信息映射到本地插件文件/目录路径。"""
        module_parts = plugin_info.module_path.replace("/", ".").split(".")
        plugin_name = next(
            (part for part in reversed(module_parts) if part and part.isidentifier()),
            plugin_info.module,
        )

        if plugin_info.is_dir:
            return BASE_PATH / "plugins" / plugin_name

        return BASE_PATH / "plugins" / f"{plugin_name}.py"

    @classmethod
    async def get_data(
        cls, *, refresh: bool = False
    ) -> tuple[list[StorePluginInfo], list[StorePluginInfo]]:
        """获取插件信息数据

        返回:
            tuple[list[StorePluginInfo], list[StorePluginInfo]]:
                原生插件信息数据，第三方插件信息数据
        """
        global _PLUGIN_STORE_GENERATION

        cache_key = "plugins_json"
        requested_generation = _PLUGIN_STORE_GENERATION
        warnings: list[dict[str, Any]] = []

        def parse_entries(content: str, source: str) -> list[StorePluginInfo]:
            try:
                raw_entries = json.loads(content)
            except Exception as error:
                raise PluginStoreException(f"{source}_catalog_invalid") from error
            if not isinstance(raw_entries, list):
                raise PluginStoreException(f"{source}_catalog_invalid")
            entries: list[StorePluginInfo] = []
            for index, raw in enumerate(raw_entries):
                try:
                    entries.append(StorePluginInfo(**raw))
                except Exception as error:
                    warnings.append(
                        {
                            "source": source,
                            "index": index,
                            "code": "catalog_entry_invalid",
                            "error_type": type(error).__name__,
                        }
                    )
            return entries

        if not refresh:
            cached_data = await _PLUGIN_STORE_DATA_CACHE.get(cache_key)
            if cached_data is not None:
                return deepcopy(cached_data)

        async with _PLUGIN_STORE_DATA_LOCK:
            cached_data = await _PLUGIN_STORE_DATA_CACHE.get(cache_key)
            if cached_data is not None and (
                not refresh or _PLUGIN_STORE_GENERATION != requested_generation
            ):
                return deepcopy(cached_data)

            plugins = await RepoFileManager.get_text_content(
                DEFAULT_GITHUB_URL, "plugins.json"
            )
            extra_plugins = await RepoFileManager.get_text_content(
                EXTRA_GITHUB_URL, "plugins.json", "index"
            )
            result = (
                parse_entries(plugins, "official"),
                parse_entries(extra_plugins, "community"),
            )
            cls._catalog_warnings = warnings
            await _PLUGIN_STORE_DATA_CACHE.set(cache_key, result)
            _PLUGIN_STORE_GENERATION += 1
            return deepcopy(result)

    @classmethod
    async def invalidate_cache(cls) -> None:
        global _PLUGIN_STORE_GENERATION

        async with _PLUGIN_STORE_DATA_LOCK:
            await _PLUGIN_STORE_DATA_CACHE.delete("plugins_json")
            _PLUGIN_STORE_GENERATION += 1
            cls._catalog_health = {}
            cls._catalog_health_checked_at = 0.0

    @classmethod
    async def catalog_health(
        cls, plugins: list[StorePluginInfo], *, refresh: bool = False
    ) -> dict[str, dict[str, Any]]:
        now = time.monotonic()
        if (
            not refresh
            and cls._catalog_health
            and now - cls._catalog_health_checked_at < 60
        ):
            return cls._catalog_health.copy()
        source_files: list[set[str]] = []
        source_errors: list[str] = []
        for repo_type in (RepoType.ALIYUN, RepoType.GITHUB):
            try:
                files = await RepoFileManager.list_directory_files(
                    DEFAULT_GITHUB_URL,
                    "",
                    "main",
                    repo_type=repo_type,
                )
            except Exception as error:
                source_errors.append(type(error).__name__)
                continue
            source_files.append(
                {Path(file.path).as_posix() for file in files if not file.is_dir}
            )
        result = {}
        for plugin in plugins:
            if plugin.github_url not in {None, DEFAULT_GITHUB_URL} or plugin.ali_url:
                result[plugin.module] = {"status": "unknown", "reason": None}
                continue
            module_path = plugin.module_path.replace(".", "/").strip("/")
            expected = (
                f"{module_path}/__init__.py" if plugin.is_dir else f"{module_path}.py"
            )
            available = any(expected in files for files in source_files)
            all_sources_checked = len(source_files) == 2
            result[plugin.module] = {
                "status": (
                    "available"
                    if available
                    else "missing"
                    if all_sources_checked
                    else "unknown"
                ),
                "reason": (
                    None
                    if available
                    else "catalog_source_missing"
                    if all_sources_checked
                    else "catalog_health_unavailable"
                ),
                "expected_path": expected,
                "failed_sources": source_errors,
            }
        cls._catalog_health = result
        cls._catalog_health_checked_at = now
        return result.copy()

    @classmethod
    def version_check(cls, plugin_info: StorePluginInfo, suc_plugin: dict[str, str]):
        """版本检查

        参数:
            plugin_info: StorePluginInfo
            suc_plugin: 模块名: 版本号

        返回:
            str: 版本号
        """
        module = plugin_info.module
        if suc_plugin.get(module) and not cls.check_version_is_new(
            plugin_info, suc_plugin
        ):
            return f"{suc_plugin[module]} (有更新->{plugin_info.version})"
        return plugin_info.version

    @classmethod
    def check_version_is_new(
        cls, plugin_info: StorePluginInfo, suc_plugin: dict[str, str]
    ):
        """检查版本是否有更新

        参数:
            plugin_info: StorePluginInfo
            suc_plugin: 模块名: 版本号

        返回:
            bool: 是否有更新
        """
        module = plugin_info.module
        return suc_plugin.get(module) and plugin_info.version == suc_plugin[module]

    @classmethod
    async def get_installed_plugins(cls) -> dict[str, str]:
        """获取已安装插件的模块与版本。

        返回:
            dict[str, str]: 模块 -> 版本
        """
        db_plugin_list = await PluginInfo.get_plugins_values_list(
            "module", "version", load_status=True, filter_parent=False
        )
        return {p[0]: (p[1] or "0.1") for p in db_plugin_list}

    @classmethod
    async def get_plugins_info(cls) -> list[BuildImage] | str:
        """插件列表

        返回:
            BuildImage | str: 返回消息
        """
        plugin_list, extra_plugin_list = await cls.get_data()
        column_name = ["-", "ID", "名称", "简介", "作者", "版本", "类型"]
        suc_plugin = await cls.get_installed_plugins()
        index = 0
        data_list = []
        extra_data_list = []
        for plugin_info in plugin_list:
            data_list.append(
                [
                    "已安装" if plugin_info.module in suc_plugin else "",
                    index,
                    plugin_info.name,
                    plugin_info.description,
                    plugin_info.author,
                    cls.version_check(plugin_info, suc_plugin),
                    plugin_info.plugin_type_name,
                ]
            )
            index += 1
        for plugin_info in extra_plugin_list:
            extra_data_list.append(
                [
                    "已安装" if plugin_info.module in suc_plugin else "",
                    index,
                    plugin_info.name,
                    plugin_info.description,
                    plugin_info.author,
                    cls.version_check(plugin_info, suc_plugin),
                    plugin_info.plugin_type_name,
                ]
            )
            index += 1
        return [
            await ImageTemplate.table_page(
                "原生插件列表",
                "通过添加/移除插件 ID 来管理插件",
                column_name,
                data_list,
                text_style=row_style,
            ),
            await ImageTemplate.table_page(
                "第三方插件列表",
                "通过添加/移除插件 ID 来管理插件",
                column_name,
                extra_data_list,
                text_style=row_style,
            ),
        ]

    @classmethod
    async def get_plugin_by_value(
        cls,
        index_or_module: str,
        is_update: bool = False,
        is_remove: bool = False,
    ) -> tuple[StorePluginInfo, bool]:
        """获取插件信息

        参数:
            index_or_module: 插件索引或模块名
            is_update: 是否是更新插件
            is_remove: 是否是移除插件

        异常:
            PluginStoreException: 插件不存在
            PluginStoreException: 插件已安装

        返回:
            StorePluginInfo: 插件信息
            bool: 是否是外部插件
        """
        plugin_list: list[StorePluginInfo]
        extra_plugin_list: list[StorePluginInfo]
        plugin_list, extra_plugin_list = await cls.get_data()
        plugin_info = None
        is_external = False
        try:
            plugin_key = await cls._resolve_plugin_key(index_or_module)
        except PluginStoreException:
            if not is_remove:
                raise
            # 移除时插件可能已不在商店列表，回退到数据库查找
            plugin_key = None

        if plugin_key is not None:
            for p in plugin_list:
                if p.module == plugin_key:
                    is_external = False
                    plugin_info = p
                    break
            for p in extra_plugin_list:
                if p.module == plugin_key:
                    is_external = True
                    plugin_info = p
                    break

        installed_modules = set((await cls.get_installed_plugins()).keys())

        def is_installed(info: StorePluginInfo) -> bool:
            return (
                info.module in installed_modules
                or cls._resolve_local_plugin_path(
                    info, is_external=is_external
                ).exists()
            )

        if is_remove:
            # 商店列表中找不到时，从数据库构建最小插件信息
            if not plugin_info:
                db_obj = await PluginInfo.get_plugin(
                    module=index_or_module, plugin_type=PluginType.PARENT
                ) or await PluginInfo.get_plugin(module=index_or_module)
                if db_obj is None:
                    db_obj = await PluginInfo.get_or_none(name=index_or_module)
                if db_obj is None:
                    raise PluginStoreException("插件 Module / 名称 不存在...")
                _mp = db_obj.module_path
                _path = BASE_PATH.parent / Path(_mp.replace(".", os.sep))
                plugin_info = StorePluginInfo(
                    name=db_obj.name,
                    module=db_obj.module,
                    module_path=_mp,
                    description="",
                    usage="",
                    author=db_obj.author or "",
                    version=db_obj.version or "0.0.0",
                    plugin_type=db_obj.plugin_type or PluginType.NORMAL,
                    is_dir=_path.is_dir(),
                )
                is_external = True
            if not is_installed(plugin_info):
                raise PluginStoreException(f"插件 {plugin_info.name} 未安装，无法移除")
            if plugin_obj := await PluginInfo.get_plugin(
                module=plugin_info.module,
                plugin_type=PluginType.PARENT,
                load_status=True,
            ):
                plugin_info.module_path = plugin_obj.module_path
            elif plugin_obj := await PluginInfo.get_plugin(
                module=plugin_info.module, load_status=True
            ):
                plugin_info.module_path = plugin_obj.module_path
            return plugin_info, is_external

        if not plugin_info:
            raise PluginStoreException(f"插件不存在: {plugin_key}")

        if is_update:
            if not is_installed(plugin_info):
                raise PluginStoreException(f"插件 {plugin_info.name} 未安装，无法更新")
            return plugin_info, is_external

        if is_installed(plugin_info):
            raise PluginStoreException(f"插件 {plugin_info.name} 已安装，无需重复安装")

        return plugin_info, is_external

    @classmethod
    async def add_plugin(
        cls,
        index_or_module: str,
        source: str | None = None,
        *,
        install_dependencies: bool = True,
        confirm_source_build: bool = False,
        return_result: bool = False,
    ) -> str | StoreInstallResult:
        """添加插件

        参数:
            plugin_id: 插件id或模块名

        返回:
            str: 返回消息
        """
        plugin_info, is_external = await cls.get_plugin_by_value(index_or_module)
        if plugin_info.github_url is None:
            plugin_info.github_url = DEFAULT_GITHUB_URL
        version_split = plugin_info.version.split("-")
        if len(version_split) > 1:
            github_url_split = plugin_info.github_url.split("/tree/")
            plugin_info.github_url = f"{github_url_split[0]}/tree/{version_split[1]}"
        logger.info(f"正在安装插件 {plugin_info.name}...", LOG_COMMAND)
        install_result = await cls.install_plugin_with_repo(
            plugin_info,
            is_external,
            source,
            install_dependencies=install_dependencies,
            confirm_source_build=confirm_source_build,
        )
        dependency_status = (
            "依赖已更新，需由运行时决定生效方式"
            if install_result.dependency_changes
            else "依赖已满足"
        )
        if return_result:
            return install_result
        return f"插件 {plugin_info.name} 安装完成\n- {dependency_status}"

    @classmethod
    @coordinated_store_operation
    async def install_plugin_with_repo(
        cls,
        plugin_info: StorePluginInfo,
        is_external: bool = False,
        source: str | None = None,
        branch: str = "main",
        *,
        install_dependencies: bool = True,
        confirm_source_build: bool = False,
    ) -> StoreInstallResult:
        """安装插件

        参数:
            plugin_info: 插件信息
            is_external: 是否是外部仓库（保留用于兼容旧调用）
            source: 强制使用的源，ali 为阿里云，git 为 GitHub；
                不指定时优先阿里云，失败后回退 GitHub
        """
        source_order = cls._get_source_order(source)
        errors: list[str] = []

        with tempfile.TemporaryDirectory(prefix="zhenxun_plugin_store_") as temp_dir:
            staged_result: tuple[list[tuple[Path, Path]], list[Path]] | None = None
            selected_source: RepoType | None = None

            for repo_type in source_order:
                source_name = cls._SOURCE_NAMES[repo_type]
                staging_root = Path(temp_dir) / repo_type.value
                try:
                    staged_result = await cls._download_plugin_to_staging(
                        plugin_info,
                        repo_type,
                        branch,
                        staging_root,
                    )
                    selected_source = repo_type
                    logger.info(
                        f"插件 {plugin_info.name} 使用{source_name}下载成功",
                        LOG_COMMAND,
                    )
                    break
                except Exception as e:
                    errors.append(f"{source_name}: {e}")
                    if repo_type != source_order[-1]:
                        logger.warning(
                            f"插件 {plugin_info.name} 使用{source_name}下载失败，"
                            "尝试 GitHub",
                            LOG_COMMAND,
                            e=e,
                        )

            if staged_result is None or selected_source is None:
                raise PluginStoreException(
                    f"插件 {plugin_info.name} 下载失败（{'；'.join(errors)}）"
                )

            deploy_files, requirement_files = staged_result
            dependency_plan: dict[str, Any] = {
                "resolved_packages": {},
                "package_changes": {"added": [], "changed": [], "removed": []},
                "candidate_inputs": [],
            }
            dependency_changes = False
            if requirement_files:
                from zhenxun.nonebot_store.dependencies import (
                    DependencyAnalysisError,
                    preflight_source_requirements,
                )

                try:
                    dependency_plan = await preflight_source_requirements(
                        requirement_files
                    )
                except DependencyAnalysisError as error:
                    raise PluginStoreException(error.code) from error
                changes = dependency_plan.get("package_changes", {})
                dependency_changes = bool(
                    changes.get("added") or changes.get("changed")
                )
                if (
                    dependency_changes
                    and dependency_plan.get("source_build_required")
                    and not confirm_source_build
                ):
                    raise PluginStoreException("source_build_confirmation_required")
                if not dependency_changes:
                    logger.info(
                        f"插件 {plugin_info.module_path} 的依赖已满足，跳过安装",
                        LOG_COMMAND,
                    )
            if dependency_changes and install_dependencies:
                for requirement_file in requirement_files:
                    logger.info(
                        f"开始安装插件 {plugin_info.module_path} "
                        f"依赖文件: {requirement_file}",
                        LOG_COMMAND,
                    )
                    await VirtualEnvPackageManager.install_requirement(requirement_file)

            cls._deploy_staged_plugin(plugin_info, deploy_files)
            return StoreInstallResult(dependency_plan=dependency_plan)

    @classmethod
    def _deploy_staged_plugin(
        cls,
        plugin_info: StorePluginInfo,
        deploy_files: list[tuple[Path, Path]],
    ) -> None:
        local_path = cls._resolve_local_plugin_path(plugin_info, is_external=False)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        if not plugin_info.is_dir:
            if len(deploy_files) != 1:
                raise PluginStoreException("单文件插件下载结果不唯一")
            staged_path = deploy_files[0][0]
            fd, temp_name = tempfile.mkstemp(
                prefix=f".{local_path.name}.", dir=local_path.parent
            )
            os.close(fd)
            temp_path = Path(temp_name)
            try:
                shutil.copy2(staged_path, temp_path)
                os.replace(temp_path, local_path)
            finally:
                temp_path.unlink(missing_ok=True)
            return

        transaction_root = (
            BASE_PATH.parent / "data" / "runtime" / "plugin-store-staging"
        )
        transaction_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix=f"{local_path.name}.", dir=transaction_root
        ) as transaction_dir:
            transaction_path = Path(transaction_dir)
            staged_deploy = transaction_path / "new"
            old_path = transaction_path / "old"
            staged_deploy.mkdir()
            for staged_path, destination_path in deploy_files:
                relative = destination_path.relative_to(local_path)
                target = staged_deploy / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(staged_path, target)
            if local_path.exists():
                os.replace(local_path, old_path)
            try:
                os.replace(staged_deploy, local_path)
            except Exception:
                if old_path.exists() and not local_path.exists():
                    os.replace(old_path, local_path)
                raise
            if old_path.exists():
                shutil.rmtree(old_path, onerror=win_on_rm_error)

    @staticmethod
    def _get_source_order(source: str | None) -> tuple[RepoType, ...]:
        """解析插件下载源。"""
        if source is None:
            return (RepoType.ALIYUN, RepoType.GITHUB)
        if source == "ali":
            return (RepoType.ALIYUN,)
        if source == "git":
            return (RepoType.GITHUB,)
        raise PluginStoreException(f"源类型错误: {source}，请使用 ali 或 git")

    @staticmethod
    def _get_repository_url(plugin_info: StorePluginInfo, repo_type: RepoType) -> str:
        """获取指定下载源对应的仓库地址。"""
        if repo_type == RepoType.ALIYUN and plugin_info.ali_url:
            return plugin_info.ali_url
        if plugin_info.github_url:
            return plugin_info.github_url
        raise PluginStoreException(f"插件 {plugin_info.name} 缺少仓库地址")

    @staticmethod
    def _get_repository_branch(repo_url: str | None, default_branch: str) -> str:
        """优先使用仓库 URL 中显式指定的分支、标签或提交。"""
        if repo_url and "/tree/" in repo_url:
            _, _, ref = repo_url.partition("/tree/")
            if ref := ref.strip("/"):
                return ref
        return default_branch

    @classmethod
    def _get_plugin_repository_branch(
        cls,
        plugin_info: StorePluginInfo,
        repo_type: RepoType,
        default_branch: str,
    ) -> str:
        """按下载源独立解析分支，避免把 GitHub 分支套到阿里云镜像。"""
        branch_source_url = (
            plugin_info.ali_url
            if repo_type == RepoType.ALIYUN
            else plugin_info.github_url
        )
        return cls._get_repository_branch(branch_source_url, default_branch)

    @classmethod
    async def _download_plugin_to_staging(
        cls,
        plugin_info: StorePluginInfo,
        repo_type: RepoType,
        default_branch: str,
        staging_root: Path,
    ) -> tuple[list[tuple[Path, Path]], list[Path]]:
        """从单一仓库源完整下载插件到临时目录。"""
        repo_url = cls._get_repository_url(plugin_info, repo_type)
        branch = cls._get_plugin_repository_branch(
            plugin_info,
            repo_type,
            default_branch,
        )
        module_path = plugin_info.module_path
        repository_plugin_path = module_path.replace(".", "/").strip("/")

        if plugin_info.is_dir:
            files = await RepoFileManager.list_directory_files(
                repo_url,
                repository_plugin_path,
                branch,
                repo_type=repo_type,
            )
        else:
            if not repository_plugin_path:
                raise PluginStoreException(
                    f"插件 {plugin_info.name} 的模块路径不能为空"
                )
            files = [RepoFileInfo(path=f"{repository_plugin_path}.py", is_dir=False)]

        files = [file for file in files if not file.is_dir]
        if not files:
            raise PluginStoreException(
                f"仓库中未找到插件目录: {plugin_info.module_path}"
            )

        local_plugin_path = cls._resolve_local_plugin_path(
            plugin_info, is_external=repo_type != RepoType.GITHUB
        )
        target_root = (
            local_plugin_path if plugin_info.is_dir else local_plugin_path.parent
        )
        download_files: list[tuple[str, Path]] = []
        deploy_files: list[tuple[Path, Path]] = []

        for file in files:
            source_path = Path(file.path)
            if source_path.is_absolute() or ".." in source_path.parts:
                raise PluginStoreException(f"仓库包含不安全的文件路径: {file.path}")

            staged_path = staging_root / source_path
            if plugin_info.is_dir:
                plugin_root = (
                    Path(repository_plugin_path) if repository_plugin_path else Path()
                )
                try:
                    relative_path = source_path.relative_to(plugin_root)
                except ValueError as e:
                    raise PluginStoreException(
                        f"插件文件不在模块目录内: {file.path}"
                    ) from e
                destination_path = target_root / relative_path
            else:
                destination_path = local_plugin_path

            download_files.append((file.path, staged_path))
            deploy_files.append((staged_path, destination_path))

        required_download_files = download_files.copy()
        requirement_files = [
            staging_root / Path(file.path)
            for file in files
            if Path(file.path).name in {"requirement.txt", "requirements.txt"}
        ]
        root_requirements: list[tuple[str, Path]] = []
        if not requirement_files:
            root_requirements = [
                ("requirement.txt", staging_root / "requirement.txt"),
                ("requirements.txt", staging_root / "requirements.txt"),
            ]
            download_files.extend(root_requirements)

        result = await RepoFileManager.download_files(
            repo_url,
            download_files,
            branch,
            repo_type=repo_type,
            ignore_error=bool(root_requirements),
        )
        if not result.success:
            raise PluginStoreException(result.error_message or "未知下载错误")

        for source_path, staged_path in required_download_files:
            if not staged_path.is_file():
                raise PluginStoreException(f"插件文件下载不完整: {source_path}")
            if (
                Path(source_path).suffix.lower() in cls._BINARY_EXTENSIONS
                and staged_path.stat().st_size == 0
            ):
                raise PluginStoreException(f"二进制文件下载为空: {source_path}")

        requirement_files = [path for path in requirement_files if path.is_file()]
        if root_requirements:
            requirement_files = [
                path for _, path in root_requirements if path.is_file()
            ]

        return deploy_files, requirement_files

    @classmethod
    @coordinated_store_operation
    async def remove_plugin(cls, index_or_module: str) -> str:
        """移除插件

        参数:
            index_or_module: 插件id或模块名

        返回:
            str: 返回消息
        """
        plugin_info, _ = await cls.get_plugin_by_value(index_or_module, is_remove=True)
        is_external = not plugin_info.module_path.startswith("zhenxun.")
        path = cls._resolve_local_plugin_path(plugin_info, is_external=is_external)
        if not path.exists():
            return f"插件 {plugin_info.name} 不存在..."
        logger.debug(f"尝试移除插件 {plugin_info.name} 文件: {path}", LOG_COMMAND)
        if plugin_info.is_dir:
            # 处理 Windows 下 .git 等目录内只读文件导致的 WinError 5
            shutil.rmtree(path, onerror=win_on_rm_error)
        else:
            path.unlink()
        await PluginInitManager.remove(plugin_info.module_path)
        plugin_records = await PluginInfo.get_plugins(
            load_status=None,
            filter_parent=False,
            module_path=plugin_info.module_path,
        )
        for plugin_record in plugin_records:
            await plugin_record.delete()
        return f"插件 {plugin_info.name} 移除成功! 重启后生效"

    @classmethod
    async def search_plugin(cls, plugin_name_or_author: str) -> BuildImage | str:
        """搜索插件

        参数:
            plugin_name_or_author: 插件名称或作者

        返回:
            BuildImage | str: 返回消息
        """
        plugin_list, extra_plugin_list = await cls.get_data()
        all_plugin_list = plugin_list + extra_plugin_list
        suc_plugin = await cls.get_installed_plugins()
        filtered_data = [
            (id, plugin_info)
            for id, plugin_info in enumerate(all_plugin_list)
            if plugin_name_or_author.lower() in plugin_info.name.lower()
            or plugin_name_or_author.lower() in plugin_info.author.lower()
        ]

        data_list = [
            [
                "已安装" if plugin_info.module in suc_plugin else "",
                id,
                plugin_info.name,
                plugin_info.description,
                plugin_info.author,
                cls.version_check(plugin_info, suc_plugin),
                plugin_info.plugin_type_name,
            ]
            for id, plugin_info in filtered_data
        ]
        if not data_list:
            return "未找到相关插件..."
        column_name = ["-", "ID", "名称", "简介", "作者", "版本", "类型"]
        return await ImageTemplate.table_page(
            "商店插件列表",
            "通过添加/移除插件 ID 来管理插件",
            column_name,
            data_list,
            text_style=row_style,
        )

    @classmethod
    async def update_plugin(
        cls,
        index_or_module: str,
        *,
        install_dependencies: bool = True,
        confirm_source_build: bool = False,
        return_result: bool = False,
    ) -> str | StoreInstallResult:
        """更新插件

        参数:
            index_or_module: 插件id

        返回:
            str: 返回消息
        """
        plugin_info, is_external = await cls.get_plugin_by_value(index_or_module, True)
        logger.info(f"尝试更新插件 {plugin_info.name}", LOG_COMMAND)
        suc_plugin = await cls.get_installed_plugins()
        logger.debug(f"当前插件列表: {suc_plugin}", LOG_COMMAND)
        if plugin_info.github_url is None:
            plugin_info.github_url = DEFAULT_GITHUB_URL
        install_result = await cls.install_plugin_with_repo(
            plugin_info,
            is_external,
            install_dependencies=install_dependencies,
            confirm_source_build=confirm_source_build,
        )
        if return_result:
            return install_result
        return f"插件 {plugin_info.name} 更新成功! 重启后生效"

    @classmethod
    async def update_all_plugin(cls) -> str:
        """更新插件

        参数:
            plugin_id: 插件id

        返回:
            str: 返回消息
        """
        plugin_list, extra_plugin_list = await cls.get_data()
        all_plugin_list = plugin_list + extra_plugin_list
        plugin_name_list = [p.name for p in all_plugin_list]
        update_failed_list = []
        update_success_list = []
        result = "--已更新{}个插件 {}个失败 {}个成功--"
        logger.info(f"尝试更新全部插件 {plugin_name_list}", LOG_COMMAND)
        suc_plugin = await cls.get_installed_plugins()
        for plugin_info in all_plugin_list:
            try:
                if plugin_info.module not in suc_plugin:
                    logger.debug(
                        f"插件 {plugin_info.name}({plugin_info.module}) 未安装，跳过",
                        LOG_COMMAND,
                    )
                    continue
                if cls.check_version_is_new(plugin_info, suc_plugin):
                    logger.debug(
                        f"插件 {plugin_info.name}({plugin_info.module}) "
                        "已是最新版本，跳过",
                        LOG_COMMAND,
                    )
                    continue
                logger.info(
                    f"正在更新插件 {plugin_info.name}({plugin_info.module})",
                    LOG_COMMAND,
                )
                is_external = True
                if plugin_info.github_url is None:
                    plugin_info.github_url = DEFAULT_GITHUB_URL
                    is_external = False
                await cls.install_plugin_with_repo(
                    plugin_info,
                    is_external,
                )
                update_success_list.append(plugin_info.name)
            except Exception as e:
                logger.error(
                    f"更新插件 {plugin_info.name}({plugin_info.module}) 失败",
                    LOG_COMMAND,
                    e=e,
                )
                update_failed_list.append(plugin_info.name)
        if not update_success_list and not update_failed_list:
            return "全部插件已是最新版本"
        if update_success_list:
            result += "\n* 以下插件更新成功:\n\t- {}".format(
                "\n\t- ".join(update_success_list)
            )
        if update_failed_list:
            result += "\n* 以下插件更新失败:\n\t- {}".format(
                "\n\t- ".join(update_failed_list)
            )
        return (
            result.format(
                len(update_success_list) + len(update_failed_list),
                len(update_failed_list),
                len(update_success_list),
            )
            + "\n重启后生效"
        )

    @classmethod
    async def _resolve_plugin_key(cls, plugin_id: str) -> str:
        """获取插件module

        参数:
            plugin_id: module，id或插件名称

        异常:
            PluginStoreException: 插件不存在
            PluginStoreException: 插件不存在

        返回:
            str: 插件模块名
        """
        plugin_list, extra_plugin_list = await cls.get_data()
        all_plugin_list = plugin_list + extra_plugin_list
        if is_number(plugin_id):
            idx = int(plugin_id)
            if idx < 0 or idx >= len(all_plugin_list):
                raise PluginStoreException("插件ID不存在...")
            return all_plugin_list[idx].module
        elif isinstance(plugin_id, str):
            if plugin_id in [v.module for v in all_plugin_list]:
                return plugin_id

            for plugin_info in all_plugin_list:
                if plugin_info.name.lower() == plugin_id.lower():
                    return plugin_info.module

            raise PluginStoreException("插件 Module / 名称 不存在...")
