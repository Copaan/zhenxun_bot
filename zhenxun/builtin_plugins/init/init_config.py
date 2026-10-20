from io import StringIO
from pathlib import Path

import nonebot
from nonebot import get_loaded_plugins
from nonebot.drivers import Driver
from nonebot.plugin import Plugin
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

from zhenxun.configs.config import Config
from zhenxun.configs.path_config import DATA_PATH
from zhenxun.configs.utils import RegisterConfig
from zhenxun.services.log import logger
from zhenxun.utils.manager.priority_manager import PriorityLifecycle

_yaml = YAML(pure=True)
_yaml.allow_unicode = True
_yaml.indent = 2

driver: Driver = nonebot.get_driver()

SIMPLE_CONFIG_FILE = DATA_PATH / "config.yaml"

old_config_file = Path() / "zhenxun" / "configs" / "config.yaml"
if old_config_file.exists():
    old_config_file.rename(SIMPLE_CONFIG_FILE)


def _handle_config(plugin: Plugin, exists_module: list[str]):
    """处理配置项，配置组名称取同名插件，否则取首个注册的插件

    参数:
        plugin: Plugin
        exists_module: 已注册的配置键列表
    """
    if plugin.metadata and plugin.metadata.extra:
        extra = plugin.metadata.extra
        if configs := extra.get("configs"):
            for config in configs:
                reg_config = RegisterConfig(**config)
                module = reg_config.module or plugin.name
                g_config = Config.get(module)
                if not g_config.name or plugin.name == module:
                    g_config.name = plugin.metadata.name
                Config.add_plugin_config(
                    module,
                    reg_config.key,
                    reg_config.value,
                    help=reg_config.help,
                    default_value=reg_config.default_value,
                    type=reg_config.type,  # type: ignore
                    arg_parser=reg_config.arg_parser,
                    ui=reg_config.ui,
                    plugin=plugin.name,
                    _override=False,
                )
                exists_module.append(f"{module}:{reg_config.key}".lower())


def _normalize_config_keys(config_keys: list[str]) -> list[str]:
    return sorted(set(config_keys) | set(Config.add_module))


def _dump_yaml(data: CommentedMap) -> str:
    stream = StringIO()
    _yaml.dump(data, stream)
    return stream.getvalue()


def _write_simple_config_if_changed(data: CommentedMap) -> bool:
    content = _dump_yaml(data)
    current = (
        SIMPLE_CONFIG_FILE.read_text(encoding="utf8")
        if SIMPLE_CONFIG_FILE.exists()
        else None
    )
    if current == content:
        return False
    SIMPLE_CONFIG_FILE.write_text(content, encoding="utf8")
    return True


def _generate_simple_config(exists_module: list[str]) -> bool:
    """
    生成简易配置

    异常:
        AttributeError: AttributeError
    """
    # 读取用户配置
    _data = {}
    _tmp_data = {}
    if SIMPLE_CONFIG_FILE.exists():
        _data = _yaml.load(SIMPLE_CONFIG_FILE.open(encoding="utf8"))
    # 将简易配置文件的数据填充到配置文件
    for module in Config.keys():
        _tmp_data[module] = {}
        for k in Config[module].configs.keys():
            try:
                if _data.get(module) and k in _data[module].keys():
                    Config.set_config(module, k, _data[module][k])
                if f"{module}:{k}".lower() in exists_module:
                    _tmp_data[module][k] = Config.get_config(
                        module, k, build_model=False
                    )
            except AttributeError as e:
                raise AttributeError(f"{e}\n可能为config.yaml配置文件填写不规范") from e
        if not _tmp_data[module]:
            _tmp_data.pop(module)
    Config.save()
    temp_file = DATA_PATH / "temp_config.yaml"
    # 重新生成简易配置文件以挂载注释
    try:
        with open(temp_file, "w", encoding="utf8") as wf:
            _yaml.dump(_tmp_data, wf)
        with open(temp_file, encoding="utf8") as rf:
            _data = _yaml.load(rf)
        # 添加注释
        for module in _data.keys():
            help_text = ""
            plugin_name = Config.get(module).name or module
            help_text += plugin_name + "\n"
            for k in _data[module].keys():
                help_text += f"{k}: {Config[module].configs[k].help}" + "\n"
            _data.yaml_set_comment_before_after_key(after=help_text[:-1], key=module)
        return _write_simple_config_if_changed(_data)
    except Exception as e:
        logger.error("生成简易配置注释错误...", e=e)
        return False
    finally:
        if temp_file.exists():
            temp_file.unlink()


@PriorityLifecycle.on_startup(
    priority=0,
    stage="runtime",
    timeout=15,
    component_id="runtime:reconcile_config",
    depends_on=("management:database",),
)
def reconcile_config_runtime() -> set[Path]:
    """初始化插件数据配置，按模块名顺序处理插件，保证生成的配置文件稳定

    返回:
        set[Path]: 被改写的配置文件
    """
    exists_module = []
    for plugin in sorted(get_loaded_plugins(), key=lambda p: p.module_name):
        if plugin.metadata:
            _handle_config(plugin, exists_module)
    changed = _generate_simple_config(_normalize_config_keys(exists_module))
    Config.reload()
    return {SIMPLE_CONFIG_FILE.resolve()} if changed else set()
