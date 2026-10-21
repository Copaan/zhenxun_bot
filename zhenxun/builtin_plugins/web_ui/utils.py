import contextlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import Depends, HTTPException, Request
from fastapi.security import OAuth2PasswordBearer
from nonebot.utils import run_sync
import psutil
import ujson as json

from zhenxun.configs.config import Config
from zhenxun.configs.path_config import DATA_PATH
from zhenxun.services.db_context import with_db_timeout
from zhenxun.services.message_load import is_db_unhealthy
from zhenxun.utils.webui_session import SESSION_SECONDS

from .base_model import SystemStatus, User
from .security import validate_access_token

ALGORITHM = "HS256"
ACCESS_TOKEN_IDLE_MINUTES = SESSION_SECONDS // 60
ACCESS_TOKEN_ABSOLUTE_HOURS = SESSION_SECONDS // 3600
# Compatibility alias for extensions importing the old constant.
ACCESS_TOKEN_EXPIRE_MINUTES = ACCESS_TOKEN_IDLE_MINUTES
DB_BUSY_MESSAGE = "数据库繁忙，请稍后再试"
WEBUI_DB_TIMEOUT = 3.0

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/login")

token_file = DATA_PATH / "web_ui" / "token.json"
token_file.parent.mkdir(parents=True, exist_ok=True)
token_data = {"token": []}
if token_file.exists():
    with contextlib.suppress(json.JSONDecodeError):
        token_data = json.load(open(token_file, encoding="utf8"))


_WEBUI_PLATFORMS = {"qq_client": "onebot_v11", "qq_api": "qq_official"}


def webui_platform(bot) -> str:
    """WebUI 展示用的协议类型

    参数:
        bot: Bot

    返回:
        str: OneBot V11 为 onebot_v11，QQ 官方为 qq_official，其余为 other
    """
    from zhenxun.utils.platform import PlatformUtils

    return _WEBUI_PLATFORMS.get(PlatformUtils.get_platform_scope(bot), "other")


async def webui_db_call(coro, operation: str):
    if is_db_unhealthy():
        close = getattr(coro, "close", None)
        if callable(close):
            close()
        raise TimeoutError(DB_BUSY_MESSAGE)
    return await with_db_timeout(
        coro,
        timeout=WEBUI_DB_TIMEOUT,
        operation=operation,
        source="web_ui",
    )


def validate_path(path_str: str | None) -> tuple[Path | None, str | None]:
    """验证路径是否安全

    参数:
        path_str: 用户输入的路径

    返回:
        tuple[Path | None, str | None]: (验证后的路径, 错误信息)
    """
    try:
        if not path_str:
            return Path().resolve(), None

        # 1. 规范化路径并转换为绝对路径（resolve() 会展开所有 .. 和符号链接）
        path = Path(path_str).resolve()

        # 2. 获取项目根目录
        root_dir = Path().resolve()

        # 3. 验证 resolve() 后的路径是否仍在项目根目录内（防路径穿越）
        try:
            if not path.is_relative_to(root_dir):
                return None, "访问路径超出允许范围"
        except ValueError:
            return None, "无效的路径格式"

        # 4. 验证路径长度是否合理
        return (None, "路径长度超出限制") if len(str(path)) > 4096 else (path, None)
    except Exception as e:
        return None, f"路径验证失败: {e!s}"


def validate_filename(name: str) -> str | None:
    """验证文件名是否安全（不允许路径分隔符或路径穿越）

    参数:
        name: 用户输入的文件名

    返回:
        str | None: 错误信息，无错误则返回 None
    """
    if not name or not name.strip():
        return "文件名不能为空"
    # 禁止任何路径分隔符，防止将文件名当路径使用
    if any(c in name for c in ("/", "\\", "\x00")):
        return "文件名包含非法路径分隔符"
    # 禁止 . 和 .. 作为文件名
    if name.strip(".") == "":
        return "文件名非法"
    # 禁止危险字符（Windows / Linux 通用）
    if any(c in name for c in ("<", ">", ":", '"', "|", "?", "*")):
        return "文件名包含非法字符"
    return None


def get_user(uname: str) -> User | None:
    """获取账号密码

    参数:
        uname: uname

    返回:
        Optional[User]: 用户信息
    """
    username = Config.get_config("web-ui", "username")
    password = Config.get_config("web-ui", "password")
    if username and password and uname == username:
        return User(username=username, password=password)


def create_token(
    user: User,
    expires_delta: timedelta | None = None,
    extra_claims: dict[str, Any] | None = None,
):
    """创建token

    参数:
        user: 用户信息
        expires_delta: 过期时间.
    """
    from zhenxun.utils.webui_session import issue_token

    return issue_token(
        Config.get_config("web-ui", "secret"), user.username, claims=extra_claims
    )


def authentication():
    """权限验证

    异常:
        HTTPException: HTTPException
    """

    # if token not in token_data["token"]:
    def inner(token: str = Depends(oauth2_scheme)):
        if not validate_access_token(token):
            raise HTTPException(
                status_code=401,
                detail="登录验证失败或已失效, 踢出房间!",
                headers={"WWW-Authenticate": "Bearer"},
            )

    return Depends(inner)


def request_authenticated(request: Request) -> bool:
    """请求是否携带有效的 WebUI 登录凭证

    参数:
        request: Request

    返回:
        bool: 凭证是否有效
    """
    token = request.headers.get("Authorization", "")
    return bool(token) and validate_access_token(token)


_PUBLIC_STARTUP_KEYS = (
    "boot_id",
    "launcher_boot_id",
    "startup_id",
    "pid",
    "state",
    "server_bound",
    "operating_mode",
    "accepts_bot_events",
    "failure_terminal",
    "transaction_verification_pending",
)


def public_startup_status(status: dict[str, Any]) -> dict[str, Any]:
    """未登录时可见的启动状态，只保留启动进度与身份字段

    参数:
        status: 完整启动状态

    返回:
        dict[str, Any]: 公开启动状态
    """
    stages = status.get("stages")
    return {
        **{key: status.get(key) for key in _PUBLIC_STARTUP_KEYS if key in status},
        "stages": {
            name: {"state": stage.get("state")}
            for name, stage in (stages.items() if isinstance(stages, dict) else ())
            if isinstance(stage, dict)
        },
    }


@run_sync
def get_system_status() -> SystemStatus:
    """获取系统信息等"""
    from zhenxun.services.plugin_host import plugin_host

    from .public import webui_resources

    resources = webui_resources

    cpu = psutil.cpu_percent()
    memory = psutil.virtual_memory().percent
    disk_root = Path().resolve().anchor  # 跨平台：取当前工作目录所在盘的根
    disk = psutil.disk_usage(disk_root).percent
    return SystemStatus(
        cpu=cpu,
        memory=memory,
        disk=disk,
        check_time=datetime.now().replace(microsecond=0),
        runtime_generation=plugin_host.generation,
        webui_revision=str(resources.manifest.get("revision") or ""),
        resources_ready=resources.ready,
    )
