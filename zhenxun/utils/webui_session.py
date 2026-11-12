from __future__ import annotations

import copy
from io import StringIO
from pathlib import Path
import secrets
import time
from typing import Any

import jwt

SESSION_VERSION = 2
SESSION_SECONDS = 6 * 60 * 60
SIMPLE_CONFIG = Path("data/config.yaml")
PLUGIN_CONFIG = Path("data/configs/plugins2config.yaml")
_CREDENTIAL_HELP = {
    "USERNAME": "前端管理用户名",
    "PASSWORD": "前端管理密码",
    "SECRET": "JWT密钥",
}


def credential_documents(
    username: str,
    password_hash: str,
    secret: str | None = None,
    *,
    root: Path | None = None,
    templates: dict[str, dict[str, Any]] | None = None,
) -> tuple[bytes, bytes]:
    """生成写入 WebUI 管理员凭据后的两份配置文件内容，不依赖运行中的 Bot

    参数:
        username: 用户名
        password_hash: 密码哈希
        secret: JWT 密钥，为 None 时保持原值
        root: 实例目录，默认为当前目录
        templates: 配置项名 -> 已注册配置项的字段，缺少的字段补入插件配置

    返回:
        tuple[bytes, bytes]: data/config.yaml 与 data/configs/plugins2config.yaml 的内容
    """
    from ruamel.yaml import YAML

    def parser() -> YAML:
        value = YAML()
        value.preserve_quotes = True
        value.indent(mapping=2, sequence=2, offset=0)
        return value

    def load(path: Path) -> Any:
        if not path.exists():
            return {}
        with path.open(encoding="utf-8") as stream:
            return parser().load(stream) or {}

    def dump(data: Any) -> bytes:
        stream = StringIO()
        parser().dump(data, stream)
        return stream.getvalue().encode("utf-8")

    root = root or Path.cwd()
    values = {"USERNAME": username, "PASSWORD": password_hash, "SECRET": secret}
    simple = load(root / SIMPLE_CONFIG)
    simple_group = simple.setdefault("web-ui", {})
    plugins = load(root / PLUGIN_CONFIG)
    plugin_group = plugins.setdefault("web-ui", {})
    for key, value in values.items():
        entry = plugin_group.get(key)
        if not isinstance(entry, dict):
            entry = plugin_group[key] = {}
        template = (templates or {}).get(key) or {
            "value": None,
            "help": _CREDENTIAL_HELP[key],
            "default_value": None,
            "ui": None,
        }
        for field, default in template.items():
            entry.setdefault(field, copy.deepcopy(default))
        if value is not None:
            entry["value"] = value
            simple_group[key] = value
    return dump(simple), dump(plugins)


def compact_token(token: str) -> str:
    value = token.strip()
    return value[7:].strip() if value.lower().startswith("bearer ") else value


def issue_token(secret: str, subject: str, *, claims=None, now=None) -> str:
    """Issue the fixed-lifetime credential shared by all WebUI entry points."""
    issued = int(time.time() if now is None else now)
    extra = dict(claims or {})
    value = {
        **extra,
        "sub": subject,
        "sid": extra.get("sid") or secrets.token_urlsafe(24),
        "session_version": SESSION_VERSION,
        "auth_time": issued,
        "iat": issued,
        "exp": issued + SESSION_SECONDS,
        "absolute_exp": issued + SESSION_SECONDS,
    }
    return jwt.encode(value, secret, algorithm="HS256")


def decode_token(token: str, secret: str, *, now=None) -> dict:
    """Verify the common session contract without loading a running bot."""
    if not isinstance(token, str) or not token or len(token) > 16384 or not secret:
        raise jwt.InvalidTokenError("invalid session")
    value = jwt.decode(
        compact_token(token),
        secret,
        algorithms=["HS256"],
        options={"verify_exp": False, "verify_iat": False},
    )
    current = time.time() if now is None else now
    if (
        value.get("session_version") != SESSION_VERSION
        or not isinstance(value.get("sub"), str)
        or not value["sub"]
        or not isinstance(value.get("sid"), str)
        or not 1 <= len(value["sid"]) <= 128
        or any(
            type(value.get(key)) is not int
            for key in ("iat", "exp", "auth_time", "absolute_exp")
        )
        or value["iat"] != value["auth_time"]
        or value["exp"] != value["absolute_exp"]
        or value["exp"] - value["iat"] != SESSION_SECONDS
        or value["iat"] > current + 30
    ):
        raise jwt.InvalidTokenError("invalid session contract")
    if current >= value["exp"]:
        raise jwt.ExpiredSignatureError("session expired")
    return value


def session_response(token: str, claims: dict) -> dict:
    return {
        "access_token": compact_token(token),
        "token_type": "bearer",
        "expires_at": claims["exp"],
        "expires_in": max(0, int(claims["exp"] - time.time())),
        "session_auth_version": SESSION_VERSION,
    }


class AuthenticatedSession(str):
    """Carry verified context alongside the stable task-owner identifier."""

    def __new__(cls, sid, claims=None, secret="", *, first_deployment=False):
        instance = super().__new__(cls, sid)
        instance.claims = dict(claims or {})
        instance.secret = secret
        instance.first_deployment = first_deployment
        instance.administrator = None
        instance.task_id = None
        return instance

    def context(self, task_id):
        return {
            "version": SESSION_VERSION,
            "task_id": task_id,
            "sid": str(self),
            "sub": self.claims["sub"],
            "auth_source": self.claims.get("auth_source", "password"),
            "boot_id": self.claims.get("boot_id", ""),
            "iat": self.claims["iat"],
            "exp": self.claims["exp"],
        }


def matches_context(claims: dict, context: dict) -> bool:
    return bool(context) and all(
        claims.get(key, "password" if key == "auth_source" else "") == context.get(key)
        for key in ("sid", "sub", "auth_source", "boot_id", "iat", "exp")
    )
