from pathlib import Path
from typing import Literal

import nonebot
from pydantic import BaseModel, Field, field_validator

from zhenxun.utils.time_utils import DEFAULT_TIMEZONE, check_timezone

from .utils import ConfigsManager

__all__ = ["BotConfig", "Config"]


class BotSetting(BaseModel):
    self_nickname: str = ""
    """回复时NICKNAME"""
    system_proxy: str | None = None
    """系统代理"""
    network_proxy_mode: (
        Literal["", "legacy", "disabled", "global", "selected"] | None
    ) = None
    network_proxy_plugins: list[str] = Field(default_factory=list)
    network_proxy_core_enabled: bool = False
    network_proxy_bypass: list[str] | None = None
    db_url: str = ""
    """数据库链接, 默认值为sqlite:data/zhenxun.db"""
    platform_superusers: dict[str, list[str]] = Field(default_factory=dict)
    """平台超级用户"""
    qbot_id_data: dict[str, str] = Field(default_factory=dict)
    """官bot id:账号id"""
    ext_path: list[str] = Field(default_factory=list)
    """第三方插件路径"""
    qq_adapter_load: bool = False
    """是否加载 QQ 官方适配器"""
    telegram_adapter_load: bool = False
    """是否加载 Telegram 适配器"""
    business_identity_scopes: list[str] = Field(default_factory=lambda: ["telegram"])
    """按平台全局解析业务身份（金币、签到等）的平台作用域"""
    timezone: str = DEFAULT_TIMEZONE
    """Bot 计算日期与时间使用的 IANA 时区"""

    @field_validator("timezone", mode="before")
    @classmethod
    def _check_timezone(cls, value: object) -> str:
        name = str(value or "").strip() or DEFAULT_TIMEZONE
        if error := check_timezone(name):
            nonebot.logger.warning(
                f"TIMEZONE 配置无效（{error}），已使用 {DEFAULT_TIMEZONE}"
            )
            return DEFAULT_TIMEZONE
        return name

    def get_qbot_uid(self, qbot_id: str) -> str | None:
        """获取官bot账号id

        参数:
            qbot_id: 官bot id

        返回:
            str: 账号id
        """
        return self.qbot_id_data.get(qbot_id)

    def get_superuser(self, platform: str) -> list[str]:
        """获取超级用户

        参数:
            platform: 对应平台

        返回:
            list[str]: 超级用户id
        """
        if self.platform_superusers:
            return self.platform_superusers.get(platform, [])
        return []

    def get_sql_type(self) -> str:
        """获取数据库类型

        返回:
            str: 数据库类型, postgres, mysql, sqlite
        """
        return self.db_url.split(":", 1)[0] if self.db_url else ""


Config = ConfigsManager(Path() / "data" / "configs" / "plugins2config.yaml")

BotConfig = nonebot.get_plugin_config(BotSetting)
