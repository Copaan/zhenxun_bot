from datetime import date, datetime, timedelta
import os
from pathlib import Path
import re
import time
from zoneinfo import ZoneInfo

DEFAULT_TIMEZONE = "Asia/Shanghai"
"""未配置 TIMEZONE 时使用的时区"""
_ZONE_NAME = re.compile(r"^[A-Za-z0-9_+\-/]+$")


def check_timezone(name: str) -> str | None:
    """检查时区名称是否可用

    参数:
        name: IANA 时区名称，如 Asia/Shanghai

    返回:
        str | None: 错误信息，可用时为 None
    """
    if not _ZONE_NAME.fullmatch(name or ""):
        return "时区名称只能包含字母、数字和 _ + - /，例如 Asia/Shanghai"
    try:
        ZoneInfo(name)
    except Exception:
        return f"未知时区 {name}，请填写 IANA 时区名称，例如 Asia/Shanghai"
    return None


def apply_process_timezone(name: str) -> None:
    """把进程的本地时区设为指定时区，time.localtime、datetime.now 与日志时间
    都按该时区计算；系统缺少时区数据（如 Windows）时按当前的 UTC 偏移设置

    参数:
        name: IANA 时区名称
    """
    if hasattr(time, "tzset") and (Path("/usr/share/zoneinfo") / name).is_file():
        os.environ["TZ"] = name
        time.tzset()
        return
    offset = datetime.now(ZoneInfo(name)).utcoffset() or timedelta()
    if "TZ" not in os.environ and datetime.now().astimezone().utcoffset() == offset:
        return
    minutes = int(offset.total_seconds()) // 60
    sign, minutes = ("-", minutes) if minutes >= 0 else ("+", -minutes)
    os.environ["TZ"] = f"UTC{sign}{minutes // 60}:{minutes % 60:02d}"
    if hasattr(time, "tzset"):
        time.tzset()
    else:
        import ctypes

        ctypes.cdll.ucrtbase._tzset()


class TimeUtils:
    @staticmethod
    def zone() -> ZoneInfo:
        """Bot 使用的时区（.env.dev 的 TIMEZONE）

        返回:
            ZoneInfo: 时区
        """
        from zhenxun.configs.config import BotConfig

        return ZoneInfo(BotConfig.timezone)

    @classmethod
    def now(cls) -> datetime:
        """Bot 时区的当前时间

        返回:
            datetime: 带时区的当前时间
        """
        return datetime.now(cls.zone())

    @classmethod
    def get_day_start(cls, target_date: date | datetime | None = None) -> datetime:
        """获取某天的0点时间

        返回:
            datetime: 今天某天的0点时间
        """
        zone = cls.zone()
        if not target_date:
            target_date = datetime.now(zone)

        if isinstance(target_date, datetime) and target_date.tzinfo is None:
            target_date = target_date.replace(tzinfo=zone)

        return (
            target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            if isinstance(target_date, datetime)
            else datetime.combine(target_date, datetime.min.time(), tzinfo=zone)
        )

    @classmethod
    def is_valid_date(cls, date_text: str, separator: str = "-") -> bool:
        """日期是否合法

        参数:
            date_text: 日期
            separator: 分隔符

        返回:
            bool: 日期是否合法
        """
        try:
            datetime.strptime(date_text, f"%Y{separator}%m{separator}%d")
            return True
        except ValueError:
            return False

    @classmethod
    def parse_time_string(cls, time_str: str) -> int:
        """
        将带有单位的时间字符串 (e.g., "10s", "5m", "1h", "1d") 解析为总秒数。
        """
        time_str = time_str.lower().strip()
        match = re.match(r"^(\d+)([smhd])$", time_str)
        if not match:
            raise ValueError(
                f"无效的时间格式: '{time_str}'。请使用如 '30s', '10m', '2h', '1d'的格式"
            )

        value, unit = int(match.group(1)), match.group(2)

        if unit == "s":
            return value
        if unit == "m":
            return value * 60
        if unit == "h":
            return value * 3600
        if unit == "d":
            return value * 86400
        return 0

    @classmethod
    def parse_interval_to_dict(cls, interval_str: str) -> dict:
        """
        将时间间隔字符串解析为 APScheduler 的 interval 触发器所需的字典。
        """
        time_str_lower = interval_str.lower().strip()
        match = re.match(r"^(\d+)([smhd])$", time_str_lower)
        if not match:
            raise ValueError(
                "时间间隔格式错误, 请使用如 '30m', '2h', '1d', '10s' 的格式。"
            )

        value, unit = int(match.group(1)), match.group(2)

        if unit == "s":
            return {"seconds": value}
        if unit == "m":
            return {"minutes": value}
        if unit == "h":
            return {"hours": value}
        if unit == "d":
            return {"days": value}
        return {}

    @classmethod
    def format_duration(cls, seconds: float) -> str:
        """
        将秒数格式化为易于阅读的字符串 (例如 "1小时5分钟", "30.5秒")
        """
        seconds = round(seconds, 1)
        if seconds < 0.1:
            return "不到1秒"
        if seconds < 60:
            return f"{seconds}秒"

        minutes, sec_remainder = divmod(int(seconds), 60)

        if minutes < 60:
            if sec_remainder == 0:
                return f"{minutes}分钟"
            return f"{minutes}分钟{sec_remainder}秒"

        hours, rem_minutes = divmod(minutes, 60)
        if rem_minutes == 0:
            return f"{hours}小时"
        return f"{hours}小时{rem_minutes}分钟"
