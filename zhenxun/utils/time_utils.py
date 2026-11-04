from datetime import date, datetime, timedelta
import os
from pathlib import Path
import re
import time
from zoneinfo import ZoneInfo

DEFAULT_TIMEZONE = "Asia/Shanghai"
"""未配置 TIMEZONE 时使用的时区"""
_ZONE_NAME = re.compile(r"^[A-Za-z0-9_+\-/]+$")

_LUNAR_START = date(1990, 1, 27)
"""农历 1990 年正月初一"""
_LUNAR_YEARS = [
    int(value, 16)
    for value in (
        "04af5 04970 064b0 074a3 0ea50 06b58 05ac0 0ab60 096e5 092e0 0c960 0d954 0d4a0 "
        "0da50 07552 056a0 0abb7 025d0 092d0 0cab5 0a950 0b4a0 0bca4 0ad50 055d9 04ba0 "
        "0a5b0 15176 05270 0a930 07954 06aa0 0ad50 05b52 04b60 0a6e6 0a4f0 05260 0ea65 "
        "0d520 0daa0 076a3 096d0 04afb 04ad0 0a4d0 1d0b6 0d250 0d520 0dd45 0b5a0 056d0 "
        "055b2 049b0 0a577 0a4b0 0aa50 1b255 06d20 0ada0 14b63 09370 049f8 04970 064b0 "
        "168a6 0ea50 06b20 1a6c4 0aae0 092e0 0d2e3 0c960 0d557 0d4a0 0da50 05d55 056a0 "
        "0a6d0 055d4 092d0 0a9b8 0a950 0b4a0 0b6a6 0ad50 055a0 0aba4 0a5b0 052b0 0b273 "
        "06930 07337 06aa0 0ad50 14b55 04b60 0a570 054e4 0d160 0e968 0d520 0daa0 16aa6 "
        "056d0 04ae0 0a9d4 0a2d0 0d150 0f252 0d520"
    ).split()
]
"""农历 1990-2100 年的月份数据：低 4 位为闰月，4-15 位为一至十二月是否大月，
第 16 位为闰月是否大月"""


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

    @staticmethod
    def lunar_date(day: date) -> str | None:
        """公历日期转为农历日期文本

        参数:
            day: 公历日期

        返回:
            str | None: 如 "农历丙午年 九月廿六"，超出数据范围时为 None
        """
        offset = (day - _LUNAR_START).days
        if offset < 0:
            return None
        for index, info in enumerate(_LUNAR_YEARS):
            leap = info & 0xF
            months = [(m, False, 29 + (info >> (16 - m) & 1)) for m in range(1, 13)]
            if leap:
                months.insert(leap, (leap, True, 29 + (info >> 16 & 1)))
            for month, is_leap, length in months:
                if offset < length:
                    year = _LUNAR_START.year + index
                    stem = "甲乙丙丁戊己庚辛壬癸"[(year - 4) % 10]
                    branch = "子丑寅卯辰巳午未申酉戌亥"[(year - 4) % 12]
                    name = ("闰" if is_leap else "") + "正二三四五六七八九十冬腊"[
                        month - 1
                    ]
                    number = offset + 1
                    day_name = {10: "初十", 20: "二十", 30: "三十"}.get(number) or (
                        "初十廿"[number // 10] + "一二三四五六七八九"[number % 10 - 1]
                    )
                    return f"农历{stem}{branch}年 {name}月{day_name}"
                offset -= length
        return None
