from datetime import datetime
from hashlib import sha256
import os
from pathlib import Path
import random
from zoneinfo import ZoneInfo

import aiofiles
import nonebot
from nonebot.drivers import Driver
from nonebot_plugin_uninfo import Uninfo

from zhenxun import ui
from zhenxun.configs.config import BotConfig, Config
from zhenxun.configs.path_config import THEMES_PATH
from zhenxun.models.sign_user import SignUser
from zhenxun.models.user_console import UserConsole
from zhenxun.services.avatar_service import avatar_service
from zhenxun.utils.manager.priority_manager import PriorityLifecycle

from .config import (
    SIGN_TODAY_CARD_PATH,
    level2attitude,
    lik2level,
    lik2relation,
)

assert (
    len(level2attitude) == len(lik2level) == len(lik2relation)
), "好感度态度、等级、关系长度不匹配！"

AVA_URL = "http://q1.qlogo.cn/g?b=qq&nk={}&s=160"

driver: Driver = nonebot.get_driver()

base_config = Config.get("sign_in")


MORNING_MESSAGE = [
    "早上好，希望今天是美好的一天！",
    "醒了吗，今天也要元气满满哦！",
    "早上好呀，今天也要开心哦！",
    "早安，愿你拥有美好的一天！",
]

LG_MESSAGE = [
    "今天要早点休息哦~",
    "可不要熬夜到太晚呀",
    "请尽早休息吧！",
    "不要熬夜啦！",
]


def _now() -> datetime:
    """签到日期使用的北京时间"""
    return datetime.now(ZoneInfo("Asia/Shanghai"))


def _cached(card_file: Path) -> bool:
    """卡片缓存是否可用，模板更新后的旧缓存会被删除

    参数:
        card_file: 卡片路径

    返回:
        bool: 是否可直接使用缓存
    """
    if not card_file.exists():
        return False
    if not _is_sign_card_cache_stale(card_file):
        return True
    card_file.unlink(missing_ok=True)
    return False


def _get_sign_template_files() -> list[Path]:
    theme_name = str(Config.get_config("UI", "THEME", "default") or "default")
    files: list[Path] = []
    theme_candidates = [theme_name]
    if theme_name != "default":
        theme_candidates.append("default")

    for candidate in theme_candidates:
        base = THEMES_PATH / candidate / "pages" / "builtin" / "sign"
        for file_name in ("main.html", "style.css", "manifest.json"):
            file_path = base / file_name
            if file_path.exists():
                files.append(file_path)
    return files


def _is_sign_card_cache_stale(card_file: Path) -> bool:
    if not card_file.exists():
        return False
    try:
        card_mtime = card_file.stat().st_mtime
    except OSError:
        return True

    for template_file in _get_sign_template_files():
        try:
            if template_file.stat().st_mtime > card_mtime:
                return True
        except OSError:
            continue
    return False


@PriorityLifecycle.on_startup(priority=5)
async def init_image():
    SIGN_TODAY_CARD_PATH.mkdir(exist_ok=True, parents=True)
    clear_sign_data_pic()


def _card_cache_key(user, session) -> str:
    from zhenxun.services.business_identity import current_business_identity

    identity = current_business_identity()
    revision = identity.revision if identity else 0
    display = sha256(
        f"identity-v2:{session.scope}:{session.self_id}:{session.user.id}:{revision}".encode()
    ).hexdigest()[:20]
    return f"{user.user_console_id}_{display}"


def _identity_display(session) -> dict[str, str | None]:
    from zhenxun.services.business_identity import current_business_identity

    identity = current_business_identity()
    label = "平台 ID"
    bound_qq = None
    raw = str(session.user.id)
    if identity:
        protocol = identity.protocol
        raw = protocol.raw_user_id
        if protocol.domain == "qq_api":
            label = (
                "平台 ID（OpenID）"
                if protocol.scene in {"c2c", "group"}
                else "平台 ID（频道用户 ID）"
            )
            if identity.storage_key != protocol.original_key:
                bound_qq = identity.storage_key
        elif protocol.domain == "qq_client":
            label = "平台 ID（QQ）"
    return {"platform_id": raw, "platform_id_label": label, "bound_qq": bound_qq}


async def get_card(
    user: SignUser,
    session: Uninfo,
    nickname: str,
    add_impression: float,
    gold: int | None,
    gift: str,
    is_double: bool = False,
    is_card_view: bool = False,
) -> Path:
    """获取好感度卡片，签到卡片当天复用，查看卡片在数据变化后重新生成

    参数:
        user: SignUser
        session: Uninfo
        nickname: 用户昵称
        add_impression: 新增的好感度，-1 表示本次未签到
        gold: 本次获得的金币，未签到时为当前金币
        gift: 礼物
        is_double: 是否触发双倍.
        is_card_view: 是否展示好感度卡片.

    返回:
        Path: 卡片路径
    """
    key = _card_cache_key(user, session)
    date = _now().date()
    if not is_card_view:
        card_file = SIGN_TODAY_CARD_PATH / f"{key}_sign_{date}.png"
        if _cached(card_file):
            return card_file
        if add_impression != -1:
            return await _generate_html_card(
                card_file,
                user,
                await user.user_console,
                session,
                nickname,
                reward=(add_impression, gold or 0, gift, is_double),
            )
    rank = await SignUser.impression_rank(user.impression)
    digest = sha256(
        f"{user.impression}:{user.sign_count}:{gold}:{rank}".encode()
    ).hexdigest()[:12]
    card_file = SIGN_TODAY_CARD_PATH / f"{key}_view_{date}_{digest}.png"
    if _cached(card_file):
        return card_file
    for old in SIGN_TODAY_CARD_PATH.glob(f"{key}_view_*.png"):
        old.unlink(missing_ok=True)
    return await _generate_html_card(
        card_file, user, await user.user_console, session, nickname, rank=rank
    )


def get_level_and_next_impression(impression: float) -> tuple[int, int | float, int]:
    """获取当前好感等级与下一等级的差距

    参数:
        impression: 好感度

    返回:
        tuple[int, int, int]: 好感度等级，下一等级好感度要求，已达到的好感度要求
    """

    keys = list(lik2level.keys())
    level_int, next_impression, previous_impression = (
        int(lik2level[keys[-1]]),
        keys[-2],
        keys[-1],
    )
    for i in range(len(keys)):
        if impression >= keys[i]:
            level_int, next_impression, previous_impression = (
                int(lik2level[keys[i]]),
                keys[i - 1],
                keys[i],
            )
            if i == 0:
                next_impression = impression
            break
    return level_int, next_impression, previous_impression


def clear_sign_data_pic():
    """
    清空当前签到图片数据
    """
    date = _now().date()
    for file in os.listdir(SIGN_TODAY_CARD_PATH):
        if str(date) not in file:
            os.remove(SIGN_TODAY_CARD_PATH / file)


async def _generate_html_card(
    card_file: Path,
    user: SignUser,
    user_console: UserConsole | None,
    session: Uninfo,
    nickname: str,
    *,
    reward: tuple[float, int, str, bool] | None = None,
    rank: int | None = None,
) -> Path:
    """使用渲染服务生成签到卡片

    参数:
        card_file: 卡片保存路径
        user: SignUser
        user_console: 用户数据
        session: Uninfo
        nickname: 用户昵称
        reward: 签到奖励（好感度、金币、礼物、是否双倍），为 None 时生成查看卡片
        rank: 查看卡片中的好感度排名

    返回:
        Path: 卡片路径
    """
    now = _now()
    is_card_view = reward is None
    impression = float(user.impression)
    if user_console and user_console.uid is not None:
        uid = f"{user_console.uid}".rjust(12, "0")
        uid_formatted = f"{uid[:4]} {uid[4:8]} {uid[8:]}"
    else:
        uid_formatted = "XXXX XXXX XXXX"

    level, next_impression, previous_impression = get_level_and_next_impression(
        impression
    )

    attitude = f"对你的态度: {level2attitude.get(str(level), '未知')}"
    interpolation_val = max(0, next_impression - impression)
    interpolation = f"{interpolation_val:.2f}"

    denominator = next_impression - previous_impression
    progress = (
        100.0
        if denominator == 0
        else min(100.0, ((impression - previous_impression) / denominator) * 100)
    )

    hour = now.hour
    if 6 < hour < 10:
        message = random.choice(MORNING_MESSAGE)
    elif 0 <= hour < 6:
        message = random.choice(LG_MESSAGE)
    else:
        message = f"{BotConfig.self_nickname}希望你开心！"
    bot_message = f"{BotConfig.self_nickname}说: {message}"

    temperature = random.randint(1, 40)
    weather_icon_name = f"{random.randint(0, 11)}.png"
    tag_icon_name = f"{random.randint(0, 5)}.png"

    font_size = 45
    if len(nickname) > 6:
        font_size = 27

    avatar_path = await avatar_service.get_session_avatar_path(session)
    user_info = {
        "nickname": nickname,
        "uid_str": uid_formatted,
        "avatar_url": avatar_path.as_uri() if avatar_path else "",
        "sign_count": user.sign_count,
        "font_size": font_size,
        **_identity_display(session),
    }

    favorability_info = {
        "current": impression,
        "level": level,
        "level_text": f"{level} [{lik2relation.get(str(level), '未知')}]",
        "heart2": [1 for _ in range(level)],
        "heart1": [1 for _ in range(len(lik2level) - level - 1)],
        "next_level_at": next_impression,
        "previous_level_at": previous_impression,
    }

    total_gold = None
    add_impression, gold, gift, is_double = reward or (0, 0, "", False)
    reward_info = {
        "impression_added": add_impression,
        "gold_added": gold,
        "gift_received": gift,
        "is_double": is_double,
    }
    if is_card_view:
        total_gold = user_console.gold if user_console else 0

    page_info = {
        "date_str": str(now.replace(microsecond=0)),
        "weather_icon_name": weather_icon_name,
        "temperature": temperature,
        "tag_icon_name": tag_icon_name,
    }

    card_data = {
        "is_card_view": is_card_view,
        "user": user_info,
        "favorability": favorability_info,
        "reward": reward_info,
        "page": page_info,
        "bot_message": bot_message,
        "attitude": attitude,
        "interpolation": interpolation,
        "progress": progress,
        "rank": rank,
        "total_gold": total_gold,
    }

    image_bytes = await ui.render_template(
        "pages/builtin/sign",
        data=card_data,
        clip_selector=".wrapper",
        clip_padding=0,
        disable_animations=True,
        screenshot_scale="css",
    )

    async with aiofiles.open(card_file, "wb") as f:
        await f.write(image_bytes)

    return card_file
