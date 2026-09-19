from datetime import datetime
from pathlib import Path
import random
import secrets

from nonebot_plugin_uninfo import Uninfo
import pytz

from zhenxun import ui
from zhenxun.configs.path_config import IMAGE_PATH
from zhenxun.models.asset_operation import AssetOperation
from zhenxun.models.friend_user import FriendUser
from zhenxun.models.goods_info import GoodsInfo
from zhenxun.models.sign_log import SignLog
from zhenxun.models.sign_user import SignUser
from zhenxun.models.user_console import UserConsole
from zhenxun.services.account_binding import identity_daily_claimed
from zhenxun.services.asset_transaction import (
    asset_call,
    asset_transaction,
    current_asset_connection,
)
from zhenxun.services.avatar_service import avatar_service
from zhenxun.services.business_identity import (
    BusinessIdentityError,
    business_user_id,
    official_group_business_keys,
)
from zhenxun.services.hot_query_cache import get_group_user_ids, get_member_names
from zhenxun.services.log import logger
from zhenxun.services.message_execution import current_execution, operation_key
from zhenxun.ui.models import ImageCell, TextCell
from zhenxun.utils.exception import GoodsNotFound
from zhenxun.utils.platform import PlatformUtils

from ._random_event import random_event
from .utils import get_card

ICON_PATH = IMAGE_PATH / "_icon"

PLATFORM_PATH = {
    "dodo": ICON_PATH / "dodo.png",
    "discord": ICON_PATH / "discord.png",
    "kaiheila": ICON_PATH / "kook.png",
    "qq": ICON_PATH / "qq.png",
}


class SignManage:
    @classmethod
    async def rank(
        cls, session: Uninfo, num: int, group_id: str | None = None
    ) -> bytes | str:
        """好感度排行

        参数:
            session: Uninfo
            num: 排行榜数量
            group_id: 群组id

        返回:
            bytes: 构造图片
        """
        query = SignUser
        own_key = await business_user_id(session)
        if group_id:
            if PlatformUtils.get_platform_scope(session) == "qq_api":
                try:
                    user_list = await official_group_business_keys(session)
                except BusinessIdentityError as error:
                    return str(error)
                query = query.filter(user_id__in=user_list)
            elif user_list := await get_group_user_ids(group_id):
                query = query.filter(user_id__in=user_list)
        user_list = (
            await query.annotate()
            .order_by("-impression")
            .values_list("user_id", "impression", "sign_count", "platform")
        )
        if not user_list:
            return "当前还没有人签到过哦..."
        user_id_list = [user[0] for user in user_list]
        if own_key in user_id_list:
            index = user_id_list.index(own_key) + 1
        else:
            index = "-1（未统计）"
        user_list = user_list[:num] if num < len(user_list) else user_list
        column_name = ["排名", "-", "名称", "好感度", "签到次数", "平台"]
        friend_list = await FriendUser.filter(user_id__in=user_id_list).values_list(
            "user_id", "user_name"
        )
        uid2name = {f[0]: f[1] for f in friend_list}
        if diff_id := set(user_id_list).difference(set(uid2name.keys())):
            uid2name.update(await get_member_names(diff_id))
        data_list = []
        platform = PlatformUtils.get_platform(session)
        for i, user in enumerate(user_list):
            avatar_path = await avatar_service.get_avatar_path(
                platform=user[3] or "qq", identifier=user[0]
            )
            data_list.append(
                [
                    TextCell(content=f"{i + 1}"),
                    ImageCell(
                        src=avatar_path.as_uri() if avatar_path else "", shape="circle"
                    )
                    if avatar_path
                    else TextCell(content=""),
                    TextCell(content=uid2name.get(user[0]) or user[0]),
                    TextCell(content=str(user[1]), bold=True),
                    TextCell(content=str(user[2])),
                    ImageCell(src=platform_path.resolve().as_uri())
                    if (platform_path := PLATFORM_PATH.get(platform))
                    else TextCell(content=""),
                ]
            )
        if group_id:
            title = "好感度群组内排行"
            tip = f"你的排名在本群第 {index} 位哦!"
        else:
            title = "好感度全局排行"
            tip = f"你的排名在全局第 {index} 位哦!"

        table = ui.table(title, tip)
        table.set_headers(column_name).add_rows(data_list)
        return await ui.render(table)

    @classmethod
    async def sign(
        cls, session: Uninfo, nickname: str, is_card_view: bool = False
    ) -> Path:
        """签到

        参数:
            session: Uninfo
            nickname: 用户昵称
            is_card_view: 是否展示卡片

        返回:
            Path: 卡片路径
        """
        card_args = await cls._commit_sign(session, nickname, is_card_view)
        return await get_card(*card_args, is_card_view=is_card_view)

    @classmethod
    @asset_call
    async def _commit_sign(cls, session, nickname, is_card_view):
        platform = PlatformUtils.get_platform(session)
        now = datetime.now(pytz.timezone("Asia/Shanghai"))
        user_id = await business_user_id(session)
        receipt_id = None if is_card_view else operation_key("sign.reward", user_id)
        async with asset_transaction(user_id, platform) as user_console:
            user, _ = await SignUser.get_or_create(
                using_db=current_asset_connection(),
                user_id=user_id,
                defaults={"user_console": user_console, "platform": platform},
            )
            if (
                receipt_id
                and await AssetOperation.filter(id=receipt_id)
                .using_db(current_asset_connection())
                .exists()
            ):
                return (user, session, nickname, -1, user_console.gold, "")
            new_log = (
                await SignLog.filter(user_id=user_id)
                .using_db(current_asset_connection())
                .order_by("-create_time")
                .first()
            )
            log_time = (
                new_log.create_time.astimezone(pytz.timezone("Asia/Shanghai")).date()
                if new_log
                else None
            )
            if (
                not is_card_view
                and log_time != now.date()
                and not await identity_daily_claimed(current_asset_connection())
            ):
                card_args = await cls._handle_sign_in(user, nickname, session)
            else:
                card_args = (user, session, nickname, -1, user_console.gold, "")
            if receipt_id:
                await AssetOperation.create(
                    using_db=current_asset_connection(),
                    id=receipt_id,
                    user_id=user_id,
                    event_id=current_execution.get().identity,
                    kind="sign_reward",
                    state="committed",
                    payload={"day": str(now.date()), "rewarded": card_args[3] != -1},
                )
        return card_args

    @classmethod
    async def _handle_sign_in(
        cls,
        user: SignUser,
        nickname: str,
        session: Uninfo,
    ) -> tuple:
        """签到处理

        参数:
            user: SignUser
            nickname: 用户昵称
            session: Uninfo

        返回:
            Path: 卡片路径
        """
        platform = PlatformUtils.get_platform(session)
        impression_added = (secrets.randbelow(99) + 1) / 100
        rand = random.random()
        add_probability = float(user.add_probability)
        specify_probability = float(user.specify_probability)
        if rand + add_probability > 0.97 or rand < specify_probability:
            impression_added *= 2
        user = await SignUser.sign(user, impression_added, session.self_id, platform)
        gold = random.randint(1, 100)
        gift = random_event(float(user.impression))
        if isinstance(gift, int):
            gold += gift
            await UserConsole.add_gold(user.user_id, gold, "sign_in", platform)
            gift = f"额外金币 +{gift}"
        else:
            goods = await GoodsInfo.get_or_none(goods_name=gift)
            if not goods:
                raise GoodsNotFound("未找到商品...")
            await UserConsole.add_gold(user.user_id, gold, "sign_in", platform)
            await UserConsole.add_props(user.user_id, goods.uuid, 1, platform)
            gift += " + 1"
        logger.info(
            f"签到成功. score: {user.impression:.2f} "
            f"(+{impression_added:.2f}).获取金币/道具: {gold}",
            "签到",
            session=session,
        )
        return (
            user,
            session,
            nickname,
            impression_added,
            gold,
            gift,
            rand + add_probability > 0.97 or rand < specify_probability,
        )
