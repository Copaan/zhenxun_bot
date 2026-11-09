import asyncio
from datetime import datetime
from pathlib import Path
import uuid

import nonebot
from nonebot.adapters import Bot
from nonebot.drivers import Driver
from tortoise import Tortoise
from tortoise.exceptions import OperationalError
import ujson as json

from zhenxun.models.bot_connect_log import BotConnectLog
from zhenxun.models.goods_info import GoodsInfo
from zhenxun.models.group_member_info import GroupInfoUser
from zhenxun.models.sign_user import SignUser
from zhenxun.models.user_console import UserConsole
from zhenxun.services.db_context import database_ready
from zhenxun.services.log import logger
from zhenxun.utils.decorator.shop import shop_register
from zhenxun.utils.manager.priority_manager import PriorityLifecycle
from zhenxun.utils.platform import PlatformUtils

driver: Driver = nonebot.get_driver()


@driver.on_bot_connect
async def _(bot: Bot):
    logger.debug(f"Bot: {bot.self_id} 建立连接...")
    if not database_ready():
        logger.warning("bot_connect_database_not_ready: 跳过连接记录，数据库尚未就绪")
        return
    storage_bot_id = PlatformUtils.get_storage_bot_id(bot)
    await BotConnectLog.create(
        bot_id=storage_bot_id,
        platform=bot.adapter,
        connect_time=datetime.now(),
        type=1,
    )


@driver.on_bot_disconnect
async def _(bot: Bot):
    logger.debug(f"Bot: {bot.self_id} 断开连接...")
    if not database_ready():
        logger.debug("bot_disconnect_database_not_ready: 跳过断连记录")
        return
    try:
        await BotConnectLog.create(
            bot_id=PlatformUtils.get_storage_bot_id(bot),
            platform=bot.adapter,
            connect_time=datetime.now(),
            type=0,
        )
    except Exception as e:
        logger.warning(f"记录bot: {bot.self_id} 断开连接失败", e=e)


SIGN_SQL = """
SELECT user_id, checkin_count, add_probability, specify_probability, impression
FROM (
    SELECT
        t1.user_id,
        t1.checkin_count,
        t1.add_probability,
        t1.specify_probability,
        t1.impression,
        ROW_NUMBER() OVER(PARTITION BY t1.user_id ORDER BY t1.impression DESC) AS rn
    FROM sign_group_users t1
    INNER JOIN (
        SELECT user_id, MAX(impression) AS max_impression
        FROM sign_group_users
        GROUP BY user_id
    ) t2 ON t2.user_id = t1.user_id AND t2.max_impression = t1.impression
) t
WHERE rn = 1
"""

BAG_SQL = """
select t1.user_id, t1.gold, t1.property
from bag_users t1
  join (
    select user_id, max(t2.gold) as max_gold
    from bag_users t2
    group by user_id
  ) t on t.user_id = t1.user_id and t.max_gold = t1.gold
"""


@PriorityLifecycle.on_startup(
    priority=10,
    stage="warmup",
    timeout=300,
    parallel_safe=True,
    failure_policy="degrade",
    component_id="warmup:resources",
)
async def _prepare_resources():
    from zhenxun.services.resource_install import resources_ready

    if not await asyncio.to_thread(
        resources_ready, Path("resources"), Path("resources.spec")
    ):
        raise RuntimeError("resources_waiting_for_update")


@PriorityLifecycle.on_startup(
    priority=10,
    stage="management",
    timeout=300,
    component_id="management:resource-update",
)
async def _start_resource_update():
    from zhenxun.services.update.update_service import start_resource_update

    start_resource_update()


@PriorityLifecycle.on_shutdown(priority=10, component_id="management:resource-update")
async def _stop_resource_update():
    from zhenxun.services.update.update_service import stop_resource_update

    await stop_resource_update()


@PriorityLifecycle.on_startup(
    priority=5,
    stage="runtime",
    timeout=300,
    failure_policy="fatal",
    component_id="runtime:legacy_data",
    depends_on=("management:database", "runtime:reconcile_tasks"),
)
async def _migrate_legacy_data():
    """签到与用户的数据迁移"""
    if goods_list := await GoodsInfo.filter(uuid__isnull=True).all():
        for goods in goods_list:
            goods.uuid = uuid.uuid1()  # type: ignore
        # 使用逐条保存替代 bulk_update，避免 SQLite 兼容性问题
        for goods in goods_list:
            await goods.save(update_fields=["uuid"])
    await shop_register.load_register()
    if (
        not await UserConsole.annotate().count()
        and not await SignUser.annotate().count()
    ):
        try:
            group_user = []
            try:
                group_user = await GroupInfoUser.filter(uid__isnull=False).all()
            except Exception as e:
                logger.warning("获取GroupInfoUser数据uid失败...", e=e)
            user2uid = {u.user_id: u.uid for u in group_user}
            db = Tortoise.get_connection("default")
            try:
                old_sign_list = await db.execute_query_dict(SIGN_SQL)
            except OperationalError as e:
                if "no such table" in str(e).lower() or "sign_group_users" in str(e):
                    # 旧签到表不存在，说明是全新环境或已完成过迁移，正常跳过
                    logger.debug("旧签到表 sign_group_users 不存在，跳过数据迁移")
                    old_sign_list = []
                else:
                    raise
            try:
                old_bag_list = await db.execute_query_dict(BAG_SQL)
            except OperationalError as e:
                if "no such table" in str(e).lower() or "bag_users" in str(e):
                    logger.debug("旧背包表 bag_users 不存在，跳过数据迁移")
                    old_bag_list = []
                else:
                    raise
            goods = {
                g["goods_name"]: g["uuid"]
                for g in await GoodsInfo.annotate().values("goods_name", "uuid")
            }
            create_list = []
            sign_id_list = []
            max_uid = max(user2uid.values()) + 1 if user2uid else 0
            for old_sign in old_sign_list:
                sign_id_list.append(old_sign["user_id"])
                if old_bag := [
                    b for b in old_bag_list if b["user_id"] == old_sign["user_id"]
                ]:
                    old_bag = old_bag[0]
                    property = json.loads(old_bag["property"])
                    props = {}
                    if property:
                        for name, num in property.items():
                            if name in goods:
                                props[goods[name]] = num
                    create_list.append(
                        UserConsole(
                            user_id=old_sign["user_id"],
                            platform="qq",
                            uid=user2uid.get(old_sign["user_id"]) or max_uid,
                            props=props,
                            gold=old_bag["gold"],
                        )
                    )
                    if not user2uid.get(old_sign["user_id"]):
                        max_uid += 1
                else:
                    create_list.append(
                        UserConsole(
                            user_id=old_sign["user_id"], platform="qq", uid=max_uid
                        )
                    )
                    max_uid += 1
            if create_list:
                logger.info("开始迁移用户数据...")
                await UserConsole.bulk_create(create_list, 10)
                logger.info("迁移用户数据完成!")
            create_list.clear()
            uc_dict = {u.user_id: u for u in await UserConsole.all()}
            for old_sign in old_sign_list:
                user_console = uc_dict.get(
                    old_sign["user_id"]
                ) or await UserConsole.get_user(old_sign["user_id"], "qq")
                create_list.append(
                    SignUser(
                        user_id=old_sign["user_id"],
                        user_console=user_console,
                        platform="qq",
                        sign_count=old_sign["checkin_count"],
                        impression=old_sign["impression"],
                        add_probability=old_sign["add_probability"],
                        specify_probability=old_sign["specify_probability"],
                    )
                )
            if create_list:
                logger.info("开始迁移签到数据...")
                await SignUser.bulk_create(create_list, 10)
                logger.info("迁移签到数据完成!")
        except OperationalError as e:
            logger.warning("数据迁移", e=e)
            raise
