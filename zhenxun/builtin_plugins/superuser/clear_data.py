import os
import shutil
import time

from nonebot.permission import SUPERUSER
from nonebot.plugin import PluginMetadata
from nonebot.rule import to_me
from nonebot.utils import run_sync
from nonebot_plugin_alconna import Alconna, on_alconna
from nonebot_plugin_apscheduler import scheduler
from nonebot_plugin_session import EventSession

from zhenxun.configs.path_config import DATA_PATH, TEMP_PATH
from zhenxun.configs.utils import PluginExtraData
from zhenxun.models.asset_operation import AssetOperation
from zhenxun.services.log import logger
from zhenxun.utils.enum import PluginType
from zhenxun.utils.manager.priority_manager import PriorityLifecycle
from zhenxun.utils.message import MessageUtils
from zhenxun.utils.utils import ResourceDirManager

__plugin_meta__ = PluginMetadata(
    name="清理数据",
    description="清理已添加的临时文件夹中的数据",
    usage="""
    清理临时数据
    """.strip(),
    extra=PluginExtraData(
        author="HibiKier",
        version="0.1",
        plugin_type=PluginType.SUPERUSER,
    ).to_dict(),
)


_matcher = on_alconna(
    Alconna("清理临时数据"),
    rule=to_me(),
    permission=SUPERUSER,
    priority=5,
    block=True,
)


ResourceDirManager.add_temp_dir(TEMP_PATH, True)


@PriorityLifecycle.on_startup(priority=5)
async def _remove_legacy_inbox():
    """删除旧版消息收件箱遗留的数据目录"""
    path = DATA_PATH / "runtime" / "message-inbox"
    if path.exists():
        await run_sync(shutil.rmtree)(path, ignore_errors=True)
        logger.info("已删除旧版消息收件箱数据目录", "清理临时数据")


@_matcher.handle()
async def _(session: EventSession):
    await MessageUtils.build_message("开始清理临时数据...").send()
    size = await _clear_data()
    await MessageUtils.build_message(
        f"共清理了 {size / 1024 / 1024:.2f}MB 的数据..."
    ).send()
    logger.info(
        f"清理临时数据完成，共清理了 {size / 1024 / 1024:.2f}MB 的数据...",
        session=session,
    )


@run_sync
def _clear_data(report: dict | None = None) -> float:
    """清理过期临时文件并可选返回逐项处理数量。"""
    report = report if report is not None else {}
    report.update(deleted=0, errors=0)
    logger.debug("开始清理临时文件...")
    size = 0
    dir_list = [dir_ for dir_ in ResourceDirManager.temp_path if dir_.exists()]
    for dir_ in dir_list:
        logger.debug(f"尝试清理文件夹: {dir_.absolute()}", "清理临时数据")
        dir_size = 0
        try:
            files = os.listdir(dir_)
        except OSError as error:
            report["errors"] += 1
            logger.error("无法读取临时目录", "清理临时数据", e=error)
            continue
        for file in files:
            file = dir_ / file
            if file.is_file():
                try:
                    if time.time() - os.path.getatime(file) > 10:
                        file_size = os.path.getsize(file)
                        file.unlink()
                        report["deleted"] += 1
                        size += file_size
                        dir_size += file_size
                        logger.debug(f"移除临时文件: {file.absolute()}", "清理临时数据")
                except Exception as e:
                    report["errors"] += 1
                    logger.error(
                        f"清理临时数据错误，临时文件夹: {dir_.absolute()}...",
                        "清理临时数据",
                        e=e,
                    )
        logger.debug(f"清理临时文件夹大小: {size / 1024 / 1024:.2f}MB", "清理临时数据")
    return float(size)


@scheduler.scheduled_job(
    "cron",
    hour=1,
    minute=1,
)
async def _():
    from zhenxun.services.scheduler.lifecycle import core_task_run

    async with core_task_run("temporary_and_asset_cleanup") as run:
        run.update(failed=0, bytes=0, asset_receipts=0)
        file_report = {"deleted": 0, "errors": 0}
        try:
            size = await _clear_data(file_report)
            run["bytes"] = size
        except Exception as error:
            run["failed"] += 1
            logger.error("自动清理临时数据失败", "定时任务", e=error)
        run["files_deleted"] = file_report["deleted"]
        run["failed"] += file_report["errors"]
        run["processed"] = file_report["deleted"]
        try:
            count = await AssetOperation.clear_expired()
            run["asset_receipts"] = count
            run["processed"] += count
        except Exception as error:
            run["failed"] += 1
            logger.error("自动清理过期资产回执失败", "定时任务", e=error)
