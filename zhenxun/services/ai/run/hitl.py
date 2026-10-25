from __future__ import annotations

import asyncio
from collections.abc import Callable
import time

from nonebot.adapters import Event
from nonebot.permission import SUPERUSER
from nonebot_plugin_waiter import waiter

from zhenxun.services.ai.core.exceptions import AbortException, ToolFatalError
from zhenxun.services.ai.utils.logger import log_agent as logger

from .context import RunContext

CANCEL_WORDS = {"取消", "cancel", "0", "退出", "quit"}
CONFIRM_WORDS = {"y", "yes", "是", "1", "ok", "确认"}
REJECT_WORDS = {"n", "no", "否", "0", "拒绝"}


class HITLController:
    """
    人机协同控制器 (Human-in-the-Loop Controller)。
    封装底层的物理环境隔离等待逻辑，供上层工具和中间件发起提问或审批。
    """

    def __init__(self, context: RunContext):
        self.context = context

    async def wait_event(
        self,
        prompt_msg: str | None = None,
        timeout: float = 60.0,
        custom_checker: Callable[[Event], bool] | None = None,
        strict_user: bool = True,
    ) -> Event | None:
        """
        底层交互方法：进行严格群组与用户隔离的挂起等待。
        """
        bot = self.context.get_bot()
        event = self.context.get_event()

        if not bot or not event:
            logger.warning("HITLController: 当前环境无 Bot/Event 实例，无法发起交互。")
            raise ToolFatalError(
                "当前处于自动化后台调度环境，无法发起人工交互。任务已强行中止。"
            )

        if prompt_msg:
            await bot.send(event, prompt_msg)

        orig_user_id = self.context.get_user_id()
        orig_group_id = self.context.get_group_id()

        @waiter(waits=["message"], keep_session=False)
        async def event_waiter(e: Event):
            raw_curr_group = getattr(e, "group_id", getattr(e, "channel_id", None))

            curr_group_id = str(raw_curr_group) if raw_curr_group else None
            tgt_group_id = str(orig_group_id) if orig_group_id else None
            if curr_group_id != tgt_group_id:
                return None

            if strict_user and str(e.get_user_id()) != orig_user_id:
                return None

            if custom_checker and not custom_checker(e):
                return None

            return e

        task = asyncio.create_task(event_waiter.wait(timeout=timeout))
        cancellation_token = self.context.run.cancellation_token
        if cancellation_token:
            cancellation_token.link_future(task)

        return await task

    async def ask_text(self, prompt_msg: str, timeout: float = 60.0) -> str:
        """
        高阶交互：发起文本提问，内置超时与取消词检测。
        如用户取消或超时，自动抛出异常切断 Agent 思考循环。
        """
        event = await self.wait_event(prompt_msg, timeout=timeout, strict_user=True)
        if event is None:
            logger.warning("🛡️ [HITL] 文本参数收集超时，任务已被系统强杀。")
            raise ToolFatalError("参数收集超时，用户已离开，任务已中止。")

        user_input = event.get_plaintext().strip()
        if user_input.lower() in CANCEL_WORDS:
            raise AbortException(reason="用户主动取消了操作")

        return user_input

    async def ask_confirm(self, prompt_msg: str, timeout: float = 60.0) -> bool:
        """发起高危操作审批，仅超级用户可以批准，发起人与超级用户可以拒绝

        参数:
            prompt_msg: 审批说明
            timeout: 超时时间，秒

        返回:
            bool: 批准时为 True，拒绝或超时抛出异常
        """
        orig_user_id = self.context.get_user_id()
        bot = self.context.get_bot()

        if not bot:
            raise ToolFatalError("非交互环境无法发起人工审批。")

        full_prompt = (
            f"{prompt_msg}\n\n需要超级用户在规定时间内回复 [Y/是] 批准，"
            "发起人或超级用户回复 [N/否/取消] 拒绝。"
        )

        start_time = time.time()
        is_first_prompt = True

        while True:
            remaining = timeout - (time.time() - start_time)
            if remaining <= 0:
                raise ToolFatalError("审批超时")

            event = await self.wait_event(
                prompt_msg=full_prompt if is_first_prompt else None,
                timeout=remaining,
                strict_user=False,
            )
            is_first_prompt = False

            if event is None:
                raise ToolFatalError("审批超时")

            text = event.get_plaintext().strip().lower()
            if (
                text not in CONFIRM_WORDS
                and text not in REJECT_WORDS
                and text not in CANCEL_WORDS
            ):
                continue

            is_superuser = await SUPERUSER(bot, event)
            is_requester = str(event.get_user_id()) == orig_user_id
            if text in CONFIRM_WORDS:
                if is_superuser:
                    return True
                if is_requester:
                    await bot.send(event, "⚠️ 该操作需要超级用户批准。")
                continue
            if is_superuser or is_requester:
                raise AbortException(reason="审批被拒绝")
