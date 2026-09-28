from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

import nonebot
from nonebot_plugin_alconna import UniMessage
from playwright.async_api import Page

from zhenxun.utils.message import MessageUtils


class BrowserIsNone(Exception):
    pass


class AsyncPlaywright:
    @classmethod
    @asynccontextmanager
    async def new_page(
        cls, cookies: list[dict[str, Any]] | dict[str, Any] | None = None, **kwargs
    ) -> AsyncGenerator[Page, None]:
        """获取一个新页面

        参数:
            cookies: cookies
        """
        nonebot.require("nonebot_plugin_htmlrender")
        from nonebot_plugin_htmlrender.browser import get_browser

        from zhenxun.services.renderer.engine import _HTMLRENDER_TASK_TRACKER
        from zhenxun.services.renderer.resources import browser_resources

        async with _HTMLRENDER_TASK_TRACKER.track("browser_utils"):
            browser = await get_browser()
            ctx = await browser_resources.create_context(
                browser, owner="browser_utils", **kwargs
            )
            page = None
            failed = False
            try:
                if cookies:
                    if isinstance(cookies, dict):
                        cookies = [cookies]
                    await ctx.add_cookies(cookies)  # type: ignore
                page = await ctx.new_page()
                yield page
            except BaseException:
                failed = True
                raise
            finally:
                try:
                    await browser_resources.cleanup(
                        browser_resources.finish_page(page, ctx, retain=False),
                        name="renderer-browser-utils-cleanup",
                    )
                except Exception:
                    if not failed:
                        raise

    @classmethod
    async def screenshot(
        cls,
        url: str,
        path: Path | str,
        element: str | list[str],
        *,
        wait_time: int | None = None,
        viewport_size: dict[str, int] | None = None,
        wait_until: (
            Literal["domcontentloaded", "load", "networkidle"] | None
        ) = "networkidle",
        timeout: float | None = None,
        type_: Literal["jpeg", "png"] | None = None,
        user_agent: str | None = None,
        cookies: list[dict[str, Any]] | dict[str, Any] | None = None,
        **kwargs,
    ) -> UniMessage | None:
        """截图，该方法仅用于简单快捷截图，复杂截图请操作 page

        参数:
            url: 网址
            path: 存储路径
            element: 元素选择
            wait_time: 等待截取超时时间
            viewport_size: 窗口大小
            wait_until: 等待类型
            timeout: 超时限制
            type_: 保存类型
            user_agent: user_agent
            cookies: cookies
        """
        if viewport_size is None:
            viewport_size = {"width": 2560, "height": 1080}
        if isinstance(path, str):
            path = Path(path)
        wait_time = wait_time * 1000 if wait_time else None
        element_list = [element] if isinstance(element, str) else element
        async with cls.new_page(
            cookies,
            viewport=viewport_size,
            user_agent=user_agent,
            **kwargs,
        ) as page:
            await page.goto(url, timeout=timeout, wait_until=wait_until)
            card = page
            for e in element_list:
                if not card:
                    return None
                card = await card.wait_for_selector(e, timeout=wait_time)
            if card:
                await card.screenshot(path=path, timeout=timeout, type=type_)
                return MessageUtils.build_message(path)
        return None
