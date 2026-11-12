import asyncio
from contextlib import asynccontextmanager
from typing import Any

from zhenxun.services.ai.core.exceptions import InterventionHandledException
from zhenxun.services.ai.core.models import CancellationToken
from zhenxun.services.ai.utils.logger import log_flow as logger


class AgentSessionManager:
    """
    Agent 会话运行管理器。
    登记会话中运行或排队的任务，维护同一会话的执行锁与追加消息队列；
    会话没有运行中的任务时立即释放这些记录。
    """

    def __init__(self):
        self.live_tasks: dict[
            str, list[tuple[CancellationToken, asyncio.Task | None]]
        ] = {}
        self._revisions: dict[str, int] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._follow_ups: dict[str, list[Any]] = {}
        self._owners: dict[str, str | None] = {}
        self._in_tools: set[str] = set()

    def revision(self, session_id: str) -> int:
        """会话当前的修订号，会话被重置时递增

        参数:
            session_id: 会话id

        返回:
            int: 修订号
        """
        return self._revisions.get(session_id, 0)

    def invalidate(self, session_id: str) -> int:
        """递增会话修订号并丢弃待并入的输入，此前开始的运行不再写回历史

        参数:
            session_id: 会话id

        返回:
            int: 新的修订号
        """
        revision = self.revision(session_id) + 1
        self._revisions[session_id] = revision
        self._follow_ups.pop(session_id, None)
        return revision

    def drain_follow_ups(self, session_id: str) -> list[Any]:
        """取出会话中等待并入当前运行的用户输入

        参数:
            session_id: 会话 ID

        返回:
            list[Any]: 追加的用户输入
        """
        return self._follow_ups.pop(session_id, [])

    def set_tools_running(self, session_id: str, running: bool) -> None:
        """标记会话当前运行是否正在执行工具，执行期间同一发送者的新消息可以并入

        参数:
            session_id: 会话 ID
            running: 是否正在执行工具
        """
        if running:
            self._in_tools.add(session_id)
        else:
            self._in_tools.discard(session_id)

    @asynccontextmanager
    async def run_scope(
        self,
        session_id: str,
        cancel_token: CancellationToken,
        exclusive: bool,
        follow_up: Any = None,
        sender: str | None = None,
        revision: int | None = None,
    ):
        """登记一次运行；独占运行时同一会话依次执行。会话正在执行工具，且新输入
        与当前运行来自同一发送者时，把输入并入当前运行，否则排队等待

        参数:
            session_id: 会话 ID
            cancel_token: 本次运行的取消令牌
            exclusive: 是否与同一会话的其他独占运行互斥
            follow_up: 可以并入当前运行的用户输入，为 None 时总是排队
            sender: 发送者 ID
            revision: 发起时的会话修订号，取得锁时会话已被重置则不再运行

        异常:
            InterventionHandledException: 输入已并入正在运行的任务
        """
        entry = (cancel_token, asyncio.current_task())
        tasks = self.live_tasks.setdefault(session_id, [])
        tasks.append(entry)
        try:
            if not exclusive:
                yield
                return
            lock = self._locks.setdefault(session_id, asyncio.Lock())
            if lock.locked():
                if (
                    follow_up is not None
                    and sender
                    and session_id in self._in_tools
                    and self._owners.get(session_id) == sender
                ):
                    self._follow_ups.setdefault(session_id, []).append(follow_up)
                    raise InterventionHandledException("Follow-up successful")
                logger.info(f"⏳ [并发控制] 会话 {session_id} 正忙，新请求排队等待...")
            async with lock:
                cancel_token.raise_if_cancelled()
                if revision is not None and revision != self.revision(session_id):
                    raise asyncio.CancelledError("会话已重置")
                self._owners[session_id] = sender
                try:
                    yield
                finally:
                    self._owners.pop(session_id, None)
                    self._in_tools.discard(session_id)
                    self._follow_ups.pop(session_id, None)
        finally:
            tasks.remove(entry)
            if not tasks:
                self.live_tasks.pop(session_id, None)
                self._locks.pop(session_id, None)


session_manager = AgentSessionManager()
