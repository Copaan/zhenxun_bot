import sys

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import nonebot

from zhenxun.services.startup import startup_coordinator

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from strenum import StrEnum


PRIVATE_ORIGIN_REGEX = (
    r"^https?://(?:localhost|127(?:\.\d{1,3}){3}|10(?:\.\d{1,3}){3}|"
    r"192\.168(?:\.\d{1,3}){2}|172\.(?:1[6-9]|2\d|3[01])"
    r"(?:\.\d{1,3}){2}|\[::1\])(?::\d{1,5})?$"
)


def install_cors_middleware(app: FastAPI | None = None) -> bool:
    """Install the WebUI CORS boundary before the ASGI application starts."""
    target = app or nonebot.get_app()
    if any(item.cls is CORSMiddleware for item in target.user_middleware):
        return True
    if target.middleware_stack is not None:
        return False
    target.add_middleware(
        CORSMiddleware,
        allow_origin_regex=PRIVATE_ORIGIN_REGEX,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-Setup-Token"],
    )
    if not getattr(target.state, "zhenxun_startup_guard", False):

        @target.middleware("http")
        async def startup_mutation_guard(request: Request, call_next):
            path = request.url.path
            recovery_prefixes = (
                "/zhenxun/api/auth",
                "/zhenxun/api/configure",
                "/zhenxun/api/system/restart",
                "/zhenxun/api/system/update",
            )
            if (
                request.method in {"POST", "PUT", "PATCH", "DELETE"}
                and path.startswith("/zhenxun/api/")
                and not path.startswith(recovery_prefixes)
                and not startup_coordinator.runtime_ready
            ):
                snapshot = startup_coordinator.snapshot()
                return JSONResponse(
                    status_code=409,
                    content={
                        "code": 409,
                        "message": "startup_in_progress",
                        "suc": False,
                        "data": {
                            "state": snapshot["state"],
                            "current_operation": snapshot.get("current_operation"),
                            "load_plan": snapshot.get("load_plan"),
                        },
                    },
                )
            return await call_next(request)

        target.state.zhenxun_startup_guard = True
    return True


AVA_URL = "http://q1.qlogo.cn/g?b=qq&nk={}&s=160"

GROUP_AVA_URL = "http://p.qlogo.cn/gh/{}/{}/640/"


class QueryDateType(StrEnum):
    """
    查询日期类型
    """

    DAY = "day"
    """日"""
    WEEK = "week"
    """周"""
    MONTH = "month"
    """月"""
    YEAR = "year"
    """年"""
