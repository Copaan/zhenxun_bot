import asyncio
from collections.abc import Callable
import functools
import inspect
import ipaddress
import threading
from urllib.parse import urljoin, urlsplit


def wrap_to_async(func: Callable) -> Callable:
    """
    将任意类型的函数（同步、异步、同步生成器、异步生成器）统一包装为异步执行形态。
    确保不会阻塞主线程，并保留原函数的元数据和签名。
    """
    if getattr(func, "__is_async_wrapper__", False):
        return func

    if inspect.isasyncgenfunction(func) or inspect.iscoroutinefunction(func):
        return func

    if inspect.isgeneratorfunction(func):

        async def async_gen_wrapper(*args, **kwargs):
            loop = asyncio.get_running_loop()
            q = asyncio.Queue()
            _sentinel = object()

            def _sync_runner():
                try:
                    for item in func(*args, **kwargs):
                        asyncio.run_coroutine_threadsafe(q.put(item), loop).result()
                    asyncio.run_coroutine_threadsafe(q.put(_sentinel), loop).result()
                except Exception as e:
                    asyncio.run_coroutine_threadsafe(q.put(e), loop).result()

            t = threading.Thread(target=_sync_runner)
            t.start()

            while True:
                item = await q.get()
                if item is _sentinel:
                    break
                if isinstance(item, Exception):
                    raise item
                yield item

        functools.update_wrapper(async_gen_wrapper, func)
        setattr(async_gen_wrapper, "__is_async_wrapper__", True)
        setattr(async_gen_wrapper, "_is_async_gen", True)
        return async_gen_wrapper

    async def async_wrapper(*args, **kwargs):
        p_func = functools.partial(func, *args, **kwargs)
        return await asyncio.to_thread(p_func)

    functools.update_wrapper(async_wrapper, func)
    setattr(async_wrapper, "__is_async_wrapper__", True)
    setattr(async_wrapper, "_is_coroutine", True)
    return async_wrapper


MEDIA_MAX_BYTES = 20 * 1024 * 1024
"""单个媒体允许下载的最大字节数"""
_MEDIA_MAX_REDIRECTS = 5
_FAKE_IP_NETWORK = ipaddress.ip_network("198.18.0.0/15")
"""代理软件 fake-ip 模式使用的地址段，域名都会解析到这里，不视为内网"""


class MediaFetchError(Exception):
    """媒体下载失败"""

    def __init__(self, message: str, too_large: bool = False):
        super().__init__(message)
        self.too_large = too_large


async def _ensure_public_host(host: str) -> None:
    """确认主机解析到的地址都是公网地址

    参数:
        host: 主机名或 IP

    异常:
        MediaFetchError: 解析到内网、回环、链路本地或保留地址
    """
    try:
        addresses = [ipaddress.ip_address(host.strip("[]"))]
    except ValueError:
        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(host, None)
        except OSError:
            return
        addresses = [
            ipaddress.ip_address(str(info[4][0]).split("%")[0]) for info in infos
        ]
    for address in addresses:
        if address.is_global or address in _FAKE_IP_NETWORK:
            continue
        raise MediaFetchError(f"拒绝访问非公网地址: {host}")


async def fetch_media(
    url: str, max_bytes: int = MEDIA_MAX_BYTES, allow_private: bool = False
) -> bytes:
    """下载媒体内容，限制大小并逐跳检查重定向目标

    参数:
        url: http/https 地址
        max_bytes: 允许的最大字节数
        allow_private: 是否允许访问内网地址（仅用于协议端提供的地址）

    返回:
        bytes: 媒体内容

    异常:
        MediaFetchError: 地址不合法、访问被拒绝、请求失败或超过大小上限
    """
    from zhenxun.utils.http_utils import get_client

    client = get_client()
    for _ in range(_MEDIA_MAX_REDIRECTS + 1):
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise MediaFetchError(f"只能下载 http/https 地址: {url[:80]}")
        if not allow_private:
            await _ensure_public_host(parts.hostname)
        try:
            async with client.stream(
                "GET", url, follow_redirects=False, timeout=30
            ) as response:
                if response.is_redirect and "location" in response.headers:
                    url = urljoin(url, response.headers["location"])
                    continue
                if response.status_code >= 400:
                    raise MediaFetchError(f"下载失败 HTTP {response.status_code}")
                length = response.headers.get("content-length", "")
                if length.isdigit() and int(length) > max_bytes:
                    raise MediaFetchError("媒体超过大小上限", too_large=True)
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > max_bytes:
                        raise MediaFetchError("媒体超过大小上限", too_large=True)
                    chunks.append(chunk)
                return b"".join(chunks)
        except MediaFetchError:
            raise
        except Exception as e:
            raise MediaFetchError(f"下载失败: {type(e).__name__} {e}") from e
    raise MediaFetchError("重定向次数过多")
