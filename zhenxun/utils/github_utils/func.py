from aiocache import cached

from zhenxun.utils.http_utils import AsyncHttpx

from .const import (
    ARCHIVE_URL_FORMAT,
    CACHED_API_TTL,
    RAW_CONTENT_FORMAT,
    RELEASE_ASSETS_FORMAT,
    RELEASE_SOURCE_FORMAT,
)


async def __get_fastest_formats(formats: dict[str, str], origin: str) -> list[str]:
    """测速后排列下载地址格式，GitHub 源站可访问时排在首位，镜像按延迟排在其后

    参数:
        formats: 测速地址 -> 下载地址格式
        origin: GitHub 源站的测速地址

    返回:
        list[str]: 可访问的下载地址格式
    """
    sorted_urls = await AsyncHttpx.get_fastest_mirror(list(formats.keys()))
    if not sorted_urls:
        raise Exception("无法获取任意GitHub资源加速地址，请检查网络")
    if origin in sorted_urls:
        sorted_urls.remove(origin)
        sorted_urls.insert(0, origin)
    return [formats[url] for url in sorted_urls]


@cached(ttl=CACHED_API_TTL)
async def get_fastest_raw_formats() -> list[str]:
    """获取最快的raw下载地址格式"""
    formats: dict[str, str] = {
        "https://raw.githubusercontent.com/": RAW_CONTENT_FORMAT,
        "https://gh-proxy.com/": f"https://gh-proxy.com/{RAW_CONTENT_FORMAT}",
        "https://cdn.jsdelivr.net/": "https://cdn.jsdelivr.net/gh/{owner}/{repo}@{branch}/{path}",
    }
    return await __get_fastest_formats(formats, "https://raw.githubusercontent.com/")


@cached(ttl=CACHED_API_TTL)
async def get_fastest_archive_formats() -> list[str]:
    """获取最快的归档下载地址格式"""
    formats: dict[str, str] = {
        "https://github.com/": ARCHIVE_URL_FORMAT,
        "https://gh-proxy.com/": f"https://gh-proxy.com/{ARCHIVE_URL_FORMAT}",
    }
    return await __get_fastest_formats(formats, "https://github.com/")


@cached(ttl=CACHED_API_TTL)
async def get_fastest_release_formats() -> list[str]:
    """获取最快的发行版资源下载地址格式"""
    formats: dict[str, str] = {
        "https://objects.githubusercontent.com/": RELEASE_ASSETS_FORMAT,
        "https://gh-proxy.com/": f"https://gh-proxy.com/{RELEASE_ASSETS_FORMAT}",
    }
    return await __get_fastest_formats(
        formats, "https://objects.githubusercontent.com/"
    )


@cached(ttl=CACHED_API_TTL)
async def get_fastest_release_source_formats() -> list[str]:
    """获取最快的发行版源码下载地址格式"""
    formats: dict[str, str] = {
        "https://github.bibk.top": "https://github.bibk.top/{owner}/{repo}/releases/download/{version}/{filename}",
        "https://codeload.github.com/": RELEASE_SOURCE_FORMAT,
        "https://p.102333.xyz/": f"https://p.102333.xyz/{RELEASE_SOURCE_FORMAT}",
    }
    return await __get_fastest_formats(formats, "https://codeload.github.com/")
