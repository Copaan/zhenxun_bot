from typing import Literal

RepoKey = Literal["bot", "resource", "plugin", "webui"]

OFFICIAL_GITHUB_REPOS: dict[RepoKey, str] = {
    "bot": "zhenxun-org/zhenxun_bot",
    "resource": "zhenxun-org/zhenxun-bot-resources",
    "plugin": "zhenxun-org/zhenxun_bot_plugins",
    "webui": "HibiKier/zhenxun_bot_webui",
}
"""官方 GitHub 仓库，阿里云镜像同步自这些仓库"""

GITHUB_REPOS: dict[RepoKey, str] = {
    "bot": "Copaan/zhenxun_bot",
    "resource": "Copaan/zhenxun-bot-resources",
    "plugin": "Copaan/zhenxun_bot_plugins",
    "webui": "Copaan/zhenxun_bot_webui",
}
"""更新与插件商店使用的 GitHub 仓库（owner/repo）"""


def github_url(key: RepoKey, branch: str | None = None) -> str:
    """获取仓库的 GitHub 地址

    参数:
        key: 仓库标识
        branch: 分支，传入时返回 tree 地址

    返回:
        str: 仓库地址
    """
    url = f"https://github.com/{GITHUB_REPOS[key]}"
    return f"{url}/tree/{branch}" if branch else f"{url}.git"


def aliyun_mirrored(key: RepoKey) -> bool:
    """判断仓库是否可使用阿里云镜像，仅官方仓库同步到阿里云

    参数:
        key: 仓库标识

    返回:
        bool: 是否可使用阿里云镜像
    """
    return GITHUB_REPOS[key] == OFFICIAL_GITHUB_REPOS[key]
