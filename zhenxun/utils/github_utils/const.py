import base64
import os
import re

GITHUB_REPO_URL_PATTERN = re.compile(
    r"^https://github.com/(?P<owner>[^/]+)/(?P<repo>[^/]+)(/tree/(?P<branch>[^/]+))?$"
)
"""github仓库地址正则"""

JSD_PACKAGE_API_FORMAT = (
    "https://data.jsdelivr.com/v1/packages/gh/{owner}/{repo}@{branch}"
)
"""jsdelivr包地址格式"""

GIT_API_TREES_FORMAT = (
    "https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
)
"""git api trees地址格式"""

CACHED_API_TTL = 300
"""缓存api ttl"""

RAW_CONTENT_FORMAT = "https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
"""raw content格式"""

GITEE_RAW_CONTENT_FORMAT = "https://gitee.com/{owner}/{repo}/raw/main/{path}"
"""gitee raw content格式"""

ARCHIVE_URL_FORMAT = "https://github.com/{owner}/{repo}/archive/refs/heads/{branch}.zip"
"""archive url格式"""

RELEASE_ASSETS_FORMAT = (
    "https://github.com/{owner}/{repo}/releases/download/{version}/{filename}"
)
"""release assets格式"""

RELEASE_SOURCE_FORMAT = (
    "https://codeload.github.com/{owner}/{repo}/legacy.{compress}/refs/tags/{version}"
)
"""release 源码格式"""

GIT_API_COMMIT_FORMAT = "https://api.github.com/repos/{owner}/{repo}/commits/{branch}"
"""git api commit地址格式"""

GIT_API_PROXY_COMMIT_FORMAT = (
    "https://git-api.zhenxun.org/repos/{owner}/{repo}/commits/{branch}"
)
"""git api commit地址格式 (代理)"""

ALIYUN_ORG_ID = "69624f9a58acb3fac7aed520"
"""阿里云 organization id"""

ALIYUN_ENDPOINT = "openapi-rdc.aliyuncs.com"
"""云效 Codeup OpenAPI endpoint"""

ALIYUN_GROUP = "ATTomatoo"
"""核心仓库所在分组"""

ALIYUN_REPO_MAPPING = {
    "zhenxun-bot-resources": "7665131",
    "zhenxun_bot_plugins_index": "7238731",
    "zhenxun_bot_plugins": "7238732",
    "zhenxun_docs": "6626235",
    "zhenxun_bot": "6188183",
    "zhenxun_bot_webui": "7397717",
}
"""阿里云仓库 ID 映射"""

ALIYUN_EXTERNAL_PLUGIN_GROUPS: list[str] = ["zhenxun_plugins"]
"""阿里云外部插件分组列表"""

RDC_access_token_encrypted = (
    "cHQtbGlJbjd2YzE5WUJNTURPdGFXRVNNYVNoX2ZjNjY2NDBlLTkxODktNGIxNC04"
    "Y2E1LWJkMzFiZDg1NDczMg=="
)
"""云效只读访问令牌（base64），未配置环境变量或 NoneBot 配置时使用"""


def get_aliyun_token(encoded_token: str = "") -> str:
    """读取云效访问令牌，依次使用环境变量、NoneBot 配置和内置默认令牌

    参数:
        encoded_token: 未配置令牌时使用的 base64 编码令牌

    返回:
        str: 云效访问令牌
    """
    token = os.getenv("ALIYUN_CODEUP_TOKEN", "").strip()
    encoded = os.getenv("ALIYUN_CODEUP_TOKEN_BASE64", "").strip()
    if not token and not encoded:
        from nonebot import get_driver

        try:
            config = get_driver().config
        except ValueError:
            config = None
        token = str(getattr(config, "aliyun_codeup_token", "") or "").strip()
        encoded = str(getattr(config, "aliyun_codeup_token_base64", "") or "").strip()
    encoded = encoded or encoded_token
    if not token and encoded:
        try:
            token = base64.b64decode(encoded, validate=True).decode("ascii").strip()
        except (ValueError, UnicodeError):
            raise ValueError("ALIYUN_CODEUP_TOKEN_BASE64 格式无效") from None
    if not token:
        raise ValueError("未配置 ALIYUN_CODEUP_TOKEN 或 ALIYUN_CODEUP_TOKEN_BASE64")
    if not token.isascii() or any(char.isspace() for char in token):
        raise ValueError("云效访问令牌格式无效")
    return token
