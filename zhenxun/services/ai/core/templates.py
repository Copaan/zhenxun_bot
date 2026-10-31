import html
import re
import secrets

_TAG_LIKE = re.compile(r"<(?=\s*/?\s*[^\W\d])")


def wrap_untrusted(
    content: str, label: str = "data", attrs: dict[str, str] | None = None
) -> str:
    """用带随机后缀的标签包裹不可信内容，供拼入提示词

    内容中形似标签的 `<` 替换为全角 `＜`，属性值做转义，
    内容无法伪造闭合标签或插入新的标签。

    参数:
        content: 不可信内容
        label: 标签名
        attrs: 标签属性

    返回:
        str: 包裹后的文本
    """
    tag = f"{label}-{secrets.token_hex(4)}"
    attr_text = "".join(
        f' {key}="{html.escape(str(value))}"' for key, value in (attrs or {}).items()
    )
    body = _TAG_LIKE.sub("＜", content)
    return f"<{tag}{attr_text}>\n{body}\n</{tag}>"
