from collections.abc import Awaitable, Callable
from io import BytesIO
import os
from pathlib import Path
import random
import re

from nonebot.utils import is_coroutine_callable, run_sync
from PIL import Image

from zhenxun.configs.path_config import TEMP_PATH
from zhenxun.services.log import logger
from zhenxun.utils.http_utils import AsyncHttpx

from ._build_image import BuildImage, ColorAlias
from ._build_mat import BuildMat, MatType  # noqa: F401
from ._image_template import ImageTemplate, RowStyle  # noqa: F401


async def text2image(
    text: str,
    auto_parse: bool = True,
    font_size: int = 20,
    color: str | tuple[int, int, int] = (255, 255, 255),
    font: str = "HYWenHei-85W.ttf",
    font_color: str | tuple[int, int, int] = (0, 0, 0),
    padding: int | tuple[int, int, int, int] = 0,
    _add_height: float = 0,
) -> BuildImage:
    """解析文本并转为图片
        使用标签
            <f> </f>
        可选配置项
            font: str -> 特殊文本字体
            fs / font_size: int -> 特殊文本大小
            fc / font_color: Union[str, Tuple[int, int, int]] -> 特殊文本颜色
        示例
            在不在，<f font=YSHaoShenTi-2.ttf font_size=30 font_color=red>HibiKi</f>，
            你最近还好吗，<f font_size=15 font_color=black>我非常想你</f>，
            <f font_size=25>抽卡抽不到金色</f>，这让我很痛苦
    参数:
         text: 文本
         auto_parse: 是否自动解析，否则原样发送
         font_size: 普通字体大小
         color: 背景颜色
         font: 普通字体
         font_color: 普通字体颜色
         padding: 文本外边距，元组类型时为 （上，左，下，右）
         _add_height: 由于get_size无法返回正确的高度，采用手动方式额外添加高度
    """
    if not text:
        raise ValueError("文本转图片 text 不能为空...")
    pw = ph = top_padding = left_padding = 0
    if padding:
        if isinstance(padding, int):
            pw = padding * 2
            ph = padding * 2
            top_padding = left_padding = padding
        elif isinstance(padding, tuple):
            pw = padding[1] + padding[3]
            ph = padding[0] + padding[2]
            top_padding = padding[0]
            left_padding = padding[1]
    _font = BuildImage.load_font(font, font_size)
    if auto_parse and re.search(r"<f(.*)>(.*)</f>", text):
        styled = []
        new_text = ""
        for s in text.split("</f>"):
            r = re.search(r"<f(.*)>(.*)", s)
            if r:
                new_text += s[: r.start()]
                _font_name, _font_size, _font_color = font, font_size, font_color
                for e in r.group(1).split():
                    key, _, value = e.partition("=")
                    value = value.strip("'\"")
                    if key == "font":
                        _font_name = value
                    elif key in ("font_size", "fs") and value.isdigit():
                        _font_size = min(max(int(value), 1), 1000)
                    elif key in ("font_color", "fc"):
                        _font_color = value
                new_text += f"[placeholder_{len(styled)}]"
                styled.append(
                    await BuildImage.build_text_image(
                        r.group(2),
                        font=_font_name,
                        size=_font_size,
                        font_color=_font_color,
                    )
                )
        new_text += text.split("</f>")[-1]
        basic_font_h = BuildImage.get_text_size("正", _font)[1]
        image_list = []
        # 切分换行，每行为单张图片
        for line in new_text.split("\n"):
            pieces: list[BuildImage | str] = []
            for part in re.split(r"(\[placeholder_\d+\])", line):
                if r := re.fullmatch(r"\[placeholder_(\d+)\]", part):
                    pieces.append(styled[int(r.group(1))])
                elif part:
                    pieces.append(part)
            widths = [
                p.width if isinstance(p, BuildImage) else int(_font.getlength(p))
                for p in pieces
            ]
            line_h = max(
                [basic_font_h] + [p.height for p in pieces if isinstance(p, BuildImage)]
            )
            A = BuildImage(
                max(sum(widths), 1), line_h, color=color, font=font, font_size=font_size
            )
            current_width = 0
            for piece, piece_width in zip(pieces, widths):
                if isinstance(piece, BuildImage):
                    y = line_h - piece.height
                    if new_text == "[placeholder_0]":
                        y = (line_h - piece.height) // 2
                    await A.paste(piece, (current_width, y))
                else:
                    y = line_h - basic_font_h
                    await A.text((current_width, y), piece, font_color)
                current_width += piece_width
            image_list.append(A)
        width = max(img.width for img in image_list) + pw
        height = sum(img.height for img in image_list) + ph
        A = BuildImage(width, height, color=color)
        current_height = top_padding
        for img in image_list:
            await A.paste(img, (left_padding, current_height))
            current_height += img.height
    else:
        width = 0
        height = 0
        _, h = BuildImage.get_text_size("正", _font)
        line_height = int(font_size / 3)
        image_list = []
        for s in text.split("\n"):
            w, _ = BuildImage.get_text_size(s.strip() or "正", _font)
            height += h + line_height
            width = width if width > w else w
            image_list.append(
                await BuildImage.build_text_image(
                    s.strip(), font, font_size, font_color
                )
            )
        height = sum(img.height + 8 for img in image_list) + pw
        width += pw
        # height += ph
        A = BuildImage(
            width + left_padding,
            height + top_padding + 2,
            color=color,
        )
        cur_h = ph
        for img in image_list:
            await A.paste(img, (pw, cur_h))
            cur_h += img.height + line_height
    return A


def group_image(image_list: list[BuildImage]) -> tuple[list[list[BuildImage]], int]:
    """
    说明:
        根据图片大小进行分组
    参数:
         image_list: 排序图片列表
    """
    image_list.sort(key=lambda x: x.height, reverse=True)
    max_image = max(image_list, key=lambda x: x.height)

    image_list.remove(max_image)
    max_h = max_image.height
    total_w = 0

    # 图片分组
    image_group = [[max_image]]
    is_use = []
    surplus_list = image_list[:]

    for image in image_list:
        if image.uid not in is_use:
            group = [image]
            is_use.append(image.uid)
            curr_h = image.height
            while True:
                surplus_list = [x for x in surplus_list if x.uid not in is_use]
                for tmp in surplus_list:
                    temp_h = curr_h + tmp.height + 10
                    if temp_h < max_h or abs(max_h - temp_h) < 100:
                        curr_h += tmp.height + 15
                        is_use.append(tmp.uid)
                        group.append(tmp)
                        break
                else:
                    break
            total_w += max([x.width for x in group]) + 15
            image_group.append(group)
    while surplus_list:
        surplus_list = [x for x in surplus_list if x.uid not in is_use]
        if not surplus_list:
            break
        surplus_list.sort(key=lambda x: x.height, reverse=True)
        for img in surplus_list:
            if img.uid not in is_use:
                _w = 0
                index = -1
                for i, ig in enumerate(image_group):
                    if s := sum([x.height for x in ig]) > _w:
                        _w = s
                        index = i
                if index != -1:
                    image_group[index].append(img)
                    is_use.append(img.uid)

    max_h = 0
    max_w = 0
    for ig in image_group:
        if (_h := sum([x.height + 15 for x in ig])) > max_h:
            max_h = _h
        max_w += max([x.width for x in ig]) + 30
    is_use.clear()
    while abs(max_h - max_w) > 200 and len(image_group) - 1 >= len(image_group[-1]):
        for img in image_group[-1]:
            _min_h = 999999
            _min_index = -1
            for i, ig in enumerate(image_group):
                if (_h := sum([x.height for x in ig]) + img.height) < _min_h:
                    _min_h = _h
                    _min_index = i
            is_use.append(_min_index)
            image_group[_min_index].append(img)
        max_w -= max([x.width for x in image_group[-1]]) - 30
        image_group.pop(-1)
        max_h = max([sum([x.height + 15 for x in ig]) for ig in image_group])
    return image_group, max(max_h + 250, max_w + 70)


async def build_sort_image(
    image_group: list[list[BuildImage]],
    h: int | None = None,
    padding_top: int = 200,
    color: ColorAlias = (
        255,
        255,
        255,
    ),
    background_path: Path | None = None,
    background_handle: Callable[[BuildImage], Awaitable] | None = None,
) -> BuildImage:
    """
    说明:
        对group_image的图片进行组装
    参数:
         image_group: 分组图片列表
         h: max(宽，高)，一般为group_image的返回值，有值时，图片必定为正方形
         padding_top: 图像列表与最顶层间距
         color: 背景颜色
         background_path: 背景图片文件夹路径（随机）
         background_handle: 背景图额外操作
    """
    bk_file = None
    if background_path:
        random_bk = os.listdir(background_path)
        if random_bk:
            bk_file = random.choice(random_bk)
    image_w = 0
    image_h = 0
    if not h:
        for ig in image_group:
            _w = max([x.width + 30 for x in ig])
            image_w += _w + 30
            _h = sum([x.height + 10 for x in ig])
            if _h > image_h:
                image_h = _h
        image_h += padding_top
    else:
        image_w = h
        image_h = h
    A = BuildImage(
        image_w,
        image_h,
        font_size=24,
        font="CJGaoDeGuo.otf",
        color=color,
        background=(background_path / bk_file) if background_path and bk_file else None,
    )
    if background_handle:
        if is_coroutine_callable(background_handle):
            await background_handle(A)
        else:
            background_handle(A)
    curr_w = 50
    for ig in image_group:
        curr_h = padding_top - 20
        for img in ig:
            await A.paste(img, (curr_w, curr_h))
            curr_h += img.height + 10
        curr_w += max([x.width for x in ig]) + 30
    return A


def average_hash(image: Image.Image) -> str:
    """计算图片的平均哈希

    灰度化并缩放到 8x8 后，亮度高于平均值的像素记为 1，按行拼接为 64 位。

    参数:
        image: 图片

    返回:
        str: 16 位十六进制哈希值
    """
    gray = image.convert("L").resize((8, 8), Image.Resampling.LANCZOS)
    pixels = list(gray.tobytes())
    mean = sum(pixels) / len(pixels)
    bits = "".join("1" if pixel > mean else "0" for pixel in pixels)
    return f"{int(bits, 2):016x}"


def get_img_hash(image_file: str | Path) -> str:
    """获取图片的hash值

    参数:
        image_file: 图片文件路径

    返回:
        str: 哈希值，读取失败时为空字符串
    """
    try:
        with Image.open(image_file) as image:
            return average_hash(image)
    except Exception as e:
        logger.warning("获取图片Hash出错", "禁言检测", e=e)
    return ""


async def get_download_image_hash(url: str, mark: str, use_proxy: bool = False) -> str:
    """下载图片获取哈希值

    参数:
        url: 图片url
        mark: 随机标志符

    返回:
        str: 哈希值
    """
    try:
        if await AsyncHttpx.download_file(
            url, TEMP_PATH / f"compare_download_{mark}_img.jpg", use_proxy=use_proxy
        ):
            img_hash = await run_sync(get_img_hash)(
                TEMP_PATH / f"compare_download_{mark}_img.jpg"
            )
            return str(img_hash)
    except Exception as e:
        logger.warning("下载读取图片Hash出错", e=e)
    return ""


def pic2bytes(image) -> bytes:
    """获取bytes

    返回:
        bytes: bytes
    """
    with BytesIO() as buf:
        image.save(buf, format="PNG")
        return buf.getvalue()
