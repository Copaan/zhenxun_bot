"""文档解析：知识库与聊天中的文件统一转为 Markdown 文本，可选提取文档中的图片"""

import csv
from dataclasses import dataclass, field
from html.parser import HTMLParser
import importlib.util
import io
import json
import re
import threading
from typing import Any, ClassVar
import zipfile

from PIL import Image

TEXT_TYPES = {".txt", ".md", ".markdown", ".html", ".htm", ".csv", ".json", ".epub"}
"""只依赖标准库即可解析的类型"""
DOC_TYPES = {".docx", ".xlsx", ".pptx", ".pdf"}
"""需要安装文档解析组件的类型"""
IMAGE_TYPES = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"}
DOC_MODULES = ("docx", "openpyxl", "pptx", "pymupdf")
"""解析 DOC_TYPES 需要的模块（WebUI 的“知识库文档解析”组件）"""
OCR_MODULES = ("rapidocr", "onnxruntime")
"""OCR 需要的模块（WebUI 的“知识库 OCR”组件）"""
"""图片类型"""
MIN_IMAGE_SIDE = 256
"""提取文档图片时跳过短边小于该值的图"""
MAX_IMAGES = 20
"""每个文档最多提取的图片数"""
OCR_DPI = 200
"""扫描页渲染分辨率"""

_ocr_engine: Any = None
_ocr_lock = threading.Lock()


@dataclass
class Parsed:
    text: str
    """Markdown 文本"""
    images: list[bytes] = field(default_factory=list)
    """从文档中提取的图片"""


def _md_table(rows: list[list[str]]) -> str:
    """二维表格转 Markdown 表"""
    rows = [
        [(c or "").replace("|", "\\|").replace("\n", " ").strip() for c in r]
        for r in rows
    ]
    if not rows:
        return ""
    width = max(len(r) for r in rows)
    rows = [r + [""] * (width - len(r)) for r in rows]
    lines = ["| " + " | ".join(rows[0]) + " |", "|" + " --- |" * width]
    lines += ["| " + " | ".join(r) + " |" for r in rows[1:]]
    return "\n".join(lines)


class _Html2Md(HTMLParser):
    """HTML 转 Markdown：保留标题、段落与表格"""

    BLOCK: ClassVar[set[str]] = {
        "p",
        "div",
        "br",
        "li",
        "h4",
        "h5",
        "h6",
        "tr",
        "section",
        "article",
    }

    def __init__(self):
        super().__init__()
        self.out: list[str] = []
        self.skip = 0
        self.table: list[list[str]] | None = None
        self.row: list[str] | None = None
        self.cell: list[str] | None = None

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style", "head"}:
            self.skip += 1
        elif tag == "table":
            self.table = []
        elif tag == "tr" and self.table is not None:
            self.row = []
        elif tag in {"td", "th"} and self.row is not None:
            self.cell = []
        elif tag in {"h1", "h2", "h3"}:
            self.out.append("\n\n" + "#" * int(tag[1]) + " ")
        elif tag in self.BLOCK:
            self.out.append("\n\n")

    def handle_endtag(self, tag):
        if tag in {"script", "style", "head"}:
            self.skip -= 1
        elif tag in {"td", "th"} and self.cell is not None and self.row is not None:
            self.row.append("".join(self.cell).strip())
            self.cell = None
        elif tag == "tr" and self.row is not None and self.table is not None:
            self.table.append(self.row)
            self.row = None
        elif tag == "table" and self.table is not None:
            self.out.append("\n\n" + _md_table(self.table) + "\n\n")
            self.table = None

    def handle_data(self, data):
        if self.skip:
            return
        if self.cell is not None:
            self.cell.append(data)
        else:
            self.out.append(data)


def html_to_md(html: str) -> str:
    """HTML 转 Markdown

    参数:
        html: HTML 文本

    返回:
        str: Markdown 文本
    """
    parser = _Html2Md()
    parser.feed(html)
    return re.sub(r"\n{3,}", "\n\n", "".join(parser.out)).strip()


def decode_text(data: bytes) -> str:
    """按常见中文编码依次尝试解码

    参数:
        data: 文件内容

    返回:
        str: 文本
    """
    for encoding in ("utf-8-sig", "gb18030", "big5", "utf-16"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _keep_image(data: bytes) -> bool:
    """图片是否足够大，值得提取"""
    try:
        with Image.open(io.BytesIO(data)) as image:
            return min(image.size) >= MIN_IMAGE_SIDE
    except Exception:
        return False


def _zip_images(data: bytes, prefix: str) -> list[bytes]:
    """从 docx / pptx 压缩包的媒体目录提取图片"""
    images = []
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for name in sorted(z.namelist()):
            if name.startswith(prefix) and name.lower().endswith(
                (".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp")
            ):
                if _keep_image(blob := z.read(name)):
                    images.append(blob)
                if len(images) >= MAX_IMAGES:
                    break
    return images


def _parse_docx(data: bytes) -> str:
    import docx
    from docx.table import Table
    from docx.text.paragraph import Paragraph

    document = docx.Document(io.BytesIO(data))
    out = []
    for element in document.element.body.iterchildren():
        tag = element.tag.rsplit("}", 1)[-1]
        if tag == "p":
            paragraph = Paragraph(element, document)
            text = paragraph.text.strip()
            if not text:
                continue
            style = paragraph.style.name if paragraph.style else ""
            level = style.split()[-1] if style.startswith("Heading") else ""
            out.append(("#" * int(level) + " " if level.isdigit() else "") + text)
        elif tag == "tbl":
            table = Table(element, document)
            out.append(_md_table([[c.text for c in r.cells] for r in table.rows]))
    return "\n\n".join(out)


def _parse_xlsx(data: bytes) -> str:
    import openpyxl

    workbook = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    out = []
    for sheet in workbook.worksheets:
        rows = [
            ["" if v is None else str(v) for v in row]
            for row in sheet.iter_rows(values_only=True)
            if any(v is not None for v in row)
        ]
        if rows:
            out.append(f"## {sheet.title}\n\n" + _md_table(rows))
    return "\n\n".join(out)


def _parse_pptx(data: bytes) -> str:
    from pptx import Presentation

    out = []
    for index, slide in enumerate(Presentation(io.BytesIO(data)).slides, 1):
        out.append(f"## 第 {index} 页")
        for shape in slide.shapes:
            if getattr(shape, "has_table", False):
                rows = shape.table.rows  # pyright: ignore [reportAttributeAccessIssue]
                out.append(_md_table([[c.text for c in r.cells] for r in rows]))
            elif getattr(shape, "has_text_frame", False):
                text = shape.text_frame.text.strip()  # pyright: ignore [reportAttributeAccessIssue]
                if text:
                    out.append(text)
    return "\n\n".join(out)


def _pdf_page(page: Any, ocr: bool) -> str:
    """PDF 单页：表格转 Markdown 表，其余文字按位置排序；无文字层时可 OCR"""
    try:
        tables = list(page.find_tables().tables)
    except Exception:
        tables = []
    boxes = [t.bbox for t in tables]
    parts: list[tuple[float, str]] = []
    for block in page.get_text("blocks"):
        x0, y0, x1, y1, text = block[:5]
        if not str(text).strip():
            continue
        inside = any(
            x0 >= b[0] - 2 and y0 >= b[1] - 2 and x1 <= b[2] + 2 and y1 <= b[3] + 2
            for b in boxes
        )
        if not inside:
            parts.append((y0, str(text).strip()))
    for table in tables:
        if markdown := table.to_markdown().strip():
            parts.append((table.bbox[1], markdown))
    if parts:
        return "\n\n".join(text for _, text in sorted(parts, key=lambda p: p[0]))
    if ocr:
        return ocr_image(page.get_pixmap(dpi=OCR_DPI).tobytes("png"))
    return ""


def _parse_pdf(data: bytes, extract_images: bool) -> Parsed:
    import pymupdf

    ocr = has_modules(OCR_MODULES)
    with pymupdf.open(stream=data, filetype="pdf") as document:
        pages = [_pdf_page(page, ocr) for page in document]
        images = []
        if extract_images:
            seen = set()
            for page in document:
                for item in page.get_images(full=True):
                    if item[0] in seen or len(images) >= MAX_IMAGES:
                        continue
                    seen.add(item[0])
                    blob = document.extract_image(item[0]).get("image")
                    if blob and _keep_image(blob):
                        images.append(blob)
    return Parsed("\n\n".join(p for p in pages if p), images)


def _parse_epub(data: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        container = z.read("META-INF/container.xml").decode("utf-8", "replace")
        match = re.search(r'full-path="([^"]+)"', container)
        if not match:
            raise ValueError("epub 缺少目录文件。")
        opf_path = match.group(1)
        opf = z.read(opf_path).decode("utf-8", "replace")
        base = opf_path.rsplit("/", 1)[0] + "/" if "/" in opf_path else ""
        items: dict[str, str] = {}
        for tag in re.findall(r"<item\b[^>]*>", opf):
            ident = re.search(r'\bid="([^"]+)"', tag)
            href = re.search(r'\bhref="([^"]+)"', tag)
            if ident and href:
                items[ident.group(1)] = href.group(1)
        out = []
        for ref in re.findall(r'<itemref[^>]*idref="([^"]+)"', opf):
            href = items.get(ref)
            if href and not href.startswith("nav"):
                out.append(html_to_md(z.read(base + href).decode("utf-8", "replace")))
        return "\n\n".join(out)


def has_modules(modules: tuple[str, ...]) -> bool:
    """模块是否都可以导入

    参数:
        modules: 模块名

    返回:
        bool: 是否都已安装
    """
    return all(importlib.util.find_spec(m) is not None for m in modules)


def parse(data: bytes, ext: str, extract_images: bool = False) -> Parsed:
    """按扩展名把文档解析为 Markdown 文本

    参数:
        data: 文件内容
        ext: 小写扩展名，带点
        extract_images: 是否提取 pdf / docx / pptx 中的图片

    返回:
        Parsed: 文本与提取的图片

    异常:
        ValueError: 不支持的类型，或需要的组件未安装
    """
    if ext in DOC_TYPES:
        if not has_modules(DOC_MODULES):
            raise ValueError("解析该类型需要先在 WebUI 安装“知识库文档解析”组件。")
        if ext == ".pdf":
            return _parse_pdf(data, extract_images)
        if ext == ".xlsx":
            return Parsed(_parse_xlsx(data))
        if ext == ".docx":
            images = _zip_images(data, "word/media/") if extract_images else []
            return Parsed(_parse_docx(data), images)
        images = _zip_images(data, "ppt/media/") if extract_images else []
        return Parsed(_parse_pptx(data), images)
    if ext not in TEXT_TYPES:
        raise ValueError(f"不支持的文件类型：{ext or '无扩展名'}")
    if ext == ".epub":
        return Parsed(_parse_epub(data))
    text = decode_text(data)
    if ext in {".html", ".htm"}:
        return Parsed(html_to_md(text))
    if ext == ".csv":
        return Parsed(_md_table(list(csv.reader(io.StringIO(text)))))
    if ext == ".json":
        try:
            return Parsed(json.dumps(json.loads(text), ensure_ascii=False, indent=2))
        except ValueError:
            return Parsed(text)
    return Parsed(text)


def ocr_image(data: bytes) -> str:
    """识别图片中的文字，未安装 OCR 组件时返回空

    参数:
        data: 图片内容

    返回:
        str: 识别出的文字，每行一段
    """
    global _ocr_engine
    if not has_modules(OCR_MODULES):
        return ""
    with _ocr_lock:
        if _ocr_engine is None:
            from rapidocr import RapidOCR

            _ocr_engine = RapidOCR()
        result = _ocr_engine(data)
    return "\n".join(getattr(result, "txts", None) or [])
