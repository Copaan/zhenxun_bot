"""Observe published WebUI files without a frontend compiler."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from hashlib import sha256
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import time
from urllib.parse import unquote, urljoin, urlsplit

_EXTENSIONS = {
    ".html",
    ".js",
    ".mjs",
    ".css",
    ".json",
    ".webmanifest",
    ".xml",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".webp",
    ".avif",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".otf",
    ".eot",
    ".wasm",
    ".mp3",
    ".mp4",
    ".webm",
    ".ogg",
    ".wav",
}
# 入口文件，变化时重新扫描全部资源
_ENTRY_FILES = ("index.html", "version.json")
# 资源未就绪时两次扫描的最小间隔秒数
_RESCAN_INTERVAL = 2.0


class _References(HTMLParser):
    def __init__(self):
        super().__init__()
        self.base = "http://webui.invalid/"
        self.references: list[str] = []

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "base" and values.get("href"):
            self.base = urljoin(self.base, values["href"])
        if tag in {"script", "img", "source", "video", "audio", "iframe"}:
            if src := values.get("src"):
                self.references.append(src)
        if tag == "link" and (href := values.get("href")):
            if set((values.get("rel") or "").split()) & {
                "stylesheet",
                "icon",
                "apple-touch-icon",
                "preload",
                "modulepreload",
                "prefetch",
                "manifest",
            }:
                self.references.append(href)


@dataclass(frozen=True)
class ResourceSnapshot:
    revision: str = ""
    ready: bool = False
    html: str = ""
    manifest: dict = field(default_factory=dict)
    digests: dict[str, str] = field(default_factory=dict)
    stats: dict[str, tuple] = field(default_factory=dict)

    def public(self) -> dict:
        return {
            **self.manifest,
            "resource_revision": self.revision,
            "resources_ready": self.ready,
        }


class WebUIResources:
    """Publish a stable content fingerprint and the matching entry document."""

    def __init__(self, root: Path | None = None):
        self.root = (root or Path("data/web_ui/public")).resolve()
        self._started = False
        self.snapshot = ResourceSnapshot()
        self._lock = asyncio.Lock()
        self._scanned_at = 0.0

    def _included(self, path: Path) -> bool:
        relative = path.relative_to(self.root)
        return path.suffix.lower() in _EXTENSIONS and not any(
            part.startswith(".") or part.endswith("~") for part in relative.parts
        )

    @staticmethod
    def _stat(path: Path) -> tuple:
        value = path.stat()
        return (value.st_mtime_ns, value.st_ctime_ns, value.st_size, value.st_ino)

    def _inventory(self) -> dict[str, tuple]:
        return {
            path.relative_to(self.root).as_posix(): self._stat(path)
            for path in self.root.rglob("*")
            if path.is_file()
            and self._included(path)
            and path.resolve().is_relative_to(self.root)
        }

    def scan(self) -> ResourceSnapshot | None:
        """Read a consistent batch and validate the entry's local references."""
        try:
            before = self._inventory()
            if "index.html" not in before:
                return None
            digests = {}
            entry = b""
            manifest = {}
            for relative in sorted(before):
                path = self.root / relative
                digest = sha256()
                with path.open("rb") as stream:
                    if relative in {"index.html", "version.json"}:
                        data = stream.read()
                        digest.update(data)
                        if relative == "index.html":
                            entry = data
                        else:
                            manifest = json.loads(data)
                            if not isinstance(manifest, dict):
                                return None
                    else:
                        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                            digest.update(chunk)
                digests[relative] = digest.hexdigest()
            html = entry.decode("utf-8-sig")
            references = _References()
            references.feed(html)
            for reference in references.references:
                url = urlsplit(urljoin(references.base, reference))
                if url.netloc != "webui.invalid":
                    continue
                path = (self.root / unquote(url.path).lstrip("/")).resolve()
                if not path.is_relative_to(self.root) or not path.is_file():
                    return None
            if before != self._inventory():
                return None
            revision = sha256(json.dumps(digests, sort_keys=True).encode()).hexdigest()
            meta = (
                '<meta name="zhenxun-webui-resource-revision" ' f'content="{revision}">'
            )
            if re.search(r"</head\s*>", html, re.I):
                html = re.sub(
                    r"</head\s*>", meta + "</head>", html, count=1, flags=re.I
                )
            else:
                html = meta + html
            return ResourceSnapshot(revision, True, html, manifest, digests, before)
        except (OSError, ValueError, UnicodeError):
            return None

    async def start(self, root: Path) -> None:
        """扫描 WebUI 资源目录

        参数:
            root: 资源目录
        """
        self.root = root.resolve()
        self._started = True
        self._scanned_at = time.monotonic()
        self.snapshot = await asyncio.to_thread(self.scan) or ResourceSnapshot()

    def _entry_changed(self, snapshot: ResourceSnapshot) -> bool:
        for name in _ENTRY_FILES:
            try:
                current = self._stat(self.root / name)
            except OSError:
                current = None
            if current != snapshot.stats.get(name):
                return True
        return False

    async def refresh(self) -> ResourceSnapshot:
        """入口文件变化或资源未就绪时重新扫描，返回最新快照

        返回:
            ResourceSnapshot: 资源快照
        """
        if not self._started:
            return self.snapshot
        async with self._lock:
            snapshot = self.snapshot
            if snapshot.ready and not self._entry_changed(snapshot):
                return snapshot
            if time.monotonic() - self._scanned_at < _RESCAN_INTERVAL:
                return snapshot
            self._scanned_at = time.monotonic()
            self.snapshot = await asyncio.to_thread(self.scan) or replace(
                snapshot, ready=False
            )
            return self.snapshot

    def etag(self, path: Path) -> str | None:
        snapshot = self.snapshot
        try:
            relative = path.resolve().relative_to(self.root).as_posix()
            if snapshot.ready and snapshot.stats.get(relative) == self._stat(path):
                if digest := snapshot.digests.get(relative):
                    return f'"{digest}"'
        except (OSError, ValueError, TypeError):
            pass
        return None


webui_resources = WebUIResources()
