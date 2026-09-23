"""Observe published WebUI files without a frontend compiler."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from hashlib import sha256
from html.parser import HTMLParser
import json
import logging
from pathlib import Path
import re
import time
from urllib.parse import unquote, urljoin, urlsplit

from watchfiles import Change, awatch

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
_LOGGER = logging.getLogger(__name__)


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
            if values.get("src"):
                self.references.append(values["src"])
        if tag == "link" and values.get("href"):
            if set((values.get("rel") or "").split()) & {
                "stylesheet",
                "icon",
                "apple-touch-icon",
                "preload",
                "modulepreload",
                "prefetch",
                "manifest",
            }:
                self.references.append(values["href"])


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
        self.root = root.resolve() if root else None
        self.snapshot = ResourceSnapshot()
        self.stop_event = asyncio.Event()

    def contains(self, path: Path) -> bool:
        root = self.root or Path("data/web_ui/public").resolve()
        return path.resolve().is_relative_to(root)

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

    async def start(self, context, root: Path) -> None:
        self.root = root.resolve()
        self.stop_event = asyncio.Event()
        self.snapshot = await asyncio.to_thread(self.scan) or ResourceSnapshot()
        context.spawn_task(
            self.watch(), name="webui-published-resources", cancel=self.stop_event.set
        )

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

    async def watch(self) -> None:
        pending = True
        changed_at = time.monotonic()
        while not self.stop_event.is_set():
            # The parent survives replacement of the complete public directory.
            try:
                async for changes in awatch(
                    self.root.parent,
                    watch_filter=None,
                    debounce=100,
                    step=50,
                    rust_timeout=500,
                    yield_on_timeout=True,
                    stop_event=self.stop_event,
                ):
                    relevant = any(
                        self.contains(path := Path(raw_path))
                        and (
                            self._included(path)
                            or (change != Change.modified and path.suffix == "")
                        )
                        for change, raw_path in changes
                    )
                    if relevant:
                        pending = True
                        changed_at = time.monotonic()
                        self.snapshot = replace(self.snapshot, ready=False)
                    if pending and time.monotonic() - changed_at >= 2:
                        snapshot = await asyncio.to_thread(self.scan)
                        if snapshot is not None:
                            self.snapshot = snapshot
                            pending = False
                        else:
                            self.snapshot = replace(self.snapshot, ready=False)
                            changed_at = time.monotonic()
            except asyncio.CancelledError:
                raise
            except OSError as error:
                self.snapshot = replace(self.snapshot, ready=False)
                _LOGGER.warning("WebUI resource watch unavailable: %s", error)
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=5)
                except asyncio.TimeoutError:
                    pending = True
                    changed_at = time.monotonic()


webui_resources = WebUIResources()
