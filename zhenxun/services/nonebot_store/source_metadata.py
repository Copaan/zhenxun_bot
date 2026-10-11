"""Read static source metadata without importing a package or its build backend."""

from email.parser import BytesParser
from hashlib import sha256
from html.parser import HTMLParser
import io
from pathlib import Path
import re
import tarfile
from urllib.parse import unquote, urljoin, urlsplit
import zipfile

from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name, parse_sdist_filename
from packaging.version import Version

_ARCHIVE_LIMIT = 64 * 1024 * 1024
_METADATA_LIMIT = 2 * 1024 * 1024


class _Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "a" and values.get("href") and "data-yanked" not in values:
            self.links.append(values["href"])


def _metadata(data: bytes, filename: str, expected: str) -> tuple[bytes, str]:
    from .dependencies import DependencyAnalysisError

    if filename.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            entries = [
                item
                for item in archive.infolist()
                if len(Path(item.filename).parts) == 2
                and item.filename.endswith("/PKG-INFO")
            ]
            if len(entries) != 1 or entries[0].file_size > _METADATA_LIMIT:
                raise ValueError("static_metadata_missing")
            raw = archive.read(entries[0])
    else:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as archive:
            entries = [
                item
                for item in archive
                if item.isfile()
                and len(Path(item.name).parts) == 2
                and item.name.endswith("/PKG-INFO")
            ]
            if len(entries) != 1 or entries[0].size > _METADATA_LIMIT:
                raise ValueError("static_metadata_missing")
            raw = archive.extractfile(entries[0]).read(_METADATA_LIMIT + 1)
    metadata = BytesParser().parsebytes(raw)
    version = str(metadata.get("Version") or "")
    if canonicalize_name(metadata.get("Name", "")) != expected or parse_sdist_filename(
        filename
    ) != (expected, Version(version)):
        raise ValueError("static_metadata_identity_changed")
    dynamic = {item.casefold() for item in metadata.get_all("Dynamic", [])}
    if Version(metadata.get("Metadata-Version", "0")) < Version("2.2") or dynamic & {
        "requires-dist",
        "requires-python",
        "provides-extra",
    }:
        raise DependencyAnalysisError(
            "dependency_source_metadata_unavailable",
            "Source dependencies require execution of its build backend; "
            "provide a wheel or install this package in the console.",
            details={"package": expected},
        )
    return raw, version


async def project_static_sources(
    name: str, directory: Path, environment: dict, specifiers: list[str]
) -> bool:
    """Expose metadata-only wheels to the preview resolver, never to an installer."""
    from zhenxun.services.network_proxy import ManagedAsyncClient

    from .dependencies import DependencyAnalysisError

    sources = []
    local = environment.get("UV_FIND_LINKS")
    if local and Path(local).is_dir():
        sources.extend(
            path.as_uri() for path in Path(local).iterdir() if path.is_file()
        )
    async with ManagedAsyncClient(timeout=30, follow_redirects=True) as client:
        if not environment.get("UV_NO_INDEX"):
            indexes = [
                *environment.get("UV_INDEX", "").split(),
                environment.get("UV_DEFAULT_INDEX", "https://pypi.org/simple"),
            ]
            for index in indexes:
                match = re.match(r"^([A-Za-z0-9_-]+)=(https?://.*)$", index)
                auth = None
                if match:
                    label, index = match.groups()
                    key = re.sub(r"[^A-Za-z0-9]", "_", label).upper()
                    username = environment.get(f"UV_INDEX_{key}_USERNAME")
                    if username is not None:
                        auth = (
                            username,
                            environment.get(f"UV_INDEX_{key}_PASSWORD", ""),
                        )
                url = index.rstrip("/") + "/" + name + "/"
                response = await client.get(url, auth=auth)
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                links = _Links()
                links.feed(response.text)
                sources.extend(urljoin(str(response.url), link) for link in links.links)
                break
        added = False
        for source in sources:
            url = urlsplit(source)
            filename = unquote(url.path.rsplit("/", 1)[-1])
            try:
                project, version = parse_sdist_filename(filename)
            except ValueError:
                continue
            if project != name or any(
                not SpecifierSet(value).contains(version, prereleases=True)
                for value in specifiers
            ):
                continue
            if url.scheme == "file":
                from urllib.request import url2pathname

                path = Path(url2pathname(url.path))
                if path.stat().st_size > _ARCHIVE_LIMIT:
                    raise ValueError("source_archive_too_large")
                data = path.read_bytes()
            else:
                chunks = []
                size = 0
                async with client.stream("GET", source) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > _ARCHIVE_LIMIT:
                            raise ValueError("source_archive_too_large")
                        chunks.append(chunk)
                data = b"".join(chunks)
            if (
                url.fragment.startswith("sha256=")
                and sha256(data).hexdigest() != url.fragment[7:]
            ):
                raise DependencyAnalysisError(
                    "dependency_source_hash_mismatch", details={"package": name}
                )
            raw, version = _metadata(data, filename, name)
            normalized = name.replace("-", "_")
            info = f"{normalized}-{version}.dist-info"
            directory.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(
                directory / f"{normalized}-{version}-py3-none-any.whl", "w"
            ) as archive:
                archive.writestr(info + "/METADATA", raw)
                archive.writestr(
                    info + "/WHEEL",
                    "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
                )
                archive.writestr(info + "/RECORD", "")
            added = True
        return added
