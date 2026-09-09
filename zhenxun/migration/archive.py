from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import struct
import tempfile
import time
import uuid
import zipfile

from .access import private_directory
from .discovery import CATEGORIES, FileEntry
from .errors import MigrationError
from .paths import PathIndex, contained_path, logical_path

SCHEMA = 1
CHUNK = 1024 * 1024
MANIFEST_LIMIT = 128 * CHUNK


@dataclass(frozen=True)
class Limits:
    compressed: int = 20 * 1024**3
    expanded: int = 100 * 1024**3
    entries: int = 200_000

    def __post_init__(self) -> None:
        if any(type(value) is not int or value <= 0 for value in asdict(self).values()):
            raise MigrationError("migration_limits_invalid")


def _checkpoint() -> None:
    pass


def _unique_object(pairs: list[tuple]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise MigrationError("migration_manifest_duplicate_key")
        result[key] = value
    return result


def _manifest_budget(raw: bytes, limits: Limits) -> None:
    # Bound nesting and allocation count before json.loads creates Python objects.
    depth = tokens = 0
    quoted = escaped = False
    for value in raw:
        if quoted:
            if escaped:
                escaped = False
            elif value == 92:
                escaped = True
            elif value == 34:
                quoted = False
            continue
        if value == 34:
            quoted = True
        elif value in (123, 91):
            depth += 1
            tokens += 1
        elif value in (125, 93):
            depth -= 1
        elif value in (44, 58):
            tokens += 1
        if depth > 32 or tokens > limits.entries * 16 + 100_000:
            raise MigrationError("migration_manifest_structure_limit")


def _zip_class(password: bytes | None):
    if not password:
        return zipfile.ZipFile
    try:
        import pyzipper
    except ImportError:
        raise MigrationError("migration_aes_dependency_missing") from None
    return pyzipper.AESZipFile


def encryption_available() -> bool:
    try:
        _zip_class(b"capability")
    except MigrationError:
        return False
    return True


def file_hash(path: Path, checkpoint: Callable[[], None] = _checkpoint) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while data := stream.read(CHUNK):
            checkpoint()
            digest.update(data)
    return digest.hexdigest()


def require_space(path: Path, size: int) -> None:
    while not path.exists():
        path = path.parent
    if shutil.disk_usage(path).free < size + 16 * CHUNK:
        raise MigrationError("migration_disk_space_insufficient")


def _central_directory_limit(path: Path, limits: Limits) -> None:
    # Check the central-directory budget before ZipFile allocates one object/member.
    size = path.stat().st_size
    if size > limits.compressed:
        raise MigrationError("migration_compressed_limit", status=413)
    with path.open("rb") as stream:
        stream.seek(max(0, size - 65557))
        tail = stream.read(65557)
        offset = tail.rfind(b"PK\x05\x06")
        if offset < 0 or offset + 22 > len(tail):
            raise MigrationError("migration_zip_invalid")
        _, disk, start_disk, count_disk, count, length, _, comment = struct.unpack(
            "<4s4H2LH", tail[offset : offset + 22]
        )
        if (
            disk
            or start_disk
            or count_disk != count
            or offset + 22 + comment != len(tail)
        ):
            raise MigrationError("migration_multivolume_or_trailing_data")
        absolute = max(0, size - 65557) + offset
        locator = b""
        if absolute >= 20:
            stream.seek(absolute - 20)
            locator = stream.read(20)
        if locator.startswith(b"PK\x06\x07"):
            signature, disk, record_offset, disks = struct.unpack("<4sLQL", locator)
            if signature != b"PK\x06\x07" or disk or disks != 1:
                raise MigrationError("migration_zip64_invalid")
            if record_offset + 56 > absolute - 20:
                raise MigrationError("migration_zip64_invalid")
            stream.seek(record_offset)
            record = stream.read(56)
            if record[:4] != b"PK\x06\x06":
                raise MigrationError("migration_zip64_invalid")
            disk, start_disk, count_disk, count, length, _ = struct.unpack(
                "<2L4Q", record[16:56]
            )
            if disk or start_disk or count_disk != count:
                raise MigrationError("migration_zip64_invalid")
        elif length == 0xFFFFFFFF:
            raise MigrationError("migration_zip64_invalid")
        if count > limits.entries + 1 or length > (limits.entries + 1) * 4096:
            raise MigrationError("migration_entry_limit", status=413)


def _validate_manifest(value: object, infos: dict, limits: Limits) -> dict:
    if not isinstance(value, dict) or value.get("format") != "zhenxun-instance":
        raise MigrationError("migration_manifest_invalid")
    if type(value.get("schema")) is not int or value["schema"] != SCHEMA:
        raise MigrationError("migration_schema_unsupported")
    if not isinstance(value.get("source"), dict):
        raise MigrationError("migration_manifest_invalid")
    try:
        uuid.UUID(value["package_id"])
    except (ValueError, TypeError, KeyError, AttributeError):
        raise MigrationError("migration_manifest_invalid") from None
    files = value.get("files")
    if not isinstance(files, list) or len(files) > limits.entries:
        raise MigrationError("migration_entry_limit")
    roots = value.get("roots")
    if not isinstance(roots, dict) or roots.get("project") != {"kind": "project"}:
        raise MigrationError("migration_logical_root_invalid")
    for name, descriptor in roots.items():
        if "/" in logical_path(name) or not isinstance(descriptor, dict):
            raise MigrationError("migration_logical_root_invalid")
        if set(descriptor) != {"kind"} or descriptor["kind"] not in {
            "project",
            "certificate",
            "database",
            "extension",
        }:
            raise MigrationError("migration_logical_root_invalid")
    logical = PathIndex()
    payloads = set()
    total = 0
    for entry in files:
        if not isinstance(entry, dict):
            raise MigrationError("migration_manifest_invalid")
        try:
            root, path = entry["root"], logical_path(entry["path"])
            payload = logical_path(entry["payload"])
            size, digest = entry["size"], entry["sha256"]
            if root not in roots or entry["category"] not in CATEGORIES | {"database"}:
                raise ValueError
            logical.add(f"{root}/{path}")
            if not payload.startswith("payload/") or payload in payloads:
                raise ValueError
            if type(size) is not int or size < 0 or infos[payload].file_size != size:
                raise ValueError
            if not isinstance(digest, str) or len(digest) != 64:
                raise ValueError
            if any(c not in "0123456789abcdef" for c in digest):
                raise ValueError
        except (ValueError, TypeError, KeyError):
            raise MigrationError("migration_manifest_invalid") from None
        payloads.add(payload)
        total += size
    if total > limits.expanded:
        raise MigrationError("migration_expanded_limit", status=413)
    if set(infos) != payloads | {"manifest.json"}:
        raise MigrationError("migration_unlisted_payload")
    return value


@contextmanager
def open_archive(
    path: Path, *, password: bytes | None = None, limits: Limits = Limits()
) -> Iterator[tuple[object, dict]]:
    try:
        _central_directory_limit(path, limits)
        with _zip_class(password)(path, "r") as archive:
            if password:
                archive.setpassword(password)
            index = PathIndex()
            infos = {}
            total = 0
            encryption_modes = set()
            for info in archive.infolist():
                name = index.add(info.filename)
                mode = info.external_attr >> 16
                if (
                    info.is_dir()
                    or info.external_attr & (0x400 | 0x10)
                    or (stat.S_IFMT(mode) not in {0, stat.S_IFREG})
                    or info.orig_filename != info.filename
                ):
                    raise MigrationError("migration_special_file", path=name)
                encrypted = bool(info.flag_bits & 1)
                encryption_modes.add(encrypted)
                if encrypted and not password:
                    raise MigrationError("migration_password_required")
                if encrypted and getattr(info, "wz_aes_strength", None) != 3:
                    raise MigrationError("migration_encryption_unsupported")
                if info.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
                    raise MigrationError("migration_compression_unsupported")
                infos[name] = info
                total += info.file_size
                if total > limits.expanded + MANIFEST_LIMIT:
                    raise MigrationError("migration_expanded_limit", status=413)
            if len(infos) > limits.entries + 1:
                raise MigrationError("migration_entry_limit")
            if len(encryption_modes) > 1:
                raise MigrationError("migration_mixed_encryption")
            if (
                "manifest.json" not in infos
                or infos["manifest.json"].file_size > MANIFEST_LIMIT
            ):
                raise MigrationError("migration_manifest_invalid")
            with archive.open("manifest.json") as stream:
                raw = stream.read(MANIFEST_LIMIT + 1)
            if len(raw) > MANIFEST_LIMIT:
                raise MigrationError("migration_manifest_limit")
            _manifest_budget(raw, limits)
            manifest = json.loads(raw, object_pairs_hook=_unique_object)
            yield archive, _validate_manifest(manifest, infos, limits)
    except MigrationError:
        raise
    except (zipfile.BadZipFile, RuntimeError, NotImplementedError):
        raise MigrationError("migration_archive_invalid_or_password") from None
    except (ValueError, UnicodeError, EOFError, struct.error, RecursionError):
        raise MigrationError("migration_archive_invalid") from None


def verify_archive(
    path: Path,
    *,
    password: bytes | None = None,
    limits: Limits = Limits(),
    checkpoint: Callable[[], None] = _checkpoint,
) -> dict:
    with open_archive(path, password=password, limits=limits) as (archive, manifest):
        total = 0
        for entry in manifest["files"]:
            digest = hashlib.sha256()
            actual = 0
            with archive.open(entry["payload"]) as stream:
                while data := stream.read(CHUNK):
                    checkpoint()
                    actual += len(data)
                    total += len(data)
                    if actual > entry["size"] or total > limits.expanded:
                        raise MigrationError("migration_expanded_limit")
                    digest.update(data)
            if actual != entry["size"] or digest.hexdigest() != entry["sha256"]:
                raise MigrationError(
                    "migration_payload_hash_mismatch", path=entry["path"]
                )
        return manifest


def build_archive(
    snapshot: Path,
    files: list[FileEntry],
    destination: Path,
    *,
    metadata: dict,
    password: bytes | None = None,
    plaintext_confirmed: bool = False,
    limits: Limits = Limits(),
    checkpoint: Callable[[], None] = _checkpoint,
    publish: Callable[[Path, Path, dict], None] | None = None,
) -> dict:
    if not password and not plaintext_confirmed:
        raise MigrationError("migration_plaintext_confirmation_required")
    if destination.suffix.lower() != ".zx":
        raise MigrationError("migration_extension_required")
    if len(files) > limits.entries or sum(f.size for f in files) > limits.expanded:
        raise MigrationError("migration_export_limit")
    if destination.exists():
        raise MigrationError("migration_output_exists", status=409)
    destination.parent.mkdir(parents=True, exist_ok=True)
    require_space(destination.parent, sum(f.size for f in files))
    manifest = {
        "format": "zhenxun-instance",
        "schema": SCHEMA,
        "package_id": str(uuid.uuid4()),
        "snapshot_at": time.time(),
        "roots": {"project": {"kind": "project"}},
        "source": metadata,
        "files": [],
    }
    index = PathIndex()
    temporary_directory = Path(
        tempfile.mkdtemp(prefix=".zx-export-", dir=destination.parent)
    )
    private_directory(temporary_directory)
    temporary = temporary_directory / "archive.part"
    try:
        with _zip_class(password)(
            temporary, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True
        ) as archive:
            if password:
                import pyzipper

                archive.setpassword(password)
                archive.setencryption(pyzipper.WZ_AES, nbits=256)
            for number, entry in enumerate(files):
                checkpoint()
                index.add(f"{entry.root}/{entry.path}")
                if entry.root != "project" or entry.category not in CATEGORIES | {
                    "database"
                }:
                    raise MigrationError("migration_logical_root_invalid")
                source = contained_path(snapshot, entry.path, regular=True)
                before = source.stat()
                if before.st_size != entry.size or before.st_mtime_ns != entry.mtime_ns:
                    raise MigrationError("migration_snapshot_changed", path=entry.path)
                digest = hashlib.sha256()
                payload = f"payload/{number:08d}"
                count = 0
                with (
                    source.open("rb") as reader,
                    archive.open(payload, "w", force_zip64=True) as writer,
                ):
                    opened = os.fstat(reader.fileno())
                    if (opened.st_ino, opened.st_dev) != (before.st_ino, before.st_dev):
                        raise MigrationError(
                            "migration_snapshot_changed", path=entry.path
                        )
                    while data := reader.read(CHUNK):
                        checkpoint()
                        count += len(data)
                        if count > entry.size:
                            raise MigrationError(
                                "migration_snapshot_changed", path=entry.path
                            )
                        writer.write(data)
                        digest.update(data)
                after = contained_path(snapshot, entry.path, regular=True).stat()
                if count != entry.size or (
                    before.st_mtime_ns,
                    before.st_ino,
                    before.st_dev,
                ) != (after.st_mtime_ns, after.st_ino, after.st_dev):
                    raise MigrationError("migration_snapshot_changed", path=entry.path)
                manifest["files"].append(
                    {
                        "root": entry.root,
                        "path": entry.path,
                        "category": entry.category,
                        "size": count,
                        "sha256": digest.hexdigest(),
                        "payload": payload,
                    }
                )
                if temporary.stat().st_size > limits.compressed:
                    raise MigrationError("migration_compressed_limit")
            raw = json.dumps(manifest, ensure_ascii=True, sort_keys=True).encode()
            if len(raw) > MANIFEST_LIMIT:
                raise MigrationError("migration_manifest_limit")
            archive.writestr("manifest.json", raw)
        if temporary.stat().st_size > limits.compressed:
            raise MigrationError("migration_compressed_limit")
        verify_archive(
            temporary, password=password, limits=limits, checkpoint=checkpoint
        )
        with temporary.open("rb+") as stream:
            os.fsync(stream.fileno())
        digest = file_hash(temporary, checkpoint)
        result = {
            "package_id": manifest["package_id"],
            "sha256": digest,
            "size": temporary.stat().st_size,
            "files": len(files),
            "encrypted": bool(password),
        }
        # Publish without overwriting a concurrently created destination.
        try:
            if publish is None:
                checkpoint()
                os.link(temporary, destination)
            else:
                publish(temporary, destination, result)
        except FileExistsError:
            raise MigrationError("migration_output_exists", status=409) from None
        return result
    finally:
        temporary.unlink(missing_ok=True)
        temporary_directory.rmdir()


def extract_verified(
    path: Path,
    staging: Path,
    *,
    password: bytes | None = None,
    limits: Limits = Limits(),
    checkpoint: Callable[[], None] = _checkpoint,
) -> dict:
    """Extract into a new private stage, never into the target instance."""
    contained_path(staging.parent, staging.name)
    if staging.exists():
        raise MigrationError("migration_stage_exists", status=409)
    with open_archive(path, password=password, limits=limits) as (archive, manifest):
        require_space(staging.parent, sum(f["size"] for f in manifest["files"]))
        private_directory(staging)
        try:
            total = 0
            for entry in manifest["files"]:
                destination = contained_path(staging, entry["payload"])
                destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                actual = 0
                digest = hashlib.sha256()
                with (
                    archive.open(entry["payload"]) as reader,
                    destination.open("xb") as writer,
                ):
                    while data := reader.read(CHUNK):
                        checkpoint()
                        actual += len(data)
                        total += len(data)
                        if actual > entry["size"] or total > limits.expanded:
                            raise MigrationError("migration_expanded_limit")
                        digest.update(data)
                        writer.write(data)
                    writer.flush()
                    os.fsync(writer.fileno())
                if actual != entry["size"] or digest.hexdigest() != entry["sha256"]:
                    raise MigrationError(
                        "migration_payload_hash_mismatch", path=entry["path"]
                    )
            return manifest
        except BaseException:
            # This is the new private directory created above, not an archive path.
            shutil.rmtree(staging)
            raise
