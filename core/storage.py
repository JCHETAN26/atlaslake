"""Object storage helpers (MinIO locally, S3 in AWS) built on pyarrow's S3 filesystem."""

from __future__ import annotations

from functools import lru_cache

from pyarrow import fs

from core.config import settings


@lru_cache(maxsize=1)
def s3fs() -> fs.S3FileSystem:
    cfg = settings().s3
    return fs.S3FileSystem(
        access_key=cfg.access_key,
        secret_key=cfg.secret_key,
        endpoint_override=cfg.endpoint_host,
        scheme="https" if cfg.use_ssl else "http",
        region=cfg.region,
        allow_bucket_creation=True,
    )


def ensure_bucket() -> None:
    bucket = settings().s3.bucket
    info = s3fs().get_file_info(bucket)
    if info.type == fs.FileType.NotFound:
        s3fs().create_dir(bucket)


def file_stats(prefix: str) -> tuple[int, int]:
    """(file_count, total_bytes) under an s3://bucket/prefix path."""
    path = prefix.removeprefix("s3://")
    infos = s3fs().get_file_info(fs.FileSelector(path, recursive=True, allow_not_found=True))
    files = [i for i in infos if i.type == fs.FileType.File]
    return len(files), sum(i.size or 0 for i in files)
