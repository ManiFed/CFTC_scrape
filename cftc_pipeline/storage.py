"""Storage abstraction — local filesystem or S3."""
from __future__ import annotations

import hashlib
import io
import shutil
from pathlib import Path
from typing import Optional

from cftc_pipeline.config import settings


class StorageBackend:
    """Abstract interface for file storage."""

    def write(self, key: str, data: bytes) -> str:
        raise NotImplementedError

    def read(self, key: str) -> bytes:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError

    def url(self, key: str) -> str:
        raise NotImplementedError


class LocalStorage(StorageBackend):
    def __init__(self, base_path: Path):
        self.base = base_path
        self.base.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        p = self.base / key
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def write(self, key: str, data: bytes) -> str:
        p = self._path(key)
        p.write_bytes(data)
        return str(p)

    def read(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def url(self, key: str) -> str:
        return str(self._path(key))


class S3Storage(StorageBackend):
    def __init__(self, bucket: str):
        import boto3

        self.bucket = bucket
        self.client = boto3.client("s3")

    def write(self, key: str, data: bytes) -> str:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
        return f"s3://{self.bucket}/{key}"

    def read(self, key: str) -> bytes:
        resp = self.client.get_object(Bucket=self.bucket, Key=key)
        return resp["Body"].read()

    def exists(self, key: str) -> bool:
        import botocore

        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except botocore.exceptions.ClientError:
            return False

    def url(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"


def get_storage() -> StorageBackend:
    if settings.storage_backend == "s3":
        return S3Storage(settings.s3_bucket)
    return LocalStorage(settings.storage_base_path)


storage = get_storage()


def html_key(docket_id: str, page: str) -> str:
    return f"raw/html/{docket_id}/{page}.html"


def attachment_key(docket_id: str, external_id: str, filename: str) -> str:
    return f"raw/attachments/{docket_id}/{external_id}/{filename}"


def detail_html_key(docket_id: str, external_id: str) -> str:
    return f"raw/html/{docket_id}/detail_{external_id}.html"


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
