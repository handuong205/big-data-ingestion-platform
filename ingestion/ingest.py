"""Load the raw CSV files into the MinIO raw layer.

Idempotent: an object whose stored sha256 matches the local file is skipped,
and every upload overwrites the same key, so re-running never duplicates data.
Integrity: size, sha256 (re-read from MinIO) and CSV record count are checked
and written to ingestion/logs/ingestion.jsonl.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

RAW_DIR = PROJECT_ROOT / "data" / "raw"
LOG_FILE = PROJECT_ROOT / "ingestion" / "logs" / "ingestion.jsonl"

# local file -> object key inside the bucket
SOURCES = {
    "books_data.csv": "raw/books/books_data.csv",
    "Books_rating.csv": "raw/reviews/Books_rating.csv",
}

CHUNK_SIZE = 8 * 1024 * 1024
TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=64 * 1024 * 1024,
    multipart_chunksize=64 * 1024 * 1024,
    max_concurrency=4,
)

# Some review cells are very long; the default 128 KB field limit is too small.
csv.field_size_limit(2**31 - 1)


class HashingReader(io.RawIOBase):
    """Binary reader that hashes every byte it hands out."""

    def __init__(self, raw: io.BufferedReader) -> None:
        self._raw = raw
        self.sha256 = hashlib.sha256()

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        n = self._raw.readinto(buffer)
        if n:
            self.sha256.update(memoryview(buffer)[:n])
        return n or 0


def profile_file(path: Path) -> tuple[str, int]:
    """Return (sha256, record count) in a single pass over the file.

    Records are counted with the csv module because review text contains
    quoted newlines, so physical line count != record count.
    """
    with path.open("rb") as raw:
        hasher = HashingReader(raw)
        text = io.TextIOWrapper(
            io.BufferedReader(hasher, CHUNK_SIZE),
            encoding="utf-8",
            errors="replace",
            newline="",
        )
        rows = sum(1 for _ in csv.reader(text)) - 1  # minus header
        # Drain anything the csv reader did not consume.
        while text.read(CHUNK_SIZE):
            pass
        return hasher.sha256.hexdigest(), max(rows, 0)


def remote_sha256(s3, bucket: str, key: str) -> str:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    digest = hashlib.sha256()
    for chunk in body.iter_chunks(CHUNK_SIZE):
        digest.update(chunk)
    return digest.hexdigest()


def head(s3, bucket: str, key: str) -> dict | None:
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"Created bucket {bucket}")


def ingest_file(
    s3, bucket: str, path: Path, key: str, force: bool, verify: bool
) -> dict:
    record: dict = {
        "file": path.name,
        "object": f"s3://{bucket}/{key}",
        "size_bytes": path.stat().st_size,
    }

    started = time.perf_counter()
    sha, rows = profile_file(path)
    record.update(
        sha256=sha, row_count=rows, profile_seconds=round(time.perf_counter() - started, 2)
    )

    existing = head(s3, bucket, key)
    if (
        not force
        and existing is not None
        and existing["Metadata"].get("sha256") == sha
        and existing["ContentLength"] == record["size_bytes"]
    ):
        record["status"] = "SKIPPED"
        return record

    started = time.perf_counter()
    s3.upload_file(
        str(path),
        bucket,
        key,
        ExtraArgs={
            "ContentType": "text/csv",
            "Metadata": {
                "sha256": sha,
                "row_count": str(rows),
                "source_size": str(record["size_bytes"]),
            },
        },
        Config=TRANSFER_CONFIG,
    )
    upload_seconds = time.perf_counter() - started
    record["upload_seconds"] = round(upload_seconds, 2)
    record["throughput_mb_s"] = round(record["size_bytes"] / 2**20 / max(upload_seconds, 1e-6), 2)

    uploaded = head(s3, bucket, key)
    if uploaded is None or uploaded["ContentLength"] != record["size_bytes"]:
        raise RuntimeError(f"Size mismatch after upload for {key}")

    if verify:
        started = time.perf_counter()
        remote = remote_sha256(s3, bucket, key)
        record["verify_seconds"] = round(time.perf_counter() - started, 2)
        if remote != sha:
            raise RuntimeError(f"sha256 mismatch for {key}: local {sha}, remote {remote}")
        record["verified"] = True

    record["status"] = "UPLOADED"
    return record


def write_log(record: dict) -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest raw CSVs into MinIO.")
    parser.add_argument("--force", action="store_true", help="Upload even if unchanged.")
    parser.add_argument(
        "--skip-verify", action="store_true", help="Skip re-reading objects to check sha256."
    )
    args = parser.parse_args()

    bucket = os.getenv("MINIO_BUCKET", "raw-data")
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
    )
    ensure_bucket(s3, bucket)

    run_id = uuid.uuid4().hex[:12]
    failed = False
    for file_name, key in SOURCES.items():
        path = RAW_DIR / file_name
        base = {"run_id": run_id, "timestamp": datetime.now(timezone.utc).isoformat()}
        if not path.exists():
            record = {**base, "file": file_name, "status": "FAILED", "error": "source file not found"}
        else:
            try:
                record = {**base, **ingest_file(s3, bucket, path, key, args.force, not args.skip_verify)}
            except Exception as exc:  # log and continue with the next file
                record = {**base, "file": file_name, "status": "FAILED", "error": str(exc)}
        failed |= record["status"] == "FAILED"
        write_log(record)
        print(json.dumps(record, ensure_ascii=False))

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
