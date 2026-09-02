"""Custom datatrove reader: range-fetches Common Crawl WARC records over signed S3.

Reuses `process_record()` from datatrove's own `WarcReader` for mime/charset
handling, so a manifest row (crawl, warc_filename, offset, length) becomes a
Document exactly as WarcReader would produce it from a local WARC file --
everything after this step in the pipeline is unmodified datatrove/FineWeb-2
code. See plan_of_action.md §4.5: a range read of exactly
[offset, offset+length) is a complete, independently decompressible gzip
member, no need to touch the rest of the WARC file.

Fetches one row at a time per worker -- concurrency comes entirely from
`tasks` (extract.py), not from threading inside the reader. That keeps total
concurrent S3 connections equal to `tasks`, one number instead of two
multiplied together.
"""

import io
import os
import time
from pathlib import Path

import boto3
import polars as pl
from botocore.exceptions import BotoCoreError, ClientError
from datatrove.data import Document, DocumentsPipeline
from datatrove.pipeline.readers.base import BaseReader
from datatrove.pipeline.readers.warc import process_record
from warcio.archiveiterator import ArchiveIterator

S3_BUCKET = "commoncrawl"

# resp["Body"].read() streaming errors (e.g. ReadTimeoutError) happen after
# boto3's own client-level retry config has already been satisfied by a
# successful initial response -- they need their own explicit retry, or a
# single transient network hiccup kills the whole chunk over a run that does
# tens of thousands of these fetches
FETCH_MAX_ATTEMPTS = 4
FETCH_RETRY_BACKOFF_SECONDS = 2


class CCIndexGreekReader(BaseReader):
    """Reads up to `limit` rows starting at `offset` from a manifest Parquet,
    range-fetching each WARC record.

    `offset`/`limit` bound how many manifest rows get materialized into
    Python dicts at once -- pass a bounded chunk (not the whole manifest) for
    a multi-million-row crawl, or `.to_dicts()` on the full result set alone
    can run a Pi out of memory before any doc is even fetched.
    """

    name = "🇬🇷 CC Index Reader"
    _requires_dependencies = [
        "boto3",
        "warcio",
        ("cchardet", "faust-cchardet"),
        ("magic", "python-magic"),
    ]

    def __init__(
        self,
        crawl: str,
        manifest_path: Path,
        offset: int = 0,
        limit: int = -1,
    ):
        super().__init__(limit=limit, default_metadata={"crawl": crawl})
        self.manifest_path = manifest_path
        self.offset = offset
        self._s3 = None

    def _client(self):
        if self._s3 is None:
            self._s3 = boto3.client(
                "s3",
                aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            )
        return self._s3

    def _fetch_bytes(self, key: str, start: int, end: int) -> bytes:
        last_error = None
        for attempt in range(FETCH_MAX_ATTEMPTS):
            try:
                resp = self._client().get_object(
                    Bucket=S3_BUCKET, Key=key, Range=f"bytes={start}-{end}"
                )
                return resp["Body"].read()
            except (BotoCoreError, ClientError) as exc:
                last_error = exc
                if attempt < FETCH_MAX_ATTEMPTS - 1:
                    time.sleep(FETCH_RETRY_BACKOFF_SECONDS * (attempt + 1))
        raise last_error

    def _fetch_row(self, row: dict) -> Document | None:
        start = row["warc_record_offset"]
        end = start + row["warc_record_length"] - 1
        raw = self._fetch_bytes(row["warc_filename"], start, end)
        record = next(ArchiveIterator(io.BytesIO(raw)), None)
        if record is None:
            return None

        extracted = process_record(record)
        if not extracted:
            return None

        # content_digest is stable across re-runs; WARC-Record-ID is not
        # guaranteed to be, so prefer it as this document's id
        extracted["id"] = row["content_digest"]
        extracted["content_digest"] = row["content_digest"]
        extracted["url_host_registered_domain"] = row["url_host_registered_domain"]
        extracted["warc_filename"] = row["warc_filename"]
        extracted["warc_record_offset"] = start

        return self.get_document_from_dict(extracted, row["warc_filename"], start)

    def run(
        self, data: DocumentsPipeline = None, rank: int = 0, world_size: int = 1
    ) -> DocumentsPipeline:
        if data:
            yield from data

        chunk = (
            pl.scan_parquet(self.manifest_path)
            .slice(self.offset, self.limit if self.limit != -1 else None)
            .collect()
        )
        # world_size>1 means the executor's chunk got fanned out across worker
        # processes (see extract.py's `tasks`) -- without an explicit per-rank
        # slice here every worker would independently re-fetch and reprocess
        # this reader's *entire* offset/limit window, multiplying S3 cost by
        # world_size for zero speedup instead of splitting the work.
        if world_size > 1:
            base, remainder = divmod(len(chunk), world_size)
            start = rank * base + min(rank, remainder)
            length = base + (1 if rank < remainder else 0)
            chunk = chunk.slice(start, length)

        for row in chunk.to_dicts():
            doc = self._fetch_row(row)
            if doc is not None:
                yield doc
