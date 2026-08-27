"""Custom datatrove reader: range-fetches Common Crawl WARC records over signed S3.

Reuses `process_record()` from datatrove's own `WarcReader` for mime/charset
handling, so a manifest row (crawl, warc_filename, offset, length) becomes a
Document exactly as WarcReader would produce it from a local WARC file --
everything after this step in the pipeline is unmodified datatrove/FineWeb-2
code. See plan_of_action.md §4.5: a range read of exactly
[offset, offset+length) is a complete, independently decompressible gzip
member, no need to touch the rest of the WARC file.

Concurrency is a bounded ThreadPoolExecutor around boto3 (not asyncio) --
simpler and equally effective for this I/O-bound access pattern.
"""

import io
import os
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from pathlib import Path

import boto3
import polars as pl
from botocore.config import Config as BotoConfig
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
        max_in_flight: int = 16,
    ):
        super().__init__(limit=limit, default_metadata={"crawl": crawl})
        self.manifest_path = manifest_path
        self.offset = offset
        self.max_in_flight = max_in_flight
        self._s3 = None

    def _client(self):
        if self._s3 is None:
            self._s3 = boto3.client(
                "s3",
                aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                # urllib3's default pool (10) is smaller than max_in_flight (16)
                # threads sharing this one client -- every request past 10 was
                # discarding its connection and opening a fresh one (each with
                # its own DNS lookup). That churn flooded the Pi's local
                # resolver hard enough to hang the whole system and force a
                # reboot mid-run (dockerd logs showed hundreds of DNS timeouts
                # for commoncrawl.s3.amazonaws.com in the seconds before it went
                # down). Sizing the pool to match max_in_flight lets threads
                # reuse connections instead of constantly reconnecting.
                config=BotoConfig(max_pool_connections=self.max_in_flight),
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

        rows = (
            pl.scan_parquet(self.manifest_path)
            .slice(self.offset, self.limit if self.limit != -1 else None)
            .collect()
            .to_dicts()
        )

        # Sliding window rather than pool.map: map() submits every row at once,
        # so the fetch threads race ahead of the (much slower) downstream
        # extraction and every fetched document sits buffered in RAM. With a
        # 100k-row chunk and trafilatura taking up to its 10s timeout per doc,
        # that read-ahead grew unbounded and drove the Pi into memory pressure
        # hard enough to take the whole machine down. Keeping exactly
        # max_in_flight requests outstanding bounds the buffer while still
        # keeping every thread busy.
        with ThreadPoolExecutor(max_workers=self.max_in_flight) as pool:
            row_iter = iter(rows)
            futures = deque(
                pool.submit(self._fetch_row, row)
                for row in islice(row_iter, self.max_in_flight)
            )
            while futures:
                doc = futures.popleft().result()
                next_row = next(row_iter, None)
                if next_row is not None:
                    futures.append(pool.submit(self._fetch_row, next_row))
                if doc is not None:
                    yield doc
