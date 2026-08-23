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
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import polars as pl
from datatrove.data import Document, DocumentsPipeline
from datatrove.pipeline.readers.base import BaseReader
from datatrove.pipeline.readers.warc import process_record
from warcio.archiveiterator import ArchiveIterator

S3_BUCKET = "commoncrawl"


class CCIndexGreekReader(BaseReader):
    """Reads up to `limit` rows from a manifest Parquet, range-fetching each WARC record."""

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
        limit: int = -1,
        max_in_flight: int = 16,
    ):
        super().__init__(limit=limit, default_metadata={"crawl": crawl})
        self.manifest_path = manifest_path
        self.max_in_flight = max_in_flight
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

    def _fetch_row(self, row: dict) -> Document | None:
        start = row["warc_record_offset"]
        end = start + row["warc_record_length"] - 1
        resp = self._client().get_object(
            Bucket=S3_BUCKET, Key=row["warc_filename"], Range=f"bytes={start}-{end}"
        )
        raw = resp["Body"].read()
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
            .slice(0, self.limit if self.limit != -1 else None)
            .collect()
            .to_dicts()
        )

        with ThreadPoolExecutor(max_workers=self.max_in_flight) as pool:
            for doc in pool.map(self._fetch_row, rows):
                if doc is not None:
                    yield doc
