"""Build a per-crawl manifest of Greek pages from Common Crawl's columnar index.

Common Crawl no longer allows unsigned/anonymous S3 access. The free HTTPS
mirror (data.commoncrawl.org, backed by CloudFront) throttles hard under
anonymous traffic with repeated 503 SlowDown errors, so part-file scans go
over signed S3 instead, which we verified hits no throttling at all. The
crawl's own manifest listing (cc-index-table.paths.gz) is small and unrelated
to that bottleneck, so it stays on the free HTTPS mirror to avoid needing
credentials just to resolve part-file paths.

See plan_of_action.md §4.3 (index scan) and §4.4 (within-crawl digest dedup).
"""

import gzip
import logging
import os
import time
import urllib.request
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

DATA_ROOT = "https://data.commoncrawl.org"
S3_ROOT = "s3://commoncrawl"

MANIFEST_COLUMNS = (
    "url",
    "url_host_registered_domain",
    "content_digest",
    "content_languages",
    "warc_filename",
    "warc_record_offset",
    "warc_record_length",
)


def warc_parquet_urls(crawl: str) -> list[str]:
    paths_url = f"{DATA_ROOT}/crawl-data/{crawl}/cc-index-table.paths.gz"
    # this Pi's IPv6 route is broken (SLAAC address present, but nothing routes),
    # so an unbounded urlopen() hangs for minutes on IPv6 before falling back to
    # the working IPv4 address. A short timeout forces that fallback quickly.
    with urllib.request.urlopen(paths_url, timeout=15) as resp:
        paths = gzip.decompress(resp.read()).decode("utf-8").splitlines()
    return [
        f"{S3_ROOT}/{path}" for path in paths if f"crawl={crawl}/subset=warc/" in path
    ]


def _s3_storage_options() -> dict[str, str]:
    return {
        "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "aws_region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    }


def fragments_dir(crawl: str, out_dir: Path) -> Path:
    return out_dir / crawl / "fragments"


def fetch_part_manifest(
    crawl: str, part_url: str, part_index: int, out_dir: Path
) -> tuple[Path, int]:
    """Scan one index part-file, write its filtered rows as a fragment Parquet.

    Deliberately does not deduplicate: content_digest dedup must run once across
    the whole crawl (plan_of_action.md §4.4), since the same payload can appear
    in two different part-files.
    """
    logger.info("%s part %d: fetching %s", crawl, part_index, part_url)
    started = time.monotonic()

    manifest = (
        pl.scan_parquet(part_url, hive_partitioning=False, storage_options=_s3_storage_options())
        .filter(
            (pl.col("fetch_status") == 200)
            # exact match on a comma-separated ISO-639-3 list, not a substring test
            & pl.col("content_languages").str.split(",").list.contains("ell")
            & pl.col("content_mime_detected").is_in(
                ["text/html", "application/xhtml+xml"]
            )
        )
        .select(*MANIFEST_COLUMNS)
        .collect(engine="streaming")
    )

    scan_seconds = time.monotonic() - started
    logger.info(
        "%s part %d: scanned in %.1fs, %d matching rows", crawl, part_index, scan_seconds, len(manifest)
    )

    dest_dir = fragments_dir(crawl, out_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    fragment_path = dest_dir / f"part-{part_index:05d}.parquet"
    manifest.write_parquet(fragment_path, compression="zstd")

    logger.info("%s part %d: %d rows -> %s", crawl, part_index, len(manifest), fragment_path)
    return fragment_path, len(manifest)


def merge_crawl_manifest(crawl: str, fragment_paths: list[Path], out_dir: Path) -> int:
    """Concatenate fragments, dedup once crawl-wide, write the final manifest."""
    manifest = (
        pl.concat([pl.scan_parquet(p, hive_partitioning=False) for p in fragment_paths])
        .sort("warc_filename", "warc_record_offset")
        .unique(subset=["content_digest"], keep="first", maintain_order=True)
        .collect(engine="streaming")
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"{crawl}.parquet"
    manifest.write_parquet(output_path, compression="zstd")

    logger.info(
        "%s merged %d fragments -> %d rows at %s",
        crawl,
        len(fragment_paths),
        len(manifest),
        output_path,
    )
    return len(manifest)
