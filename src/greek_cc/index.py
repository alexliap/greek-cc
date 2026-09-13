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
import shutil
import time
import urllib.request
from pathlib import Path

import polars as pl
from huggingface_hub import hf_hub_download

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
        pl.scan_parquet(
            part_url, hive_partitioning=False, storage_options=_s3_storage_options()
        )
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
        "%s part %d: scanned in %.1fs, %d matching rows",
        crawl,
        part_index,
        scan_seconds,
        len(manifest),
    )

    dest_dir = fragments_dir(crawl, out_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    fragment_path = dest_dir / f"part-{part_index:05d}.parquet"
    manifest.write_parquet(fragment_path, compression="zstd")

    logger.info(
        "%s part %d: %d rows -> %s", crawl, part_index, len(manifest), fragment_path
    )
    return fragment_path, len(manifest)


SLICE_ROWS = 10_000_000


def _sort_dedup_sink(lf: pl.LazyFrame, path: Path) -> None:
    (
        lf.sort("warc_filename", "warc_record_offset")
        .unique(subset=["content_digest"], keep="first", maintain_order=True)
        .sink_parquet(path, compression="zstd", engine="streaming")
    )


def merge_crawl_manifest(crawl: str, fragment_paths: list[Path], out_dir: Path) -> int:
    """Concatenate fragments, dedup once crawl-wide, write the final manifest.

    Sorting+deduping the whole crawl in one pass is what SIGKILLed this task on
    a larger crawl even with 8GB available and sink_parquet(engine="streaming")
    -- a global sort's working set still scales with total row count. Instead,
    this slices the *unsorted* concat into SLICE_ROWS-row chunks first and
    sorts+dedupes each chunk on its own (a much smaller working set), then does
    one more sort+dedup pass over the slices' combined output -- small by then,
    since each slice already dropped most of its duplicates -- to catch any
    duplicate that happened to land on opposite sides of a slice boundary.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"{crawl}.parquet"

    base = pl.concat([pl.scan_parquet(p, hive_partitioning=False) for p in fragment_paths])
    total_rows = base.select(pl.len()).collect().item()

    tmp_dir = out_dir / crawl / "_merge_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    try:
        slice_paths = []
        for i, offset in enumerate(range(0, total_rows, SLICE_ROWS)):
            slice_path = tmp_dir / f"slice-{i:04d}.parquet"
            _sort_dedup_sink(base.slice(offset, SLICE_ROWS), slice_path)
            slice_paths.append(slice_path)

        _sort_dedup_sink(
            pl.concat([pl.scan_parquet(p, hive_partitioning=False) for p in slice_paths]),
            output_path,
        )
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    row_count = pl.scan_parquet(output_path).select(pl.len()).collect().item()

    logger.info(
        "%s merged %d fragments -> %d rows at %s",
        crawl,
        len(fragment_paths),
        row_count,
        output_path,
    )
    return row_count


def publish_manifest(output_path: Path) -> None:
    """Upload a merged crawl manifest to the HF Hub manifests repo.

    Mirrors extract.py's _publish for the extraction output: same
    HF_TOKEN/repo-env-var gated, warn-and-skip pattern, just a single file
    instead of a directory of shards.
    """
    if not os.environ.get("HF_TOKEN"):
        logger.warning("HF_TOKEN is not set -- skipping manifest upload")
        return
    repo_id = os.environ.get("HF_MANIFEST_REPO")
    if not repo_id:
        logger.warning("HF_TOKEN is set but HF_MANIFEST_REPO is not -- skipping upload")
        return

    from huggingface_hub import HfApi

    api = HfApi()
    api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)
    api.upload_file(
        path_or_fileobj=str(output_path),
        path_in_repo=output_path.name,
        repo_id=repo_id,
        repo_type="dataset",
    )
    logger.info(
        "uploaded %s -> hf://datasets/%s/%s", output_path, repo_id, output_path.name
    )


def download_manifest(crawl: str, out_dir: Path) -> Path:
    """Download a crawl's merged manifest from the HF Hub manifests repo.

    Unlike publish_manifest, this does not warn-and-skip on missing config --
    a manifest is a hard prerequisite for extraction, so a misconfigured or
    missing HF_MANIFEST_REPO/file should fail loudly.
    """
    repo_id = os.environ.get("HF_MANIFEST_REPO")
    if not repo_id:
        raise RuntimeError("HF_MANIFEST_REPO is not set -- cannot download manifest")

    path = hf_hub_download(
        repo_id=repo_id,
        repo_type="dataset",
        filename=f"{crawl}.parquet",
        local_dir=out_dir,
        token=os.environ.get("HF_TOKEN"),
    )
    logger.info("downloaded hf://datasets/%s/%s.parquet -> %s", repo_id, crawl, path)
    return Path(path)
