"""Assemble and run the Phase 2 extraction pipeline for one crawl (plan_of_action.md §5).

Reuses FineWeb-2's own datatrove filter chain and ell_Grek.yml thresholds
verbatim (§5.3: "Do not hand-tune these yourself" -- the config in configs/
is vendored straight from https://github.com/huggingface/fineweb-2). The only
custom pieces are the reader (range-fetch from S3 instead of a local WARC
file, see warc_reader.py) and the dedup step (online per-crawl tagging
instead of an offline global pass, see dedup.py). No C4 filters -- confirmed
against FineWeb-2's actual pipeline script
(fineweb-2-pipeline.py: "# we do not apply the C4 filters"), which is
authoritative over plan_of_action.md §5.2's diagram (that line appears stale).

Docs dropped by each filter aren't written anywhere (no exclusion_writer) --
§5.5's `_removed` sibling config is deliberately skipped to avoid extra
Parquet clutter.

Split into two stages because a full crawl's manifest is tens of millions of
rows, and `CCIndexGreekReader` materializes whatever slice it's given as
Python dicts up front (warc_reader.py) -- pointing that at an entire manifest
at once OOM-crashed the Pi twice. Stage 1 runs everything up to (but not
including) the per-crawl MinHash dedup over one bounded manifest chunk at a
time -- most rows get dropped by these filters, so each chunk's survivors are
a small Parquet file under out_dir/_stage_1/{crawl}/chunk_{offset}/. Stage 2
runs once all chunks are done: reads every chunk's survivors back in (small
enough to buffer, unlike the raw manifest), runs the online MinHash dedup
(which needs a whole-crawl view to catch cross-chunk near-duplicates and, by
design, buffers its full input -- see dedup.py), and writes the final output
to out_dir/{crawl}/, same location/shape as before this split.
"""

import logging
import os
import shutil
from functools import partial
from pathlib import Path

import polars as pl
import pyarrow as pa
import yaml
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.extractors import Trafilatura
from datatrove.pipeline.extractors.base import ExtractorSandbox
from datatrove.pipeline.filters import (
    FineWebQualityFilter,
    GopherQualityFilter,
    GopherRepetitionFilter,
    LambdaFilter,
    LanguageFilter,
    URLFilter,
)
from datatrove.pipeline.formatters import (
    FTFYFormatter,
    PIIFormatter,
    SymbolLinesFormatter,
)
from datatrove.pipeline.readers.parquet import ParquetReader
from datatrove.pipeline.writers.parquet import ParquetWriter
from huggingface_hub import HfApi

from greek_cc.dedup import OnlineMinhashDedup
from greek_cc.warc_reader import CCIndexGreekReader

logger = logging.getLogger(__name__)

# ExtractorSandbox._worker (Trafilatura's per-doc timeout subprocess) tries to
# raise its own oom_score_adj to 1000 on startup so the OOM killer prefers it
# over the main process - but that needs CAP_SYS_RESOURCE, which Docker does
# not grant by default. Without this patch the worker dies with a
# PermissionError before ever extracting anything, on every single document:
# the parent sees that as a timeout, respawns a fresh worker for the next doc,
# and the crash-loop leaks a Process+Pipe each time until the container hits
# its file-descriptor limit. This is a best-effort OOM hint, safe to skip.
ExtractorSandbox.set_oom_score_adj = lambda self, score: None

LANGUAGE = "ell_Grek"
CONFIG_PATH = Path(__file__).parent / "configs" / "ell_Grek.yml"

# Explicit schema for Stage 1's writer. LanguageFilter(keep_top_pairs_threshold=0.01)
# adds a *variable* number of top_language_{lang}_score metadata keys per doc
# (whichever languages exceed the threshold for that specific doc) -- without
# a fixed schema, ParquetWriter infers one per write-batch, so chunk files
# can end up with mismatched schemas that pl.scan_parquet can't read across.
# This also just drops those keys, which nothing downstream needs.
STAGE_1_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("text", pa.string()),
        ("crawl", pa.string()),
        ("content_digest", pa.string()),
        ("url", pa.string()),
        ("url_host_registered_domain", pa.string()),
        ("warc_filename", pa.string()),
        ("warc_record_offset", pa.int64()),
        ("language", pa.string()),
        ("language_score", pa.float64()),
        ("language_script", pa.string()),
    ]
)


def _load_filter_config() -> dict:
    return yaml.safe_load(CONFIG_PATH.read_text())


def _above_language_threshold(doc, threshold: float) -> bool:
    # LanguageFilter(label_only=True) only annotates -- this is the actual cut,
    # both on the predicted top language and its score (FineWeb-2 gets the
    # language check for free from its per-language folder split; we don't
    # have that here, so both conditions are checked explicitly)
    return (
        doc.metadata.get("language") == "ell"
        and doc.metadata.get("language_score", 0) >= threshold
    )


def _stage_1_chunk_dir(stage_1_dir: Path, crawl: str, offset: int) -> Path:
    return Path(stage_1_dir) / "_stage_1" / crawl / f"chunk_{offset:012d}"


def compute_chunk_bounds(manifest_path: Path, chunk_size: int) -> list[dict]:
    """Split a manifest's row range into bounded (offset, length) chunks.

    A metadata-only read (row count from the Parquet footer), not a
    materialization -- safe to call regardless of manifest size.
    """
    total_rows = pl.scan_parquet(manifest_path).select(pl.len()).collect().item()
    return [
        {"offset": offset, "length": min(chunk_size, total_rows - offset)}
        for offset in range(0, total_rows, chunk_size)
    ]


def run_extraction_stage_1_chunk(
    crawl: str,
    manifest_path: Path,
    stage_1_dir: Path,
    offset: int,
    length: int,
    tasks: int = 1,
    publish: bool = False,
) -> dict:
    """Run the filter chain (reader through formatters) over one manifest chunk.

    Idempotent/resumable for free: LocalPipelineExecutor skips re-running a
    chunk whose logging_dir already has a completions marker from a prior
    successful run, and ParquetWriter opens in "wb" mode, so a chunk killed
    mid-write is cleanly overwritten by the next attempt.
    """
    cfg = _load_filter_config()
    chunk_out = _stage_1_chunk_dir(stage_1_dir, crawl, offset)

    pipeline = [
        CCIndexGreekReader(crawl, manifest_path, offset=offset, limit=length),
        URLFilter(),
        # default timeout is 1s/doc -- too tight for the Pi's CPU combined with
        # favour_precision's slower parsing, dropping plenty of legitimately
        # extractable pages as spurious timeouts
        Trafilatura(favour_precision=True, timeout=10),
        LanguageFilter(
            backend="glotlid", label_only=True, keep_top_pairs_threshold=0.01
        ),
        LambdaFilter(
            filter_function=partial(
                _above_language_threshold, threshold=cfg["language_score"]
            ),
        ),
        GopherRepetitionFilter(
            language=LANGUAGE,
            # trafilatura already strips paragraphs, so the paragraph-level
            # repetition signals are meaningless here -- disabled per FineWeb-2's
            # own script, not a plan_of_action.md guess
            dup_para_frac=0,
            dup_line_char_frac=0,
            dup_para_char_frac=0,
            dup_line_frac=cfg["dup_line_frac"],
            top_n_grams=cfg["top_n_grams"],
            dup_n_grams=cfg["dup_n_grams"],
        ),
        FineWebQualityFilter(
            language=LANGUAGE,
            short_line_thr=999,  # disabled, matches FineWeb-2's Greek config
            char_duplicates_ratio=0.1,  # raised from FineWeb English's 0.01
            line_punct_thr=cfg["line_punct_thr"],
            new_line_ratio=cfg["new_line_ratio"],
        ),
        GopherQualityFilter(
            language=LANGUAGE,
            max_avg_word_length=cfg["max_avg_word_length"],
            min_avg_word_length=cfg["min_avg_word_length"],
            stop_words=cfg["stopwords"],
            max_non_alpha_words_ratio=cfg["max_non_alpha_words_ratio"],
            min_stop_words=2,
        ),
        # no C4 filters here -- see module docstring
        FTFYFormatter(),
        PIIFormatter(),
        SymbolLinesFormatter(symbols_to_remove=["|"], replace_char="\n"),
        ParquetWriter(
            str(chunk_out),
            compression="zstd",
            expand_metadata=True,
            schema=STAGE_1_SCHEMA,
            # default (1000) meant a worker could go through its entire
            # ~4-5k row shard without writing a single byte -- survivors are
            # rare enough (aggressive Greek-language + quality filtering over
            # mostly non-Greek Common Crawl) that batch_size=1000 gave no
            # visibility into whether a run was progressing or stalled.
            batch_size=100,
        ),
    ]

    executor = LocalPipelineExecutor(
        pipeline=pipeline,
        # tasks>1 makes datatrove fork a worker process per task (each loading
        # its own copy of the ~1.57GB GlotLID model plus pipeline state). Tried
        # tasks=2 on this Mac's Docker Desktop VM (7.65GB total) and it pushed
        # the scheduler container to 5.7GB/7.65GB, starving the apiserver of
        # CPU long enough that in-flight task heartbeat JWTs expired waiting to
        # be validated -- the apiserver logged 50-80s request latencies and
        # jwt.exceptions.ExpiredSignatureError, and the resulting 403 reset the
        # running chunk. That was under Airflow/Docker Desktop specifically;
        # extraction no longer runs there (see CLAUDE.md's "Performance,
        # honestly"), but raising this still needs watching memory/CPU -- each
        # worker loads its own ~1.57GB GlotLID copy. CCIndexGreekReader shards
        # its rows across ranks (see warc_reader.py) so tasks>1 here actually
        # splits the fetch/extract work instead of redoing it per worker.
        tasks=tasks,
        # "fork" (the simpler, faster choice) actually deadlocks here once
        # tasks>1: the CLI touches polars (compute_chunk_bounds) before this
        # executor ever runs, which lazily spins up polars' Rust-side Rayon
        # thread pool in the parent -- fork() only carries the calling thread
        # into each child, so that pool's worker threads don't exist there,
        # and the reader's first pl.collect() (warc_reader.py) hangs forever
        # waiting on threads that will never respond. "spawn" gives each task
        # a genuinely fresh interpreter instead of a fork of a polars-tainted
        # parent, sidestepping the corruption entirely. Confirmed via py-spy:
        # every forked worker's stack was parked inside collect() at
        # warc_reader.py's manifest-slice read, not anywhere near S3.
        start_method="spawn",
        logging_dir=str(chunk_out / "logs"),
    )
    executor.run()

    survivor_files = list(chunk_out.glob("*.parquet"))
    row_count_survivors = (
        pl.scan_parquet(survivor_files).select(pl.len()).collect().item()
        if survivor_files
        else 0
    )

    if publish and os.environ.get("HF_TOKEN"):
        # pre-dedup: OnlineMinhashDedup only runs in stage 2, across the whole
        # crawl, so this chunk can still contain near-duplicates of survivors
        # from other chunks -- kept under raw/ so it's never mistaken for the
        # final crawl=<crawl>/ output stage 2 publishes later
        _publish(chunk_out, f"raw/crawl={crawl}/chunk={offset:012d}")
    elif publish:
        logger.warning("publish=True but HF_TOKEN is not set -- skipping upload")

    return {"chunk_out": str(chunk_out), "row_count_survivors": row_count_survivors}


def run_extraction_stage_2_dedup_and_write(
    crawl: str,
    manifest_path: Path,
    stage_1_dir: Path,
    out_dir: Path,
    publish: bool = False,
) -> dict:
    """Merge every chunk's survivors, dedup once across the whole crawl, write final output."""
    row_count_in = pl.scan_parquet(manifest_path).select(pl.len()).collect().item()
    stage_1_crawl_dir = Path(stage_1_dir) / "_stage_1" / crawl
    crawl_out = Path(out_dir) / crawl

    pipeline = [
        ParquetReader(str(stage_1_crawl_dir), glob_pattern="chunk_*/*.parquet"),
        OnlineMinhashDedup(language=LANGUAGE),
        ParquetWriter(str(crawl_out), compression="zstd", expand_metadata=True),
    ]

    executor = LocalPipelineExecutor(
        pipeline=pipeline, tasks=1, logging_dir=str(crawl_out / "logs")
    )
    executor.run()

    output_files = list(crawl_out.glob("*.parquet"))
    row_count_out = (
        pl.scan_parquet(output_files).select(pl.len()).collect().item()
        if output_files
        else 0
    )

    if publish and os.environ.get("HF_TOKEN"):
        _publish(crawl_out, f"crawl={crawl}")
    elif publish:
        logger.warning("publish=True but HF_TOKEN is not set -- skipping upload")

    # only delete Stage 1's intermediate survivors once the final write above
    # is confirmed -- if this stage itself fails/crashes, a retry can still
    # read them back in without redoing every chunk
    shutil.rmtree(stage_1_crawl_dir, ignore_errors=True)

    return {
        "output_path": str(crawl_out),
        "row_count_in": row_count_in,
        "row_count_out": row_count_out,
    }


def _publish(local_dir: Path, repo_path_prefix: str) -> None:
    repo_id = os.environ.get("HF_DATASET_REPO")
    if not repo_id:
        logger.warning("HF_TOKEN is set but HF_DATASET_REPO is not -- skipping upload")
        return

    api = HfApi()
    api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True, private=True)
    for path in local_dir.glob("*.parquet"):
        api.upload_file(
            path_or_fileobj=str(path),
            path_in_repo=f"{repo_path_prefix}/{path.name}",
            repo_id=repo_id,
            repo_type="dataset",
        )
