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

Runs a crawl's *entire* manifest (tens of millions of rows once a crawl
finishes merging) in one pass -- see dags/greek_cc_extract_dag.py's
execution_timeout, which is set long accordingly.
"""

import logging
import os
from functools import partial
from pathlib import Path

import polars as pl
import yaml
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.extractors import Trafilatura
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
from datatrove.pipeline.writers.parquet import ParquetWriter

from greek_cc.dedup import OnlineMinhashDedup
from greek_cc.warc_reader import CCIndexGreekReader

logger = logging.getLogger(__name__)

LANGUAGE = "ell_Grek"
CONFIG_PATH = Path(__file__).parent / "configs" / "ell_Grek.yml"


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


def run_extraction_pipeline(
    crawl: str,
    manifest_path: Path,
    out_dir: Path,
    publish: bool = False,
) -> dict:
    cfg = _load_filter_config()
    crawl_out = Path(out_dir) / crawl
    row_count_in = pl.scan_parquet(manifest_path).select(pl.len()).collect().item()

    pipeline = [
        CCIndexGreekReader(crawl, manifest_path),
        URLFilter(),
        Trafilatura(favour_precision=True),
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
        _publish(crawl_out, crawl)
    elif publish:
        logger.warning("publish=True but HF_TOKEN is not set -- skipping upload")

    return {
        "output_path": str(crawl_out),
        "row_count_in": row_count_in,
        "row_count_out": row_count_out,
    }


def _publish(crawl_out: Path, crawl: str) -> None:
    from huggingface_hub import HfApi

    repo_id = os.environ.get("HF_DATASET_REPO")
    if not repo_id:
        logger.warning("HF_TOKEN is set but HF_DATASET_REPO is not -- skipping upload")
        return

    api = HfApi()
    for path in crawl_out.glob("*.parquet"):
        api.upload_file(
            path_or_fileobj=str(path),
            path_in_repo=f"crawl={crawl}/{path.name}",
            repo_id=repo_id,
            repo_type="dataset",
        )
