"""On-demand extraction: run one crawl through the pipeline, outside Airflow.

Downloads that crawl's manifest from the HF Hub manifests repo
(HF_MANIFEST_REPO, see index.download_manifest) and runs it through the same
stage-1/stage-2 pipeline extract.py has always run -- just driven by a plain
loop instead of Airflow's dynamic task mapping, and triggered by hand instead
of a schedule/claim. No Postgres involved: progress is tracked by what
already exists on disk (stage-1's per-chunk completions markers, and the
final output directory), not by extract_status.
"""

import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from greek_cc import extract, index

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Run Greek text extraction for one crawl, on demand."
    )
    parser.add_argument("crawl", help="Crawl id, e.g. CC-MAIN-2024-22")
    parser.add_argument("--manifests-dir", type=Path, default=Path("manifests"))
    parser.add_argument("--out-dir", type=Path, default=Path("extractions"))
    parser.add_argument("--chunk-size", type=int, default=100_000)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess even if this crawl's output already exists",
    )
    parser.add_argument(
        "--tasks",
        type=int,
        default=max(1, (os.cpu_count() or 1) - 2),
        help=(
            "Parallel worker processes for stage 1, and thus concurrent S3 "
            "connections (default: cpu_count - 2, leaving headroom for the "
            "OS). Each worker loads its own copy of the ~1.57GB GlotLID model "
            "-- watch memory if you raise this on a constrained machine. "
            "Stage 2 dedup always runs single-task: it needs a whole-crawl "
            "view to catch cross-chunk near-duplicates."
        ),
    )
    args = parser.parse_args()

    crawl_out = args.out_dir / args.crawl
    if not args.force and list(crawl_out.glob("*.parquet")):
        logger.info("%s: output already exists at %s -- nothing to do (pass --force to redo)", args.crawl, crawl_out)
        return

    manifest_path = args.manifests_dir / f"{args.crawl}.parquet"
    if not manifest_path.exists():
        manifest_path = index.download_manifest(args.crawl, args.manifests_dir)

    chunks = extract.compute_chunk_bounds(manifest_path, args.chunk_size)
    logger.info("%s: %d chunk(s) of up to %d rows", args.crawl, len(chunks), args.chunk_size)

    for i, chunk in enumerate(chunks, 1):
        logger.info(
            "%s: stage 1 chunk %d/%d (offset=%d length=%d)",
            args.crawl, i, len(chunks), chunk["offset"], chunk["length"],
        )
        extract.run_extraction_stage_1_chunk(
            args.crawl, manifest_path, args.out_dir, chunk["offset"], chunk["length"],
            tasks=args.tasks, publish=True,
        )

    logger.info("%s: stage 2 dedup + finalize", args.crawl)
    result = extract.run_extraction_stage_2_dedup_and_write(
        args.crawl, manifest_path, args.out_dir, args.out_dir, publish=True
    )
    logger.info(
        "%s: done -- %d -> %d rows, output at %s",
        args.crawl, result["row_count_in"], result["row_count_out"], result["output_path"],
    )


if __name__ == "__main__":
    main()
