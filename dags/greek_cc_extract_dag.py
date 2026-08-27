"""Extract filtered/deduped Greek text from finished crawl manifests.

Every 3 days, checks for a crawl whose manifest has finished merging
(crawl_status.status='done') and hasn't been extracted yet, and if so runs it
through the full plan_of_action.md §5 pipeline (range-fetch -> Trafilatura ->
quality filters -> per-crawl MinHash dedup -> Parquet) over that crawl's
*entire* manifest -- tens of millions of rows once a crawl finishes merging.

Split into two stages (src/greek_cc/extract.py) because pointing the reader
at an entire multi-million-row manifest in one task OOM-crashed the Pi twice.
plan_chunks() splits the manifest into bounded row chunks; stage_1_extract is
dynamically mapped once per chunk (mirrors greek_cc_manifest_dag's
claim_work -> fetch.expand -> merge_if_complete shape) and runs everything up
to the per-crawl dedup, so a chunk's failure/timeout only costs that chunk,
not the whole crawl; stage_2_finalize runs once, after every chunk is done,
merging survivors, deduping, and writing the final output. Chunks run
sequentially (max_active_tis_per_dag=1) -- parallel chunks would multiply
peak memory on the same Pi that already OOM'd, for no throughput win since
extraction is CPU-bound, not I/O-bound.

max_active_runs=1 keeps a later 3-day tick from starting a second run on top
of one still going. Most runs are expected to skip: today no crawl has
finished merging yet.
"""

from datetime import timedelta
from pathlib import Path

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, Param, get_current_context, task

from greek_cc import db
from greek_cc.extract import (
    compute_chunk_bounds,
    run_extraction_stage_1_chunk,
    run_extraction_stage_2_dedup_and_write,
)

MANIFESTS_DIR = Path("/opt/airflow/manifests")
EXTRACTIONS_DIR = Path("/opt/airflow/manifests/extractions")


def _conn():
    return PostgresHook(postgres_conn_id="greek_cc_db").get_conn()


with DAG(
    dag_id="greek_cc_extract",
    schedule=timedelta(days=5),
    catchup=False,
    max_active_runs=1,
    params={
        "chunk_size": Param(100_000, type="integer", minimum=1),
    },
    tags=["greek-cc"],
) as dag:

    @task(
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=10),
    )
    def claim_crawl() -> str | None:
        conn = _conn()
        try:
            claimed = db.claim_ready_crawl_for_extraction(conn)
        finally:
            conn.close()

        if not claimed:
            raise AirflowSkipException("no finished-but-unextracted crawl right now")
        return claimed["crawl_id"]

    @task(retries=1, execution_timeout=timedelta(minutes=15))
    def plan_chunks(crawl_id: str) -> list[dict]:
        chunk_size = get_current_context()["params"]["chunk_size"]
        manifest_path = MANIFESTS_DIR / f"{crawl_id}.parquet"
        return [
            {"crawl_id": crawl_id, "offset": bound["offset"], "length": bound["length"]}
            for bound in compute_chunk_bounds(manifest_path, chunk_size)
        ]

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=6),
        max_active_tis_per_dag=1,
    )
    def stage_1_extract(chunk: dict) -> dict:
        manifest_path = MANIFESTS_DIR / f"{chunk['crawl_id']}.parquet"
        return run_extraction_stage_1_chunk(
            chunk["crawl_id"],
            manifest_path,
            EXTRACTIONS_DIR,
            chunk["offset"],
            chunk["length"],
        )

    @task(trigger_rule="all_done", retries=0, execution_timeout=timedelta(hours=12))
    def stage_2_finalize(crawl_id: str, stage_1_results: list[dict]) -> None:
        manifest_path = MANIFESTS_DIR / f"{crawl_id}.parquet"

        conn = _conn()
        try:
            try:
                result = run_extraction_stage_2_dedup_and_write(
                    crawl_id, manifest_path, EXTRACTIONS_DIR, EXTRACTIONS_DIR
                )
            except BaseException as exc:
                # same rationale as fetch() in greek_cc_manifest_dag: catch
                # BaseException (AirflowTaskTimeout subclasses it directly,
                # not Exception) so a timeout still records last_error, then
                # re-raise so Airflow still marks the task failed
                db.mark_extraction_failed(conn, crawl_id, str(exc)[:2000])
                raise
            db.mark_extraction_done(
                conn,
                crawl_id,
                result["output_path"],
                result["row_count_in"],
                result["row_count_out"],
            )
        finally:
            conn.close()

    crawl = claim_crawl()
    chunks = plan_chunks(crawl)
    results = stage_1_extract.expand(chunk=chunks)
    stage_2_finalize(crawl, results)
