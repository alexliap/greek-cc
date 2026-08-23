"""Extract filtered/deduped Greek text from finished crawl manifests.

Every 3 days, checks for a crawl whose manifest has finished merging
(crawl_status.status='done') and hasn't been extracted yet, and if so runs it
through the full plan_of_action.md §5 pipeline (range-fetch -> Trafilatura ->
quality filters -> per-crawl MinHash dedup -> Parquet) over that crawl's
*entire* manifest -- tens of millions of rows once a crawl finishes merging,
so a single extract() run can take days; execution_timeout is set long
accordingly, and max_active_runs=1 keeps a later 3-day tick from starting a
second run on top of one still going. Most runs are expected to skip: today
no crawl has finished merging yet.
"""

from datetime import timedelta
from pathlib import Path

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, task

from greek_cc import db
from greek_cc.extract import run_extraction_pipeline

MANIFESTS_DIR = Path("/opt/airflow/manifests")
EXTRACTIONS_DIR = Path("/opt/airflow/manifests/extractions")


def _conn():
    return PostgresHook(postgres_conn_id="greek_cc_db").get_conn()


with DAG(
    dag_id="greek_cc_extract",
    schedule=timedelta(days=3),
    catchup=False,
    max_active_runs=1,
    tags=["greek-cc"],
) as dag:

    @task(retries=1, retry_delay=timedelta(minutes=5), execution_timeout=timedelta(minutes=10))
    def claim_crawl() -> str | None:
        conn = _conn()
        try:
            claimed = db.claim_ready_crawl_for_extraction(conn)
        finally:
            conn.close()

        if not claimed:
            raise AirflowSkipException("no finished-but-unextracted crawl right now")
        return claimed["crawl_id"]

    @task(retries=0, execution_timeout=timedelta(days=21))
    def extract(crawl_id: str) -> None:
        manifest_path = MANIFESTS_DIR / f"{crawl_id}.parquet"

        conn = _conn()
        try:
            try:
                result = run_extraction_pipeline(crawl_id, manifest_path, EXTRACTIONS_DIR)
            except BaseException as exc:
                # same rationale as fetch() in greek_cc_manifest_dag: catch
                # BaseException (AirflowTaskTimeout subclasses it directly,
                # not Exception) so a timeout still records last_error, then
                # re-raise so Airflow still marks the task failed
                db.mark_extraction_failed(conn, crawl_id, str(exc)[:2000])
                raise
            db.mark_extraction_done(
                conn, crawl_id, result["output_path"], result["row_count_in"], result["row_count_out"]
            )
        finally:
            conn.close()

    extract(claim_crawl())
