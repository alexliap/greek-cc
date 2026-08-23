"""Build Greek page manifests from Common Crawl's columnar index.

Each run claims a batch of pending index part-files from the `part_status`
queue and fetches them concurrently (dynamic task mapping), then merges the
crawl once its last part lands. Unlike the free HTTPS mirror, signed S3 access
isn't throttled, so concurrency is a straightforward win here rather than
something that needs to be rate-limited away. Seeding the queue is automatic,
so once unpaused this works through every crawl in `CRAWLS` unattended.
"""

from datetime import timedelta
from pathlib import Path

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, Param, get_current_context, task

from greek_cc import db
from greek_cc.crawls import CRAWLS
from greek_cc.index import fetch_part_manifest, merge_crawl_manifest, warc_parquet_urls

OUT_DIR = Path("/opt/airflow/manifests")

# a part returns to the queue on error and is retried by a later run; after this
# many attempts it is marked failed and the crawl moves on without it
MAX_PART_ATTEMPTS = 5


def _conn():
    return PostgresHook(postgres_conn_id="greek_cc_db").get_conn()


with DAG(
    dag_id="greek_cc_manifest",
    # one run = one part-file fetch, so this interval *is* the request rate.
    # a timedelta (not a cron string) so the spacing is a true fixed 40min,
    # not the uneven :00/:40 alternation a "*/40 * * * *" cron would give
    schedule=timedelta(minutes=20),
    catchup=False,
    max_active_runs=1,
    params={
        "min_success_ratio": Param(0.98, type="number", minimum=0.0, maximum=1.0),
        # how many parts one run fetches concurrently via dynamic task mapping
        "batch_size": Param(1, type="integer", minimum=1, maximum=50),
    },
    tags=["greek-cc"],
) as dag:

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=10),
    )
    def claim_work() -> list[dict]:
        """Take a batch of pending parts, seeding the next crawl if the queue is empty.

        Returns an empty list (rather than raising AirflowSkipException) when
        there's truly nothing left, so `fetch.expand()` over it cleanly produces
        zero mapped instances instead of forcing a skip-cascade through the
        merge step.
        """
        batch_size = get_current_context()["params"]["batch_size"]
        conn = _conn()
        try:
            parts = db.claim_next_parts(conn, batch_size)
            if parts:
                return parts

            seeded = db.seeded_crawl_ids(conn)
            remaining = [crawl for crawl in CRAWLS if crawl not in seeded]
            if not remaining:
                return []

            crawl = remaining[0]
            urls = warc_parquet_urls(crawl)
            db.upsert_crawl_resolving(conn, crawl, len(urls))
            db.register_pending_parts(conn, crawl, list(zip(urls, range(len(urls)))))

            return db.claim_next_parts(conn, batch_size)
        finally:
            conn.close()

    # retries are handled by requeueing in the DB, not by Airflow, so that a
    # retry waits for the next run instead of retrying immediately
    @task(retries=0, execution_timeout=timedelta(minutes=40))
    def fetch(part: dict) -> dict:
        conn = _conn()
        try:
            try:
                fragment_path, row_count = fetch_part_manifest(
                    part["crawl_id"], part["part_url"], part["part_index"], OUT_DIR
                )
            except BaseException as exc:
                # AirflowTaskTimeout subclasses BaseException, not Exception (like
                # KeyboardInterrupt), specifically so it isn't swallowed by broad
                # except clauses. We still need to release the part back to the
                # queue on a timeout, so we catch it here too but always re-raise.
                db.mark_part_error(
                    conn,
                    part["crawl_id"],
                    part["part_url"],
                    str(exc)[:2000],
                    MAX_PART_ATTEMPTS,
                )
                raise
            db.mark_part_success(
                conn,
                part["crawl_id"],
                part["part_url"],
                str(fragment_path),
                row_count,
            )
        finally:
            conn.close()
        return part

    # all_done (not the default all_success) so this still runs and evaluates the
    # ratio even when some of this run's mapped `fetch` instances failed. It takes
    # the claimed batch directly rather than fetch's mapped output, since pulling
    # XCom from a mix of succeeded/failed mapped instances is its own headache and
    # the crawl id is all this task actually needs from upstream.
    @task(trigger_rule="all_done")
    def merge_if_complete(parts: list[dict]) -> None:
        """Merge the crawl once no part of it is outstanding."""
        if not parts:
            raise AirflowSkipException("no parts claimed this run")
        crawl = parts[0]["crawl_id"]
        min_ratio = get_current_context()["params"]["min_success_ratio"]

        conn = _conn()
        try:
            counts = db.get_part_status_counts(conn, crawl)
            if counts.get("pending", 0) or counts.get("running", 0):
                raise AirflowSkipException(f"{crawl} still has parts outstanding")

            total = sum(counts.values())
            success = counts.get("success", 0)
            ratio = success / total if total else 0.0

            if ratio < min_ratio:
                message = (
                    f"only {success}/{total} parts succeeded "
                    f"({ratio:.1%} < {min_ratio:.1%})"
                )
                db.mark_crawl_failed(
                    conn, crawl, message, success, total - success, min_ratio
                )
                raise RuntimeError(f"[{crawl}] {message}; not merging.")

            db.mark_crawl_merging(conn, crawl)
            fragments = [Path(p) for p in db.get_successful_fragments(conn, crawl)]
        finally:
            conn.close()

        row_count = merge_crawl_manifest(crawl, fragments, OUT_DIR)

        conn = _conn()
        try:
            db.mark_crawl_done(
                conn,
                crawl,
                str(OUT_DIR / f"{crawl}.parquet"),
                row_count,
                success,
                total - success,
                min_ratio,
            )
        finally:
            conn.close()

    claimed = claim_work()
    fetched = fetch.expand(part=claimed)
    merged = merge_if_complete(claimed)
    fetched >> merged
