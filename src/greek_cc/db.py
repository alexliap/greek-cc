"""Work queue and status tracking for crawl/part manifest builds.

Every function takes a DB-API 2.0 connection as its first argument and uses only
standard cursor/execute calls, so this module needs no database driver import
and stays testable without an Airflow runtime.

`part_status` doubles as the work queue: one DAG run claims one pending part,
which is what paces fetching against Common Crawl's rate limit.

Page-row data never lands here — only bookkeeping. The manifests themselves are
Parquet on disk (plan_of_action.md §5.6), referenced by path from these rows.
"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS crawl_status (
    crawl_id      TEXT PRIMARY KEY,
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending','fetching','merging','done','failed')),
    total_parts   INTEGER,
    success_parts INTEGER,
    failed_parts  INTEGER,
    min_success_ratio_used DOUBLE PRECISION,
    output_path   TEXT,
    row_count     BIGINT,
    last_error    TEXT,
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS part_status (
    id            BIGSERIAL PRIMARY KEY,
    crawl_id      TEXT NOT NULL REFERENCES crawl_status(crawl_id) ON DELETE CASCADE,
    part_url      TEXT NOT NULL,
    part_index    INTEGER NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending','running','success','failed')),
    fragment_path TEXT,
    row_count     BIGINT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (crawl_id, part_url)
);

CREATE INDEX IF NOT EXISTS ix_part_status_crawl_status ON part_status (crawl_id, status);
CREATE INDEX IF NOT EXISTS ix_part_status_queue ON part_status (status, crawl_id, part_index);

CREATE TABLE IF NOT EXISTS extract_status (
    crawl_id      TEXT PRIMARY KEY REFERENCES crawl_status(crawl_id) ON DELETE CASCADE,
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending','running','done','failed')),
    sample_size   INTEGER,
    row_count_in  BIGINT,
    row_count_out BIGINT,
    output_path   TEXT,
    last_error    TEXT,
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA)
    conn.commit()


def seeded_crawl_ids(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT crawl_id FROM crawl_status")
        return {row[0] for row in cur.fetchall()}


def upsert_crawl_resolving(conn, crawl_id: str, total_parts: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO crawl_status (crawl_id, status, total_parts, started_at, updated_at)
            VALUES (%s, 'fetching', %s, now(), now())
            ON CONFLICT (crawl_id) DO UPDATE
            SET status = 'fetching',
                total_parts = EXCLUDED.total_parts,
                started_at = COALESCE(crawl_status.started_at, now()),
                updated_at = now()
            """,
            (crawl_id, total_parts),
        )
    conn.commit()


def register_pending_parts(conn, crawl_id: str, parts: list[tuple[str, int]]) -> None:
    """Register (part_url, part_index) rows, leaving any existing row untouched.

    The DO NOTHING is what makes reseeding safe: parts that already succeeded
    keep their status instead of being reset to pending.
    """
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO part_status (crawl_id, part_url, part_index)
            VALUES (%s, %s, %s)
            ON CONFLICT (crawl_id, part_url) DO NOTHING
            """,
            [(crawl_id, url, index) for url, index in parts],
        )
    conn.commit()


def claim_next_parts(conn, limit: int) -> list[dict]:
    """Atomically take up to `limit` oldest pending parts and mark them running.

    Crawl ids sort lexicographically in chronological order (fixed-width
    YYYY-WW), so this drains crawls oldest-first without an explicit ordering
    table. Returns an empty list when no work is pending.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE part_status
            SET status = 'running', attempt_count = attempt_count + 1,
                started_at = now(), updated_at = now()
            WHERE id IN (
                SELECT id FROM part_status
                WHERE status = 'pending'
                ORDER BY crawl_id, part_index
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING crawl_id, part_url, part_index, attempt_count
            """,
            (limit,),
        )
        rows = cur.fetchall()
    conn.commit()

    return [
        {"crawl_id": row[0], "part_url": row[1], "part_index": row[2], "attempt_count": row[3]}
        for row in rows
    ]


def mark_part_success(
    conn, crawl_id: str, part_url: str, fragment_path: str, row_count: int
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE part_status
            SET status = 'success', fragment_path = %s, row_count = %s,
                last_error = NULL, finished_at = now(), updated_at = now()
            WHERE crawl_id = %s AND part_url = %s
            """,
            (fragment_path, row_count, crawl_id, part_url),
        )
    conn.commit()


def mark_part_error(
    conn, crawl_id: str, part_url: str, error: str, max_attempts: int
) -> None:
    """Return the part to the queue, or give up on it once attempts run out.

    Going back to 'pending' is the retry mechanism: a later run re-claims it,
    which spaces retries by the DAG's schedule instead of hammering the endpoint.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE part_status
            SET status = CASE WHEN attempt_count >= %s THEN 'failed' ELSE 'pending' END,
                last_error = %s, finished_at = now(), updated_at = now()
            WHERE crawl_id = %s AND part_url = %s
            """,
            (max_attempts, error, crawl_id, part_url),
        )
    conn.commit()


def get_part_status_counts(conn, crawl_id: str) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, count(*) FROM part_status WHERE crawl_id = %s GROUP BY status",
            (crawl_id,),
        )
        return {status: count for status, count in cur.fetchall()}


def get_successful_fragments(conn, crawl_id: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fragment_path FROM part_status
            WHERE crawl_id = %s AND status = 'success' AND fragment_path IS NOT NULL
            ORDER BY part_index
            """,
            (crawl_id,),
        )
        return [row[0] for row in cur.fetchall()]


def mark_crawl_merging(conn, crawl_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE crawl_status SET status = 'merging', updated_at = now() WHERE crawl_id = %s",
            (crawl_id,),
        )
    conn.commit()


def mark_crawl_done(
    conn,
    crawl_id: str,
    output_path: str,
    row_count: int,
    success_parts: int,
    failed_parts: int,
    min_success_ratio: float,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE crawl_status
            SET status = 'done', output_path = %s, row_count = %s,
                success_parts = %s, failed_parts = %s, min_success_ratio_used = %s,
                last_error = NULL, finished_at = now(), updated_at = now()
            WHERE crawl_id = %s
            """,
            (
                output_path,
                row_count,
                success_parts,
                failed_parts,
                min_success_ratio,
                crawl_id,
            ),
        )
    conn.commit()


def mark_crawl_failed(
    conn,
    crawl_id: str,
    error: str,
    success_parts: int,
    failed_parts: int,
    min_success_ratio: float,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE crawl_status
            SET status = 'failed', last_error = %s,
                success_parts = %s, failed_parts = %s, min_success_ratio_used = %s,
                finished_at = now(), updated_at = now()
            WHERE crawl_id = %s
            """,
            (error, success_parts, failed_parts, min_success_ratio, crawl_id),
        )
    conn.commit()


def claim_ready_crawl_for_extraction(conn) -> dict | None:
    """Atomically claim the oldest finished crawl not yet extracted (or retry-eligible).

    A crawl qualifies once its manifest is done and it either has no
    extract_status row yet, or its previous extraction attempt failed. Same
    FOR UPDATE SKIP LOCKED pattern as claim_next_parts, so two concurrent
    callers can't claim the same crawl. Returns None when nothing qualifies.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO extract_status (crawl_id, status, started_at, updated_at)
            SELECT c.crawl_id, 'running', now(), now()
            FROM crawl_status c
            LEFT JOIN extract_status e ON e.crawl_id = c.crawl_id
            WHERE c.status = 'done' AND (e.crawl_id IS NULL OR e.status = 'failed')
            ORDER BY c.crawl_id
            LIMIT 1
            FOR UPDATE OF c SKIP LOCKED
            ON CONFLICT (crawl_id) DO UPDATE
            SET status = 'running', last_error = NULL, started_at = now(), updated_at = now()
            RETURNING crawl_id
            """
        )
        row = cur.fetchone()
    conn.commit()

    return {"crawl_id": row[0]} if row else None


def mark_extraction_done(
    conn, crawl_id: str, output_path: str, row_count_in: int, row_count_out: int
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE extract_status
            SET status = 'done', output_path = %s, row_count_in = %s, row_count_out = %s,
                last_error = NULL, finished_at = now(), updated_at = now()
            WHERE crawl_id = %s
            """,
            (output_path, row_count_in, row_count_out, crawl_id),
        )
    conn.commit()


def mark_extraction_failed(conn, crawl_id: str, error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE extract_status
            SET status = 'failed', last_error = %s, finished_at = now(), updated_at = now()
            WHERE crawl_id = %s
            """,
            (error, crawl_id),
        )
    conn.commit()
