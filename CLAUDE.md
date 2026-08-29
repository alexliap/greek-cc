# greek-cc — orientation for agents

Building a FineWeb-style Greek text dataset out of Common Crawl, on a Raspberry
Pi 5 and a Mac. Airflow 3 (LocalExecutor) + Postgres, in Docker Compose.

## Read these first, in this order

1. **`README.md`** — what the two DAGs are and how to run the stack.
2. **The DAG docstrings** — `dags/greek_cc_manifest_dag.py` and
   `dags/greek_cc_extract_dag.py`. These are the real architecture docs; they
   explain the task graph and, more importantly, *why* it is shaped that way.
3. **`docs/running-on-macos.md`** — the two-machine split, and the throughput
   analysis that motivated it.
4. **`plan_of_action.md`** — the pre-implementation design doc. §§1–5 are still
   authoritative for *why* the pipeline works the way it does. **§§6 and 9 are
   superseded** and carry warning banners; do not plan against them.

## Shape of the thing

```
greek_cc_manifest  ──> crawl_status.status = 'done' ──> greek_cc_extract
   (network-bound)         (Postgres: greek_cc)          (CPU-bound)
   scans the columnar                                     range-fetches WARC
   index, writes a                                        records, Trafilatura,
   Parquet manifest                                       filters, MinHash dedup
```

The two DAGs have **no Airflow-level dependency** — no sensor, no dataset. They
coordinate only through the `greek_cc` application database. That is deliberate:
it is what lets them run on different machines (manifests on the Pi, extraction
on a Mac).

Two databases on one Postgres server, and the distinction matters:

- **`greek_cc`** — application state. `crawl_status`, `part_status`,
  `extract_status`. Schema in `src/greek_cc/db.py`. **Shared** across machines.
- **`airflow`** (and `airflow_mac`) — Airflow's own metadata. **Never shared**
  between machines; see the warning in `docs/running-on-macos.md`.

Paths inside the containers: manifests at `/opt/airflow/manifests/<crawl>.parquet`,
all extraction output under `/opt/airflow/manifests/extractions/`.

## Traps

**Resetting `extract_status` to `'pending'` makes a crawl unclaimable.**
`claim_ready_crawl_for_extraction` matches only:

```sql
WHERE c.status = 'done' AND (e.crawl_id IS NULL OR e.status = 'failed')
```

`'pending'` is in neither branch. The DAG then skips silently, forever, with no
error anywhere. To retry a crawl, set `status='failed'` (or delete the row).
This cost a full run once.

**The comments in `docker-compose.yaml` and `docker/Dockerfile` are load-bearing.**
Each one records a production failure that took the Pi down. Removing the setting
brings the failure back. Specifically: the `dns:` override (systemd-resolved stub
collapsing under concurrent S3 fetches), `SCHEDULER_HEALTH_CHECK_THRESHOLD: 300`
(scheduler starved by its own LocalExecutor subprocess, then resetting live tasks
as orphaned), and `chmod -R g+rwX /home/airflow/.cache` (build runs as uid 50000,
runtime as the host uid).

**Don't reorder the Dockerfile.** Slow steps — the fasttext compile and the
1.57 GB GlotLID download — sit deliberately *above* `COPY src/`. Editing source
rebuilds in ~8 seconds; move that COPY up and it becomes ~15 minutes. Any new
slow step goes above the line too.

**`.dockerignore` is an allow-list.** The build context is the repo root, which
holds ~2.8 GB of manifests and a ~950 MB venv. A new path the Dockerfile needs
to `COPY` must be explicitly un-ignored.

**`postgres-db-volume` is pinned by name** in `docker-compose.yaml`. Renaming it,
or renaming the repo directory without it, makes Compose silently start from an
empty database — losing all crawl state and DAG history.

## Runbook

```bash
docker compose up -d                 # start
docker compose build airflow-init    # rebuild the image
docker compose down                  # stop, keeps the data volume
docker compose logs -f airflow-scheduler
```

UI at http://localhost:8080 (credentials in `.env`).

Progress:

```bash
# where each crawl stands
docker exec greek-cc-postgres-1 psql -U airflow -d greek_cc \
  -c "select crawl_id, status, updated_at from crawl_status order by crawl_id"
docker exec greek-cc-postgres-1 psql -U airflow -d greek_cc \
  -c "select crawl_id, status, last_error from extract_status"

# manifest part queue for a crawl in flight
docker exec greek-cc-postgres-1 psql -U airflow -d greek_cc \
  -c "select status, count(*) from part_status group by status"
```

Extraction throughput is not logged directly. Successful extractions produce no
log line; only rejects do (`discarding data`). Counting those gives a **lower
bound** on docs/min — useful, but never quote it as a measurement.

## Performance, honestly

Measured floor on the Pi: **49.3 docs/min**, i.e. one 100k-row chunk in ≤33.8 h,
184 chunks per crawl ⇒ roughly **8 months**. `plan_of_action.md` §6.1 estimated
~100 docs/s and was wrong by about 100×; the banner there explains why.

Extraction is pinned to one core by `tasks=1` (`src/greek_cc/extract.py`) and
`max_active_tis_per_dag=1` (`dags/greek_cc_extract_dag.py`). Both were memory
guards, and the constraint they guarded against is gone — `warc_reader.py` now
bounds read-ahead with a sliding window. Raising them on a multi-core Mac is the
single change that takes this from months to about a week. Keep them at 1 on the
Pi.

## Conventions

- Comments explain **why**, not what — especially where a value looks arbitrary.
  Most non-obvious constants here were paid for with a crash.
- Secrets live in `.env` (gitignored). `.env.example` documents every key.
- The Airflow deployment lives in this repo. It was briefly split into a sibling
  `airflow/` repo and merged back on 27 Aug 2026; ignore any lingering reference
  to `../greek-cc` or a sibling directory.
