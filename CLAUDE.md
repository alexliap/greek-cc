# greek-cc — orientation for agents

Building a FineWeb-style Greek text dataset out of Common Crawl, on a Raspberry
Pi 5 and a Mac. The Pi runs Airflow 3 (LocalExecutor) + Postgres in Docker
Compose for manifest building; extraction runs as a plain CLI, anywhere.

## Read these first, in this order

1. **`README.md`** — what the manifest DAG and the extraction CLI are and how
   to run each.
2. **`dags/greek_cc_manifest_dag.py`'s docstring** and **`src/greek_cc/cli.py`'s
   module docstring**. These are the real architecture docs; they explain the
   task graph / pipeline flow and, more importantly, *why* each is shaped that
   way.
3. **`plan_of_action.md`** — the pre-implementation design doc. §§1–5 are still
   authoritative for *why* the pipeline works the way it does. **§§6 and 9 are
   superseded** and carry warning banners; do not plan against them.

## Shape of the thing

```
greek_cc_manifest (Airflow DAG)         greek-cc-extract (CLI, on demand)
   (network-bound)                          (CPU-bound)
   scans the columnar index,                downloads a crawl's manifest,
   writes a Parquet manifest,               range-fetches WARC records,
   uploads it to HF Hub          ──HF Hub──>   Trafilatura, filters,
   (alexliap/greek-cc-manifests)            MinHash dedup
```

Only `greek_cc_manifest` runs under Airflow. Extraction is a plain local
command (`src/greek_cc/cli.py`, installed as `greek-cc-extract`) — no
Airflow, no Docker, no Postgres. The two coordinate only by manifests
landing on HF Hub, not through any database or Airflow-level dependency
(no sensor, no dataset). That is deliberate: it is what lets extraction run
anywhere (a Mac, say) with nothing but this repo's Python environment.

- **`greek_cc`** (Postgres) — application state for the manifest side only:
  `crawl_status`, `part_status`. Schema in `src/greek_cc/db.py`.
- **`airflow`** — Airflow's own metadata, for the one machine running
  `greek_cc_manifest` (the Pi).
Manifests land at `manifests/<crawl>.parquet` (inside the Airflow container
for the manifest DAG; in the extraction machine's working directory for the
CLI). Extraction output lands under `extractions/`.

## Traps

**The CLI skips a crawl whose output already exists — pass `--force` to redo.**
`greek-cc-extract <crawl>` checks `extractions/<crawl>/*.parquet` first and
exits immediately if it's already there. Re-running with `--force` reprocesses
every chunk from scratch, not just the final dedup step: stage 2 deletes
stage 1's per-chunk intermediates once it succeeds, so there's nothing partial
to resume from at that point.

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

Progress (manifest side — Postgres):

```bash
# where each crawl stands
docker exec greek-cc-postgres-1 psql -U airflow -d greek_cc \
  -c "select crawl_id, status, updated_at from crawl_status order by crawl_id"

# manifest part queue for a crawl in flight
docker exec greek-cc-postgres-1 psql -U airflow -d greek_cc \
  -c "select status, count(*) from part_status group by status"
```

(`extract_status` still exists in that database from before extraction moved
off Airflow/Postgres — it's orphaned, nothing writes to it anymore, don't
trust it for current state.)

Extraction progress is now just what's on disk on whichever machine is running
`greek-cc-extract`: `extractions/_stage_1/<crawl>/chunk_*/` for in-flight
chunks, `extractions/<crawl>/*.parquet` once a crawl is fully done.

Extraction throughput is not logged directly. Successful extractions produce no
log line; only rejects do (`discarding data`). Counting those gives a **lower
bound** on docs/min — useful, but never quote it as a measurement.

## Performance, honestly

Measured floor on the Pi: **49.3 docs/min**, i.e. one 100k-row chunk in ≤33.8 h,
184 chunks per crawl ⇒ roughly **8 months**. `plan_of_action.md` §6.1 estimated
~100 docs/s and was wrong by about 100×; the banner there explains why.

Extraction is pinned to one core by `tasks=1` in `src/greek_cc/extract.py`.
That was a memory guard, and the constraint it guarded against is gone —
`warc_reader.py` now bounds read-ahead with a sliding window. Raising it on a
multi-core machine is the single change that takes this from months to about
a week.

## Conventions

- Comments explain **why**, not what — especially where a value looks arbitrary.
  Most non-obvious constants here were paid for with a crash.
- Secrets live in `.env` (gitignored). `.env.example` documents every key.
- The Airflow deployment lives in this repo. It was briefly split into a sibling
  `airflow/` repo and merged back on 27 Aug 2026; ignore any lingering reference
  to `../greek-cc` or a sibling directory.
