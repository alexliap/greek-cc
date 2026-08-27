# Running this deployment on macOS

The stack is entirely containerized, so it runs on a Mac unchanged — no code,
compose, or Dockerfile edits. This documents what to copy, the Mac-specific
settings that need attention, and the one thing that determines whether the
extraction actually finishes in reasonable time.

## Why move at all

Not stability — the crash causes (DNS flood, unbounded read-ahead, scheduler
heartbeat starvation) are fixed and the Pi now runs without crashing. It's
throughput. Measured floor on the Pi was **49.3 docs/min**, which puts one
100k-row chunk at **≤33.8 hours**, and there are **184 chunks** for
CC-MAIN-2024-22 (18.3M rows). That's months.

Note that 49.3/min is a *lower bound*: it counts only documents Trafilatura
rejected (`discarding data` log lines); successful extractions aren't logged.
True throughput is higher, so 33.8h/chunk is a ceiling, not a measurement.
The order of magnitude is what matters — even 3× faster is still ~150 days.

## Architecture note: no changes needed

The image is `linux/arm64`. Apple Silicon runs that **natively** — no emulation,
no rebuild for a different platform. The same `docker-compose.yaml` and
`docker/Dockerfile` work as-is.

(On an Intel Mac it would build `linux/amd64` instead. Also fine, just a slower
first build; nothing to edit.)

## What to copy

Both repos must sit **as siblings**, because the compose file references
`../greek-cc` for DAGs, manifests, and the Postgres init script:

```
projects/
  airflow/     <- this repo
  greek-cc/
```

| What | Where | Size | Notes |
|---|---|---|---|
| `airflow/` repo | git clone | small | |
| `greek-cc/` repo | git clone | small | |
| `airflow/.env` | copy manually | tiny | **gitignored** — won't come with the clone |
| `greek-cc/.env` | copy manually | tiny | **gitignored**; AWS creds for standalone scripts |
| `greek-cc/manifests/*.parquet` | copy manually | **1.2 GB** | gitignored; the crawl manifest |
| Postgres data | see below | 83 MB | optional |

### Postgres: fresh vs. carried over

**Fresh start (simplest).** Just `docker compose up`. `airflow-init` creates the
schema, and `greek_cc_manifest` will re-derive crawl state. You lose DAG run
history, which mostly doesn't matter.

**Carry it over.** Dump on the Pi, restore on the Mac:

```bash
# on the Pi
docker exec -i airflow-postgres-1 pg_dump -U airflow -d airflow   > airflow.sql
docker exec -i airflow-postgres-1 pg_dump -U airflow -d greek_cc  > greek_cc.sql

# on the Mac, after `docker compose up -d postgres`
docker exec -i airflow-postgres-1 psql -U airflow -d airflow  < airflow.sql
docker exec -i airflow-postgres-1 psql -U airflow -d greek_cc < greek_cc.sql
```

Do **not** try to copy the Docker volume directly — dump/restore is the
supported path across machines.

## Mac-specific settings to check

**1. `AIRFLOW_UID` in `.env`.** It's `1000` (Linux). On macOS your UID is
typically `501`. Set it to whatever `id -u` prints:

```bash
sed -i '' "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
```

macOS bind mounts virtualize ownership so this is more forgiving than on Linux,
but matching it avoids permission surprises in `airflow_logs/`.

**2. Docker Desktop resources.** Default VM memory is often 8 GB, and this
workload wants headroom. Settings → Resources: give it **at least 8 GB**, and
**as many CPUs as you can spare** — CPU is the bottleneck (see below).

**3. Enable VirtioFS.** Settings → General → file sharing implementation.
It's markedly faster than the older gRPC-FUSE, and this stack reads a 1.2 GB
parquet over a bind mount.

**4. Ports 8080 and 5432** must be free.

**5. The `dns:` setting is harmless.** It exists because the Pi's
systemd-resolved stub collapsed under concurrent S3 fetches. macOS has no
systemd-resolved, so it's simply unnecessary there — not a problem, no need to
remove it.

## Running it

```bash
cd airflow
docker compose build airflow-init     # ~15 min first time (fasttext + 1.57GB model)
docker compose up -d
```

UI at http://localhost:8080. Unpause `greek_cc_extract` to start extraction.

Subsequent rebuilds after editing `greek-cc/src/` take **~8 seconds** — the
Dockerfile puts the slow steps above `COPY src/` deliberately.

## The thing that actually determines completion time

Everything above gets it *running* identically. But the pipeline is currently
pinned to **one core**:

- `LocalPipelineExecutor(..., tasks=1)` — `greek-cc/src/greek_cc/extract.py`
- `max_active_tis_per_dag=1` — `greek-cc/dags/greek_cc_extract_dag.py`

Both were deliberate guards against the Pi running out of memory when a single
chunk buffered its entire read-ahead. **That constraint no longer applies**: the
reader now uses a bounded sliding window (`warc_reader.py`), so peak memory per
chunk is capped at `max_in_flight` documents regardless of chunk size.

So on a 10–12 core Mac, raising these is what converts "runs correctly" into
"finishes this month". Extraction is CPU-bound, so the win is close to linear
in cores. Combined with faster per-core performance, expect roughly **20–30×**,
i.e. months → **about a week**.

That's a change to greek-cc, not to this deployment, and it's optional — the
stack runs correctly either way. Worth doing as a follow-up once the move is
confirmed working, raising concurrency gradually while watching memory.
