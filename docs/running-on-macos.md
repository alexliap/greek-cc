# Splitting the pipeline: manifests on the Pi, extraction on a Mac

The Pi keeps doing what it's good at — building manifests, which is rate-limited
by Common Crawl rather than CPU — and hosts Postgres. The Mac takes the
CPU-bound text extraction. No pipeline code changes; the difference is `.env`.

```
  Pi  192.168.1.18                          Mac
  ├── Postgres :5432                        Airflow #2  (metadata: airflow_mac)
  │   ├── airflow      (Pi's Airflow)          └── greek_cc_extract
  │   ├── airflow_mac  (Mac's Airflow)  <───────────┘
  │   └── greek_cc     (SHARED app state) <─────────┘
  ├── Airflow #1 (metadata: airflow)
  │   └── greek_cc_manifest
  └── manifests/  ──────────────────────>  read over NFS (or copied)
```

## Why this shape

The two DAGs have **no Airflow-level dependency** — no `ExternalTaskSensor`, no
datasets. They coordinate entirely through the `greek_cc` application database:
`greek_cc_manifest` marks `crawl_status.status='done'`, and
`claim_ready_crawl_for_extraction` picks that up on the other side. That's what
makes splitting them across machines clean.

## The one thing that would break it

**Do not point both machines at the same Airflow metadata database.** Airflow
schedulers are homogeneous: any scheduler can run any DAG it can see. Sharing
one metadata DB means the Pi could pick up extraction tasks and the Mac could
pick up manifest tasks — exactly the opposite of the split. There is no way to
pin a DAG to a machine with LocalExecutor.

So each machine gets its own metadata database. Both can live on the Pi's
Postgres server; they're just different databases. `airflow_mac` already exists.

## Why move extraction at all

Not stability — the crash causes (DNS flood, unbounded read-ahead, scheduler
heartbeat starvation) are fixed and the Pi runs without crashing now. It's
throughput. Measured floor was **49.3 docs/min**, putting one 100k-row chunk at
**≤33.8 hours**, across **184 chunks** for CC-MAIN-2024-22 (18.3M rows).

49.3/min is a *lower bound*: it counts only documents Trafilatura rejected
(`discarding data` log lines); successful extractions aren't logged. So
33.8h/chunk is a ceiling, not a measurement. Even 3× faster is still ~150 days.

Manifest building is unaffected by any of this — it's network-bound and the Pi
handles it fine.

## Mac `.env`

```ini
# COMPOSE_PROFILES=local-db          <- commented out: Postgres lives on the Pi
COMPOSE_FILE=docker-compose.yaml:docker-compose.remote-db.yaml
POSTGRES_HOST=192.168.1.18
POSTGRES_PORT=5432
AIRFLOW_DB_NAME=airflow_mac

AIRFLOW_UID=501                      # id -u on macOS
```

`POSTGRES_PASSWORD` and `GREEK_CC_DB_PASSWORD` must match the Pi's — same
server. The other secrets (Fernet key, JWT secret, UI login) are per-Airflow
instance and can differ, though copying them is simpler. `AWS_*` is needed on
whichever machine runs extraction.

The Pi's `.env` keeps `COMPOSE_PROFILES=local-db` and no `AIRFLOW_DB_NAME`,
so it stays on the `airflow` database exactly as before.

## Which DAG runs where

Both machines mount both DAG files, but pause state lives in each machine's own
metadata database, so it's independent and sticks:

| | `greek_cc_manifest` | `greek_cc_extract` |
|---|---|---|
| **Pi** | unpaused | **paused** |
| **Mac** | **paused** | unpaused |

Set it once on each machine after first start. Since the metadata DBs are
separate, neither can override the other.

## Manifests: NFS from the Pi

The Pi writes them, the Mac reads them. This is cheap: the manifest is 188 row
groups of ~98k rows, and `scan_parquet(...).slice()` reads only the footer plus
the row groups a chunk needs — about **6 MB over the wire per chunk** (16 MB
uncompressed), against a chunk that takes hours of CPU.

**The catch is writes.** `EXTRACTIONS_DIR` is
`/opt/airflow/manifests/extractions` — *inside* the manifests tree — and it's
where stage 1 writes chunk output, stage 2 reads it back, and the final output
lands. Mounting `manifests/` over NFS naively would put all of that on the
network. A stalled NFS mount blocks in uninterruptible I/O, which is far worse
to recover from than a failed HTTP request.

Nested mounts fix this with no code change — the second mount shadows the
subdirectory, so only manifest *reads* cross the network:

```yaml
    # in the Mac's docker-compose override
    - /Volumes/pi-manifests:/opt/airflow/manifests        # NFS, input only
    - ./extractions:/opt/airflow/manifests/extractions    # local disk, all output
```

Export from the Pi read-only (`/etc/exports`), since the Mac never needs to
write there.

**Simpler alternative:** copy `CC-MAIN-2024-22.parquet` (1.2 GB) once and skip
NFS entirely. You'd re-copy when the Pi finishes a new crawl, but there's no
ongoing network dependency. Reasonable if new crawls are rare.

## Repo and secrets

One repo, one clone, one `.env`:

```bash
git clone https://github.com/alexliap/greek-cc.git
```

`.env` is gitignored, so it needs copying by hand (or rebuilding from
`.env.example` — but `POSTGRES_PASSWORD`, `GREEK_CC_DB_PASSWORD` and the AWS
credentials must match the Pi's).

## Other Mac settings

**Docker Desktop resources** — Settings → Resources: at least **8 GB RAM**, and
as many CPUs as you can spare; CPU is the bottleneck.

**Enable VirtioFS** — Settings → General. Much faster than gRPC-FUSE.

**Port 8080** must be free. 5432 doesn't need to be — no local Postgres.

**The `dns:` setting is harmless.** It exists because the Pi's systemd-resolved
stub collapsed under concurrent S3 fetches; macOS has no systemd-resolved.

**Platform:** the image is `linux/arm64`, which Apple Silicon runs natively — no
emulation, no rebuild. (Intel Macs build `linux/amd64`; also fine.)

## Running it

```bash
cd greek-cc
docker compose build airflow-init     # ~15 min first time (fasttext + 1.57GB model)
docker compose up -d
```

`airflow-init` runs `airflow db migrate` against `airflow_mac` (creating its
schema) and `ensure_schema()` against the shared `greek_cc` database — the
latter is idempotent (`CREATE TABLE IF NOT EXISTS` throughout), so running it
from both machines is safe.

UI at http://localhost:8080. Pause `greek_cc_manifest`, unpause
`greek_cc_extract`.

Rebuilds after editing `src/` take **~8 seconds** — the Dockerfile puts
the slow steps above `COPY src/` deliberately.

## Caveats worth knowing

**The Mac depends on the Pi being up.** The scheduler talks to Postgres
constantly. `scheduler_health_check_threshold` is already raised to 300s, which
absorbs stalls, but a Pi reboot mid-run will disrupt the Mac's tasks.

**The DB password crosses the LAN unencrypted.** Fine on a trusted home network;
worth revisiting otherwise.

## What actually determines completion time

Everything above gets it *running*. But extraction is still pinned to **one
core**:

- `LocalPipelineExecutor(..., tasks=1)` — `src/greek_cc/extract.py`
- `max_active_tis_per_dag=1` — `dags/greek_cc_extract_dag.py`

Both were guards against the Pi running out of memory when one chunk buffered
its entire read-ahead. **That constraint is gone**: the reader now uses a
bounded sliding window (`warc_reader.py`), capping peak memory per chunk at
`max_in_flight` documents regardless of chunk size.

On a 10–12 core Mac, raising these is what turns "runs correctly" into "finishes
this month" — extraction is CPU-bound, so the win is close to linear in cores.
With faster per-core performance too, expect roughly **20–30×**: months down to
about a week.

That's a change to greek-cc, not to this deployment, and it's optional. Worth
doing once the move is confirmed working, raising concurrency gradually while
watching memory.
