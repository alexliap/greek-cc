# Running extraction on a Mac, against the Pi

The Pi stays the system of record: it keeps Postgres and the manifests. The Mac
runs the Airflow services and does the CPU-heavy extraction. Nothing about the
pipeline code changes; the whole difference lives in `.env`.

```
   Pi  192.168.1.18                     Mac
   ├── Postgres  :5432   <────────────  Airflow scheduler / apiserver /
   └── manifests/        <────────────  dag-processor  (+ all extraction work)
                (NFS or copy)
```

## Why move at all

Not stability — the crash causes (DNS flood, unbounded read-ahead, scheduler
heartbeat starvation) are fixed and the Pi runs without crashing now. It's
throughput. Measured floor on the Pi was **49.3 docs/min**, putting one 100k-row
chunk at **≤33.8 hours**, across **184 chunks** for CC-MAIN-2024-22 (18.3M
rows). That's months.

49.3/min is a *lower bound*: it counts only documents Trafilatura rejected
(`discarding data` log lines); successful extractions aren't logged. So
33.8h/chunk is a ceiling, not a measurement. The order of magnitude is the
point — even 3× faster is still ~150 days.

## Architecture: no code changes

The image is `linux/arm64`, which Apple Silicon runs **natively** — no
emulation, no platform rebuild. (An Intel Mac builds `linux/amd64` instead;
also fine, just a slower first build, still nothing to edit.)

## Postgres stays on the Pi

Already set up for this — `listen_addresses = '*'`, `host all all all
scram-sha-256`, and port 5432 published. Verified reachable from another host
on the LAN.

On the Mac, `.env` selects the remote-database setup:

```ini
# COMPOSE_PROFILES=local-db          <- commented out: no local Postgres here
COMPOSE_FILE=docker-compose.yaml:docker-compose.remote-db.yaml
POSTGRES_HOST=192.168.1.18
POSTGRES_PORT=5432
```

The `local-db` profile keeps the Mac from starting a Postgres it doesn't need,
and `docker-compose.remote-db.yaml` clears `airflow-init`'s dependency on it.
`docker compose up -d` then works unchanged on both machines.

Both password values (`POSTGRES_PASSWORD`, `GREEK_CC_DB_PASSWORD`) must match
the Pi's, since it's the same database.

**Only one machine should run a scheduler against this database.** Airflow does
support multiple schedulers, but nothing here is set up for it — so when the
Mac takes over extraction, stop the Pi's Airflow services and leave only
Postgres running:

```bash
# on the Pi
docker compose stop airflow-scheduler airflow-apiserver airflow-dag-processor
```

### The tradeoff

The scheduler talks to Postgres constantly, so the Mac's runs now depend on the
Pi staying up and the network staying healthy. Two things soften this:
`scheduler_health_check_threshold` is already raised to 300s, which absorbs long
stalls, and Postgres connections are retried. But a Pi reboot mid-run will still
disrupt the Mac's tasks.

Also note the connection is unencrypted, so the DB password crosses your LAN in
the clear. Fine on a trusted home network; worth revisiting otherwise.

## Manifests: share or copy

The manifest is 188 row groups of ~98k rows, and `scan_parquet(...).slice()`
reads only the footer plus the row groups a chunk needs. So a 100k-row chunk
pulls **~6 MB over the wire** (16 MB uncompressed) — against a chunk that takes
hours of CPU, that's nothing. Sharing over the network is entirely practical.

**The catch is writes, not reads.** `EXTRACTIONS_DIR` is
`/opt/airflow/manifests/extractions`, i.e. *inside* the manifests tree. Naively
mounting `manifests/` over NFS would put stage 1's chunk output — and stage 2's
reads of it — on the network too. A stalled NFS mount blocks in uninterruptible
I/O, which is worse to recover from than a failed HTTP request.

Nested mounts avoid that, with no code change. In the Mac's compose, the second
mount shadows the subdirectory:

```yaml
    - /Volumes/pi-manifests:/opt/airflow/manifests        # NFS from Pi (input)
    - ./extractions:/opt/airflow/manifests/extractions    # local disk (output)
```

**Or just copy it once.** `manifests/CC-MAIN-2024-22.parquet` is 1.2 GB — a few
minutes over the LAN, then zero ongoing network dependency. Simplest, and worth
preferring unless you want the Pi to keep building manifests for new crawls
(which it's well suited to: manifest building is rate-limited by Common Crawl,
not CPU-bound).

## What else to copy

Both repos must sit **as siblings** — the compose file references `../greek-cc`
for DAGs, manifests, and the Postgres init script:

```
projects/
  airflow/     <- this repo
  greek-cc/
```

| What | How | Notes |
|---|---|---|
| `airflow/`, `greek-cc/` | git clone | |
| `airflow/.env` | copy by hand | **gitignored**; then edit as above |
| `greek-cc/.env` | copy by hand | **gitignored**; AWS creds for standalone scripts |
| manifests | NFS or copy | see above |

## Mac-specific settings

**`AIRFLOW_UID`** — it's `1000` (Linux); macOS is typically `501`:

```bash
sed -i '' "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
```

**Docker Desktop resources** — Settings → Resources: at least **8 GB RAM**, and
**as many CPUs as you can spare**; CPU is the bottleneck.

**Enable VirtioFS** — Settings → General. Markedly faster than gRPC-FUSE for
bind mounts.

**Port 8080** must be free. (5432 doesn't need to be, since no local Postgres.)

**The `dns:` setting is harmless.** It exists because the Pi's systemd-resolved
stub collapsed under concurrent S3 fetches; macOS has no systemd-resolved, so
it's simply unnecessary there.

## Running it

```bash
cd airflow
docker compose build airflow-init     # ~15 min first time (fasttext + 1.57GB model)
docker compose up -d
```

UI at http://localhost:8080. Unpause `greek_cc_extract` to start.

Rebuilds after editing `greek-cc/src/` take **~8 seconds** — the Dockerfile puts
the slow steps above `COPY src/` deliberately.

## The thing that actually determines completion time

Everything above gets it *running*. But the pipeline is still pinned to **one
core**:

- `LocalPipelineExecutor(..., tasks=1)` — `greek-cc/src/greek_cc/extract.py`
- `max_active_tis_per_dag=1` — `greek-cc/dags/greek_cc_extract_dag.py`

Both were guards against the Pi running out of memory when a single chunk
buffered its entire read-ahead. **That constraint is gone**: the reader now uses
a bounded sliding window (`warc_reader.py`), so peak memory per chunk is capped
at `max_in_flight` documents regardless of chunk size.

On a 10–12 core Mac, raising these is what turns "runs correctly" into "finishes
this month". Extraction is CPU-bound, so the win is close to linear in cores;
with faster per-core performance too, expect roughly **20–30×** — months down to
about a week.

That's a change to greek-cc, not to this deployment, and it's optional. Worth
doing once the move is confirmed working, raising concurrency gradually while
watching memory.
