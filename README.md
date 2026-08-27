# greek-cc

Extracting Greek-language text from Common Crawl, orchestrated by Airflow.

Two DAGs, coordinating only through the `greek_cc` application database:

- **`greek_cc_manifest`** — walks a crawl's index and writes a Parquet manifest
  of the Greek records worth fetching. Network-bound.
- **`greek_cc_extract`** — reads that manifest in chunks, fetches the WARC
  records and runs them through the datatrove cleaning pipeline. CPU-bound.

New here (human or agent)? Start with [CLAUDE.md](CLAUDE.md) — orientation,
known traps, and a runbook.

## Layout

- `src/greek_cc/` — the pipeline itself; installed into the Airflow image.
- `dags/` — the two DAGs, bind-mounted into `/opt/airflow/dags`.
- `docker/Dockerfile` — Airflow base plus this project's dependencies.
- `docker-compose.yaml` — the deployment.
- `manifests/` — manifest Parquet files and extraction output (gitignored).

## Usage

    docker compose up -d              # start
    docker compose build airflow-init # rebuild the image
    docker compose down               # stop (keeps the data volume)

UI at http://localhost:8080. Secrets live in `.env` (gitignored); see
`.env.example`.

## Running across two machines

One machine hosts Postgres; others connect to it. Which one this is comes
entirely from `.env`, so `docker compose up -d` is the same command everywhere:

- **Hosts Postgres:** `COMPOSE_PROFILES=local-db`
- **Uses a remote Postgres:** set `COMPOSE_FILE` to chain
  `docker-compose.remote-db.yaml`, plus `POSTGRES_HOST`

Machines that each own a different DAG need their **own metadata database**
(`AIRFLOW_DB_NAME`) — any scheduler can run any DAG it can see, so sharing one
means they steal each other's tasks. They can still share the Postgres server,
and share application state. See [docs/running-on-macos.md](docs/running-on-macos.md)
for the split this repo is set up for: manifests on the Pi, extraction on a Mac.

## Notes

- `postgres-db-volume` is pinned to the name `greek-cc_postgres-db-volume`.
  Renaming it silently starts from an empty database.
- The Dockerfile puts the slow steps (fasttext compile, 1.57GB GlotLID
  download) *above* `COPY src/`, so editing source rebuilds in ~8 seconds
  rather than ~15 minutes. Keep new slow steps above that line.
