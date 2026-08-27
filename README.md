# airflow

Shared Airflow deployment. Projects contribute DAGs to this instance rather
than each running their own Airflow.

## Layout

- `docker-compose.yaml` — the deployment. Each project's DAGs mount into their
  own subdirectory under `/opt/airflow/dags/<project>` so filenames can't collide.
- `docker/Dockerfile` — a shared Airflow base, followed by one clearly-labeled
  block per project containing only what *that* project needs.

## Adding a project

1. Add its directory as a named build context under `airflow-init.build.additional_contexts`.
2. Mount its DAGs: `- ../<project>/dags:/opt/airflow/dags/<project>`.
3. Add a `# ---- project: <name> ----` block to `docker/Dockerfile` with its
   own dependencies. Put slow steps (compiles, model downloads) *above* the
   `COPY src/` so editing source doesn't invalidate them.

## Running across two machines

One machine hosts Postgres; others connect to it. Which one this is comes
entirely from `.env`, so `docker compose up -d` is the same command everywhere:

- **Hosts Postgres:** `COMPOSE_PROFILES=local-db`
- **Uses a remote Postgres:** set `COMPOSE_FILE` to chain
  `docker-compose.remote-db.yaml`, plus `POSTGRES_HOST`

Machines that each own a different DAG need their **own metadata database**
(`AIRFLOW_DB_NAME`) — any scheduler can run any DAG it can see, so sharing one
means they steal each other's tasks. They can still share the Postgres server,
and share application state. See [docs/running-on-macos.md](docs/running-on-macos.md).

## Notes

- `postgres-db-volume` is pinned to the name `greek-cc_postgres-db-volume` for
  historical reasons — it predates this directory. Renaming it silently starts
  from an empty database.
- Secrets live in `.env` (gitignored); see `.env.example`.

## Usage

    docker compose up -d              # start
    docker compose build airflow-init # rebuild the image
    docker compose down               # stop (keeps the data volume)

UI at http://localhost:8080.
