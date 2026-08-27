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
