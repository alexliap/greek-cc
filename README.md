# greek-cc

Extracting Greek-language text from Common Crawl.

- **`greek_cc_manifest`** (Airflow DAG) — walks a crawl's index and writes a
  Parquet manifest of the Greek records worth fetching, then uploads it to
  the HF Hub dataset repo `alexliap/greek-cc-manifests`. Network-bound.
- **`greek-cc-extract`** (CLI, run on demand) — downloads one crawl's
  manifest from that HF Hub repo, fetches the WARC records and runs them
  through the datatrove cleaning pipeline. CPU-bound. Not an Airflow job —
  just `uv run greek-cc-extract <crawl>`.

The two don't talk to each other directly, Airflow-level or database-level —
they coordinate only by manifests landing on HF Hub. That's what lets
extraction run anywhere (a Mac, say) with nothing but this repo's Python
environment and a `.env`.

New here (human or agent)? Start with [CLAUDE.md](CLAUDE.md) — orientation,
known traps, and a runbook.

## Layout

- `src/greek_cc/` — the pipeline itself; installed into the Airflow image,
  and locally wherever `greek-cc-extract` runs.
- `src/greek_cc/cli.py` — the on-demand extraction entrypoint.
- `dags/` — `greek_cc_manifest`, bind-mounted into `/opt/airflow/dags`.
- `docker/Dockerfile` — Airflow base plus this project's dependencies.
- `docker-compose.yaml` — the Airflow deployment (manifest side only).
- `manifests/` — manifest Parquet files and extraction output (gitignored).

## Usage

Manifests (Airflow, wherever Postgres/the DAG lives — the Pi today):

    docker compose up -d              # start
    docker compose build airflow-init # rebuild the image
    docker compose down               # stop (keeps the data volume)

UI at http://localhost:8080. Secrets live in `.env` (gitignored); see
`.env.example`.

Extraction (any machine with this repo cloned):

    uv sync
    uv run greek-cc-extract CC-MAIN-2024-22

`fasttext-numpy2-wheel` (GlotLID's backend) builds from source and needs a
C++17 compiler — `xcode-select --install` on macOS if `uv sync` fails on it.
`python-magic` needs the native `libmagic` too — `brew install libmagic` on
macOS. `.env` needs `AWS_*` and `HF_TOKEN`/`HF_MANIFEST_REPO` at minimum;
see `.env.example`.

## Notes

- `postgres-db-volume` is pinned to the name `greek-cc_postgres-db-volume`.
  Renaming it silently starts from an empty database.
- The Dockerfile puts the slow steps (fasttext compile, 1.57GB GlotLID
  download) *above* `COPY src/`, so editing source rebuilds in ~8 seconds
  rather than ~15 minutes. Keep new slow steps above that line.
