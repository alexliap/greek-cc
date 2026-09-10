#!/bin/bash
# Optional one-time-per-boot setup, run before starting an Airflow process.
#
# docker-compose's airflow-init service (see docker-compose.yaml) is a
# dedicated one-shot container that does this same work then stops. Railway
# has no equivalent "run once, then stop" service type, so every Airflow
# service there sets AIRFLOW_RUN_DB_INIT=1 and uses this script as its start
# command instead of calling `airflow <command>` directly. Every step below
# is idempotent (CREATE TABLE IF NOT EXISTS, overwriting the same password
# file, a migration already at head is a no-op), so re-running it on every
# boot of every service is safe.
set -euo pipefail

if [ "${AIRFLOW_RUN_DB_INIT:-0}" = "1" ]; then
  airflow db migrate

  python -c "
import json, os
path = os.environ['AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE']
os.makedirs(os.path.dirname(path), exist_ok=True)
json.dump({os.environ['_AIRFLOW_WWW_USER_USERNAME']: os.environ['_AIRFLOW_WWW_USER_PASSWORD']}, open(path, 'w'))
print('seeded', path)
"

  python -c "
from airflow.providers.postgres.hooks.postgres import PostgresHook
from greek_cc import db
db.ensure_schema(PostgresHook(postgres_conn_id='greek_cc_db').get_conn())
"
fi

# airflow-init calls this with no extra args -- init above already ran, and
# there's no long-running process to start, so just stop here.
if [ "$#" -eq 0 ]; then
  exit 0
fi

exec airflow "$@"
