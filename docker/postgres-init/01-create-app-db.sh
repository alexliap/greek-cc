#!/bin/bash
# Runs only on first boot, when the postgres data volume is empty.
# Table DDL lives in greek_cc.db.ensure_schema() instead, so the schema can
# evolve without wiping this volume.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE greek_cc;
    CREATE USER greek_cc_app WITH PASSWORD '$GREEK_CC_DB_PASSWORD';
    GRANT ALL PRIVILEGES ON DATABASE greek_cc TO greek_cc_app;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname greek_cc <<-EOSQL
    GRANT ALL ON SCHEMA public TO greek_cc_app;
EOSQL
