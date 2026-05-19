#!/usr/bin/env bash
# Idempotently create Airflow and Superset metadata databases on Postgres/RDS.
set -euo pipefail

POSTGRES_HOST="${POSTGRES_HOST:?POSTGRES_HOST is required}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:?POSTGRES_USER is required}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
POSTGRES_AIRFLOW_DB="${POSTGRES_AIRFLOW_DB:-airflow}"
POSTGRES_SUPERSET_DB="${POSTGRES_SUPERSET_DB:-superset}"
POSTGRES_SSLMODE="${POSTGRES_SSLMODE:-prefer}"
MAX_RETRIES="${POSTGRES_BOOTSTRAP_RETRIES:-30}"
RETRY_INTERVAL="${POSTGRES_BOOTSTRAP_INTERVAL:-5}"

export PGPASSWORD="${POSTGRES_PASSWORD}"
export PGSSLMODE="${POSTGRES_SSLMODE:-prefer}"
PSQL=(psql -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d postgres -v ON_ERROR_STOP=1)

wait_for_postgres() {
  local attempt=1
  while [ "${attempt}" -le "${MAX_RETRIES}" ]; do
    if "${PSQL[@]}" -c "SELECT 1" >/dev/null 2>&1; then
      echo "Postgres is reachable at ${POSTGRES_HOST}:${POSTGRES_PORT}"
      return 0
    fi
    echo "Waiting for Postgres (${attempt}/${MAX_RETRIES})..."
    sleep "${RETRY_INTERVAL}"
    attempt=$((attempt + 1))
  done
  echo "ERROR: Postgres not reachable at ${POSTGRES_HOST}:${POSTGRES_PORT}" >&2
  return 1
}

ensure_database() {
  local db_name="$1"
  local exists
  exists="$("${PSQL[@]}" -tAc "SELECT 1 FROM pg_database WHERE datname = '${db_name}'")"
  if [ "${exists}" = "1" ]; then
    echo "Database '${db_name}' already exists — skipping."
    return 0
  fi
  echo "Creating database '${db_name}'..."
  "${PSQL[@]}" -c "CREATE DATABASE \"${db_name}\" OWNER \"${POSTGRES_USER}\";"
  echo "Database '${db_name}' created."
}

wait_for_postgres
ensure_database "${POSTGRES_AIRFLOW_DB}"
ensure_database "${POSTGRES_SUPERSET_DB}"
echo "Postgres bootstrap completed."
