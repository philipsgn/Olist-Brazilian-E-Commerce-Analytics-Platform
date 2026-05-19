#!/usr/bin/env bash
# Prepare bind-mounted Airflow directories with correct ownership for AIRFLOW_UID.
set -euo pipefail

AIRFLOW_UID="${AIRFLOW_UID:-50000}"
AIRFLOW_GID="${AIRFLOW_GID:-0}"

DIRS=(
  /mnt/airflow/dags
  /mnt/airflow/logs
  /mnt/airflow/plugins
  /mnt/airflow/utils
  /mnt/data
  /mnt/ingestion
  /mnt/dbt_project
  /mnt/scripts
  /mnt/analytics
  /mnt/tmp
  /mnt/certs
)

for dir in "${DIRS[@]}"; do
  mkdir -p "${dir}"
done

chown -R "${AIRFLOW_UID}:${AIRFLOW_GID}" \
  /mnt/airflow/dags \
  /mnt/airflow/logs \
  /mnt/airflow/plugins \
  /mnt/airflow/utils \
  /mnt/data \
  /mnt/ingestion \
  /mnt/dbt_project \
  /mnt/scripts \
  /mnt/analytics \
  /mnt/tmp

chmod -R ug+rwX \
  /mnt/airflow/dags \
  /mnt/airflow/logs \
  /mnt/airflow/plugins \
  /mnt/airflow/utils \
  /mnt/data \
  /mnt/ingestion \
  /mnt/dbt_project \
  /mnt/scripts \
  /mnt/analytics \
  /mnt/tmp

# Certs stay root-owned but world-readable for the non-root airflow user.
if [ -d /mnt/certs ]; then
  chmod -R a+rX /mnt/certs || true
fi

echo "Airflow bind-mount permissions initialized for uid=${AIRFLOW_UID} gid=${AIRFLOW_GID}."
