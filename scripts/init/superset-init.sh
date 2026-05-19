#!/usr/bin/env bash
# Prepare Superset writable paths inside the named volume before the app starts.
set -euo pipefail

SUPERSET_UID="${SUPERSET_UID:-1000}"
SUPERSET_GID="${SUPERSET_GID:-1000}"

mkdir -p /app/superset_home
chown -R "${SUPERSET_UID}:${SUPERSET_GID}" /app/superset_home
chmod -R ug+rwX /app/superset_home

echo "Superset home volume initialized for uid=${SUPERSET_UID} gid=${SUPERSET_GID}."
