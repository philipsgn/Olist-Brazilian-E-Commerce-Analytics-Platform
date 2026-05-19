#!/usr/bin/env bash
set -euo pipefail

REQUIRED=(
  RDS_HOST
  RDS_PORT
  RDS_DB
  RDS_USER
  RDS_PASSWORD
)

MISSING=()
for VAR in "${REQUIRED[@]}"; do
  if [ -z "${!VAR:-}" ]; then
    MISSING+=("$VAR")
  fi
done

if [ ${#MISSING[@]} -ne 0 ]; then
  echo "ERROR: Missing required RDS environment variables: ${MISSING[*]}" >&2
  echo "Set these values in .env, your CI/CD secrets, or your deployment environment." >&2
  exit 1
fi

CERT_PATH="/opt/airflow/certs/global-bundle.pem"
if [ ! -f "$CERT_PATH" ]; then
  echo "ERROR: SSL certificate not found at $CERT_PATH" >&2
  exit 1
fi
if [ ! -r "$CERT_PATH" ]; then
  echo "ERROR: SSL certificate exists but is not readable: $CERT_PATH" >&2
  exit 1
fi

echo "✔ RDS env validation passed"
echo "✔ SSL cert present and readable at $CERT_PATH"
