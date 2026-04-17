"""
=============================================================================
FILE: superset/superset_config.py
=============================================================================
All sensitive values are read from environment variables.
Provide them via docker-compose .env, EC2 UserData, or AWS Secrets Manager.

Required env vars:
  POSTGRES_USER       — PostgreSQL username  (default: de_user)
  POSTGRES_PASSWORD   — PostgreSQL password  (default: de_password)
  POSTGRES_HOST       — Database host        (default: postgres)
  POSTGRES_PORT       — Database port        (default: 5432)
  POSTGRES_DB         — Database name        (default: ecommerce_db)
  SUPERSET_SECRET_KEY — Flask secret key     (REQUIRED in production)
=============================================================================
"""

import os
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Metadata DB — where Superset stores dashboards, charts, users, etc.
# ---------------------------------------------------------------------------
_pg_user     = os.getenv("POSTGRES_USER",     "de_user")
_pg_password = os.getenv("POSTGRES_PASSWORD", "de_password")
_pg_host     = os.getenv("POSTGRES_HOST",     "postgres")   # Docker service name
_pg_port     = os.getenv("POSTGRES_PORT",     "5432")        # Internal Docker port
_pg_db       = os.getenv("POSTGRES_DB",       "ecommerce_db")

# Use SQLite for Superset metadata to prevent Alembic collision with Airflow on ecommerce_db
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

logger.info(
    "[superset_config] Metadata DB → host=%s port=%s db=%s user=%s",
    _pg_host, _pg_port, _pg_db, _pg_user,
)

# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "CHANGE_ME_IN_PRODUCTION")

if SECRET_KEY == "CHANGE_ME_IN_PRODUCTION":
    logger.warning(
        "[superset_config] ⚠️  SUPERSET_SECRET_KEY is using the insecure default. "
        "Set the env var before deploying to production!"
    )

WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

# ---------------------------------------------------------------------------
# Session / Cookie settings
# ---------------------------------------------------------------------------
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE   = os.getenv("SESSION_COOKIE_SECURE", "false").lower() == "true"
SESSION_COOKIE_HTTPONLY = True