import os
import logging

logger = logging.getLogger(__name__)
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev").lower()

# ---------------------------------------------------------------------------
# Metadata DB — Ép dùng SQLite cô lập để chống lỗi Migration loop 500
# ---------------------------------------------------------------------------
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

logger.info("[superset_config] Metadata DB switched to Isolated SQLite for Production Stability.")

# ---------------------------------------------------------------------------
# Security & Sessions
# ---------------------------------------------------------------------------
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "OKpGCLOQnI--jKmbl-__oGvtSQ_PFefKp0uY1rVGJIg=")

WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_HTTPONLY = True