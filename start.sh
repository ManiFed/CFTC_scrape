#!/bin/bash
set -euo pipefail

PORT="${PORT:-8501}"
RUN_MIGRATIONS="${RUN_MIGRATIONS:-false}"
MIGRATION_TIMEOUT_SECONDS="${MIGRATION_TIMEOUT_SECONDS:-90}"

if [ "${RUN_MIGRATIONS}" = "true" ]; then
  echo "Running database migrations (timeout: ${MIGRATION_TIMEOUT_SECONDS}s)..."
  if command -v timeout >/dev/null 2>&1; then
    timeout "${MIGRATION_TIMEOUT_SECONDS}" alembic upgrade head || {
      echo "Migration step failed or timed out; continuing startup so the service can become healthy."
    }
  else
    alembic upgrade head || {
      echo "Migration step failed; continuing startup so the service can become healthy."
    }
  fi
else
  echo "Skipping database migrations on boot (RUN_MIGRATIONS=${RUN_MIGRATIONS})."
fi

echo "Starting Streamlit on port ${PORT}..."
exec streamlit run cftc_pipeline/ui/streamlit_app.py \
  --server.port="${PORT}" \
  --server.address=0.0.0.0
