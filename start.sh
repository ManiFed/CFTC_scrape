#!/bin/bash
set -e

# Run database migrations on every startup so the schema stays current
echo "Running database migrations..."
alembic upgrade head

# Start the Streamlit UI; Railway injects $PORT
echo "Starting Streamlit on port ${PORT:-8501}..."
exec streamlit run cftc_pipeline/ui/streamlit_app.py \
    --server.port="${PORT:-8501}" \
    --server.address=0.0.0.0
