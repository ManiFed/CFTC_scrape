FROM python:3.11-slim

# Keep Python output unbuffered in logs and avoid .pyc generation in containers.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy project definition first so dependency install layer is cached.
COPY pyproject.toml ./
COPY cftc_pipeline/ ./cftc_pipeline/
COPY alembic/ ./alembic/
COPY alembic.ini ./

# Install the package and all dependencies.
RUN pip install --no-cache-dir -e .

# Optional: pre-download the sentence-transformer model.
# Disabled by default to keep Railway builds significantly faster.
ARG PRELOAD_EMBEDDING_MODEL=0
RUN if [ "$PRELOAD_EMBEDDING_MODEL" = "1" ]; then \
      python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"; \
    fi

COPY start.sh ./
RUN chmod +x start.sh

EXPOSE 8501

CMD ["./start.sh"]
