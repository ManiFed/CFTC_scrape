FROM python:3.11-slim

# System dependencies for lxml, hdbscan (C extensions), and PDF processing
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project definition first so dependency layer is cached
COPY pyproject.toml ./
COPY cftc_pipeline/ ./cftc_pipeline/
COPY alembic/ ./alembic/
COPY alembic.ini ./

# Install the package and all dependencies
RUN pip install --no-cache-dir -e .

# Pre-download the sentence-transformer model used by the clustering stage
# so it's baked into the image and won't block cold starts
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

COPY start.sh ./
RUN chmod +x start.sh

EXPOSE 8501

CMD ["./start.sh"]
