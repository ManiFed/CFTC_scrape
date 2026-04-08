# CFTC Comment Pipeline

End-to-end system for ingesting, analyzing, and reporting on CFTC public comment dockets.

## What it does

1. **Crawls** a CFTC public comment docket (list page + detail pages + attachments)
2. **Extracts** text from HTML bodies, PDFs, and Word documents
3. **Deduplicates** using exact hash + MinHash LSH + campaign detection
4. **Analyzes** each submission with OpenAI (stance, issues, arguments, scores) — strict JSON schema
5. **Clusters** submissions into issue themes using sentence embeddings + HDBSCAN
6. **Ranks** submissions by substantive signal value
7. **Generates** a traceable Markdown report
8. **Serves** a Streamlit analyst UI with search, filters, drill-down, and source inspection

---

## Quick start

### 1. Prerequisites (local run with your existing Postgres URL)

- Python 3.11+
- A valid PostgreSQL connection string (you already have this)
- An OpenRouter API key

### 2. Install

```bash
cd CFTC_scrape
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 3. Configure

Create a `.env` file in the repo root with **exactly** these values (replace placeholders):

```bash
cat > .env << 'EOF'
DATABASE_URL=postgresql+psycopg://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>
OPENROUTER_API_KEY=<YOUR_OPENROUTER_API_KEY>
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
LLM_MODEL=openai/gpt-4.1
EOF
```

Notes:
- Use your real Postgres URL in `DATABASE_URL` (if your provider gives `postgres://...`, that usually works too).
- Do **not** set `OPENAI_API_KEY` when using OpenRouter.

### 4. Create tables

```bash
cftc create-tables
# Or via alembic:
# alembic upgrade head
```

### 5. Register a docket

```bash
cftc init-docket \
  --docket "7654" \
  --url "https://comments.cftc.gov/PublicComments/CommentList.aspx?id=7654" \
  --title "Prediction Markets"
```

### 6. Run the pipeline

```bash
# Full pipeline
cftc run --docket 7654

# Single stage
cftc run-stage --docket 7654 --stage crawl_docket

# Specific stages only
cftc run --docket 7654 --stages "crawl_docket,fetch_comment_pages,fetch_attachments"

# Force re-run (skip idempotency check)
cftc run --docket 7654 --stage extract_text --force
```

### 7. Check status

```bash
cftc status --docket 7654
```

### 8. Launch the analyst UI

```bash
streamlit run cftc_pipeline/ui/streamlit_app.py
# Opens at http://localhost:8501
```

### 9. Export data

```bash
cftc export-csv --docket 7654
# Outputs: data/exports/7654/submissions.csv
#          data/exports/7654/analyses.jsonl
#          data/exports/7654/report_<id>.md
```

---

## Pipeline stages

| Stage | Description | Idempotent |
|---|---|---|
| `crawl_docket` | Crawl comment list pages, create Submission records | Yes |
| `fetch_comment_pages` | Visit each detail page, extract body, detect attachments | Yes |
| `fetch_attachments` | Download all attachment binaries | Yes |
| `extract_text` | Extract text from HTML/PDF/DOCX | Yes |
| `normalize_text` | Build canonical combined text per submission | Yes |
| `dedupe_submissions` | Exact + near-dup + campaign deduplication | Yes |
| `analyze_submission_llm` | LLM structured extraction per submission | Yes (skips analyzed) |
| `cluster_themes` | Embed + HDBSCAN cluster | Yes |
| `summarize_clusters` | LLM cluster description + rep arguments | Yes |
| `rank_high_signal_submissions` | Multi-factor ranking | Yes |
| `generate_report` | Jinja2 Markdown report | Creates new run each time |
| `build_exports` | CSV + JSONL export | Overwrites |

All stages are individually rerunnable. Pass `--force` to bypass the idempotency check.

---

## Architecture

```
CFTC site → Scraper → PostgreSQL ← Pipeline stages
                   ↓
              File Store (local/S3)
                   ↓
              Analyst UI (Streamlit) + Report (Markdown)
```

### Key tables

| Table | Purpose |
|---|---|
| `dockets` | Registered dockets |
| `submissions` | One row per comment |
| `attachments` | Attachment files and download status |
| `extraction_results` | Extracted text per source (html/pdf/canonical) |
| `dedupe_groups` | Deduplication groups |
| `submission_dedupe` | Membership + canonical flag |
| `llm_analyses` | Full structured LLM output + scores |
| `theme_clusters` | Cluster labels and summaries |
| `cluster_memberships` | Submission → cluster mapping |
| `report_runs` | Report generation runs |
| `report_claim_sources` | Claim traceability |
| `pipeline_jobs` | Stage execution log |

---

## Configuration

All settings in `.env`:

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | — | PostgreSQL connection URL |
| `OPENROUTER_API_KEY` | — | OpenRouter API key (set this for local setup in this guide) |
| `OPENROUTER_BASE_URL` | `https://openrouter.ai/api/v1` | OpenRouter-compatible API base URL |
| `LLM_MODEL` | `gpt-4.1` | LLM model ID (OpenAI or OpenRouter) |
| `OPENAI_API_KEY` | — | Optional OpenAI key (leave unset if using OpenRouter) |
| `CODEX_CLI_AUTH_TOKEN` | — | Codex CLI auth token (fallback when API key is unset) |
| `PROMPT_VERSION` | `v1` | Extraction prompt version |
| `STORAGE_BACKEND` | `local` | `local` or `s3` |
| `STORAGE_BASE_PATH` | `./data` | Local storage root |
| `REQUEST_DELAY_SECONDS` | `1.0` | Rate limit between requests |
| `MINHASH_THRESHOLD` | `0.85` | Jaccard threshold for near-dup |
| `CAMPAIGN_MIN_SIZE` | `3` | Min group size for campaign flag |

---

## Tests

```bash
pytest tests/ -v
pytest tests/ -v --cov=cftc_pipeline --cov-report=term-missing
```

---

## Production deployment

### Railway setup

This repo is already configured for Railway with:

- `railway.toml` health checks for Streamlit (`/_stcore/health`)
- Docker-based deploys (`builder = "DOCKERFILE"`)
- `start.sh` that starts Streamlit on Railway's `PORT` and can optionally run migrations

Follow these steps:

1. **Create services in Railway**
   - Create a **PostgreSQL** service.
   - Create a service from this repo for the **web app**.
   - Attach both services to the same Railway project.

2. **Set required variables on the web service**
   - `DATABASE_URL` (use Railway Postgres connection string)
   - `OPENAI_API_KEY` (or `OPENROUTER_API_KEY`)
   - `OPENROUTER_BASE_URL` (only if using OpenRouter)
   - `LLM_MODEL` (optional override, defaults to `gpt-4.1`)
   - `STORAGE_BACKEND` (`local` by default, or `s3`)
   - `STORAGE_BASE_PATH` (default `./data`)
   - `RUN_MIGRATIONS=false` (recommended on Railway web startup)
   - Optional: `MIGRATION_TIMEOUT_SECONDS=90`

3. **Deploy**
   - Railway will build with the repository `Dockerfile`.
   - On startup, `start.sh` launches Streamlit with:
     - `--server.address=0.0.0.0`
     - `--server.port=$PORT`
   - Health checks use `/_stcore/health` (already configured in `railway.toml`).

4. **Run migrations safely**
   - Recommended: run migrations as a one-off command/job:
     ```bash
     alembic upgrade head
     ```
   - Avoid running migrations in normal web boot unless needed.
   - If you do want startup migrations, set `RUN_MIGRATIONS=true`.

5. **Initialize a docket and run pipeline**
   - Open a Railway shell / one-off command and run:
     ```bash
     cftc create-tables
     cftc init-docket --docket "7654" --url "https://comments.cftc.gov/PublicComments/CommentList.aspx?id=7654" --title "Margin Requirements for Uncleared Swaps"
     cftc run --docket 7654
     ```

### Database

Use a managed PostgreSQL instance (RDS, Cloud SQL, Supabase). Set `DATABASE_URL` in environment.

Run migrations (recommended as a separate release/one-off job, not on web startup):
```bash
alembic upgrade head
```

For Railway, web startup now skips migrations by default to avoid healthcheck timeouts.
If you still want startup migrations, set `RUN_MIGRATIONS=true` (optionally tune `MIGRATION_TIMEOUT_SECONDS`, default `90`).

### Storage

Set `STORAGE_BACKEND=s3` and configure:
```
S3_BUCKET=your-bucket
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

### Running the pipeline on a schedule

Use a cron job, GitHub Actions, or any scheduler:
```bash
# Example cron: run nightly
0 2 * * * cd /opt/cftc_pipeline && .venv/bin/cftc run --docket 7654
```

### Serving the UI

```bash
# With authentication (use a reverse proxy)
streamlit run cftc_pipeline/ui/streamlit_app.py --server.port 8501 --server.address 0.0.0.0
```

Recommended: put Nginx in front with HTTP basic auth or OAuth2 proxy.

### Docker (optional)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install -e .
ENV DATABASE_URL=...
CMD ["cftc", "run", "--docket", "7654"]
```

---

## Admin: reruns and recovery

### Rerun a failed stage

```bash
cftc run-stage --docket 7654 --stage fetch_attachments --force
```

### Reset a stage (delete job record manually)

```sql
DELETE FROM pipeline_jobs WHERE docket_id = 1 AND stage = 'cluster_themes';
```

Then rerun normally (without `--force`).

### Inspect extraction failures

```sql
SELECT s.external_id, s.commenter_name, er.error_message
FROM extraction_results er
JOIN submissions s ON s.id = er.submission_id
WHERE er.extraction_status = 'failed'
  AND s.docket_id = 1;
```

### Inspect LLM analysis failures

```sql
SELECT s.external_id, la.error_message
FROM llm_analyses la
JOIN submissions s ON s.id = la.submission_id
WHERE la.analysis_status = 'failed'
  AND s.docket_id = 1;
```

### Re-analyze specific submissions

```sql
-- Remove existing analysis record to allow re-analysis
DELETE FROM llm_analyses WHERE submission_id IN (42, 87, 103);
-- Then rerun the stage
```

```bash
cftc run-stage --docket 7654 --stage analyze_submission_llm
```

### Change the LLM model or prompt version

Update `.env`:
```
LLM_MODEL=gpt-4.1
PROMPT_VERSION=v2
```

Delete old analysis records and rerun:
```sql
DELETE FROM llm_analyses WHERE submission_id IN (SELECT id FROM submissions WHERE docket_id = 1);
```

### Recalculate clusters after adding submissions

Delete cluster records and rerun:
```sql
DELETE FROM cluster_memberships
  WHERE submission_id IN (SELECT id FROM submissions WHERE docket_id = 1);
DELETE FROM theme_clusters WHERE docket_id = 1;
DELETE FROM pipeline_jobs WHERE docket_id = 1 AND stage IN ('cluster_themes', 'summarize_clusters');
```

```bash
cftc run --docket 7654 --stages "cluster_themes,summarize_clusters,rank_high_signal_submissions,generate_report"
```

---

## Limitations and known issues

- **Scanned PDFs**: OCR is not implemented. Scanned-only PDFs will return empty text. Use a separate OCR tool (Tesseract, AWS Textract) and manually insert extraction results.
- **CFTC site structure changes**: The scraper targets ASP.NET WebForms patterns. If the site redesigns, `cftc_scraper.py` will need updates.
- **Very long submissions**: Text is truncated at 12,000 characters for LLM analysis. The full text is preserved in the database.
- **LLM costs**: ~775 submissions × ~2,000 tokens each ≈ 1.5M tokens. Estimate accordingly.
- **HDBSCAN noise**: Submissions that don't fit any cluster are labeled cluster_id=-1 (noise). These appear in the outlier section of the report.

---

## File layout

```
CFTC_scrape/
├── cftc_pipeline/
│   ├── config.py              # Settings from .env
│   ├── storage.py             # File store abstraction (local/S3)
│   ├── cli.py                 # CLI entry point
│   ├── db/
│   │   ├── models.py          # SQLAlchemy ORM models
│   │   └── session.py         # DB session management
│   ├── scraper/
│   │   ├── cftc_scraper.py    # List + detail page crawler
│   │   ├── attachment_downloader.py
│   │   └── http_client.py     # Rate-limited HTTP with retry
│   ├── extraction/
│   │   ├── html_extractor.py
│   │   ├── pdf_extractor.py   # PyMuPDF + pdfplumber
│   │   ├── docx_extractor.py
│   │   └── text_cleaner.py    # Normalization + canonical text
│   ├── dedup/
│   │   └── deduplicator.py    # Exact + MinHash + campaign
│   ├── analysis/
│   │   ├── schemas.py         # Pydantic output schema
│   │   ├── llm_analyzer.py    # OpenAI API calls + retry
│   │   └── prompts/
│   │       └── v1_extraction.py
│   ├── clustering/
│   │   └── theme_clusterer.py # Embeddings + HDBSCAN + TF-IDF labels
│   ├── ranking/
│   │   └── ranker.py          # Multi-factor signal scoring
│   ├── report/
│   │   ├── generator.py       # Report assembly
│   │   └── templates/
│   │       └── report.md.j2   # Jinja2 report template
│   ├── pipeline/
│   │   ├── stages.py          # Stage implementations
│   │   └── runner.py          # Orchestrator + job tracking
│   └── ui/
│       └── streamlit_app.py   # Analyst interface
├── tests/
│   ├── test_scraper.py
│   ├── test_extraction.py
│   ├── test_dedup.py
│   ├── test_schemas.py
│   └── test_ranking.py
├── alembic/                   # DB migrations
├── data/                      # Local storage (gitignored)
├── pyproject.toml
├── .env.example
└── README.md
```
