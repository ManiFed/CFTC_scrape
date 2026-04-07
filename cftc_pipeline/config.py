"""Central configuration loaded from environment variables."""
from __future__ import annotations

from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    database_url: str = "postgresql://postgres:password@localhost:5432/cftc_pipeline"

    # OpenAI
    openai_api_key: str = ""
    openrouter_api_key: str = ""
    openrouter_base_url: str = "https://openrouter.ai/api/v1"

    # Storage
    storage_backend: str = "local"  # "local" | "s3"
    storage_base_path: Path = Path("./data")
    s3_bucket: str = ""
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_default_region: str = "us-east-1"

    # Scraper
    request_delay_seconds: float = 1.0
    request_timeout_seconds: int = 30
    max_retries: int = 3
    cftc_base_url: str = "https://comments.cftc.gov"

    # LLM
    llm_model: str = "gpt-4.1"
    llm_max_tokens: int = 4096
    prompt_version: str = "v1"

    # Pipeline
    log_level: str = "INFO"
    batch_size_llm: int = 5  # concurrent LLM calls
    batch_size_embed: int = 64

    # Deduplication
    minhash_num_perm: int = 128
    minhash_threshold: float = 0.85
    campaign_min_size: int = 3


settings = Settings()
