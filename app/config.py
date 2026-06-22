from pathlib import Path
from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = ConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    database_url: str = "sqlite:///./data/jobs.db"

    # Storage
    storage_root: str = "./data/files"

    # Redis / RQ
    redis_url: str = "redis://localhost:6379/0"

    # File upload limits
    max_upload_bytes: int = 100 * 1024 * 1024  # 100 MB
    chunk_size: int = 64 * 1024  # 64 KB read chunks

    # Allowed content types for upload
    allowed_content_types: list[str] = [
        "text/csv",
        "application/json",
        "application/octet-stream",
        "text/plain",
        "application/zip",
        "application/gzip",
        "application/x-gzip",
    ]

    allowed_extensions: list[str] = [
        ".csv", ".json", ".txt", ".zip", ".gz",
    ]

    # Retention (seconds)
    file_retention_seconds: int = 86400  # 24 hours

    # Webhook
    webhook_timeout_seconds: int = 10
    webhook_max_retries: int = 3

    # Job cleanup interval (seconds)
    cleanup_interval_seconds: int = 3600


settings = Settings()

# Ensure storage and DB directories exist at import time
Path(settings.storage_root).mkdir(parents=True, exist_ok=True)
Path(settings.database_url.replace("sqlite:///", "")).parent.mkdir(
    parents=True, exist_ok=True
)
