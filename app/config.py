
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ECMWF
    ecmwf_base_url: str = "https://data.ecmwf.int/forecasts"
    ecmwf_lookback_days: int = Field(default=3, ge=1, le=7)
    poll_interval_seconds: int = Field(default=21600, ge=60)
    time_slots: list[str] = Field(
        default_factory=lambda: ["00z", "06z", "12z", "18z"]
    )
    model: str = "ifs"
    resolution: str = "0p25"
    target_folders: list[str] = Field(default_factory=lambda: ["oper"])

    # MinIO
    minio_host: str = "minio"
    minio_port: int = 10000
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "adminminio"
    minio_secure: bool = False
    minio_bucket: str = "ecmwf-data"
    minio_region: str = "us-east-1"

    # Download
    max_parallel_downloads: int = Field(default=20, ge=1, le=100)
    retry_count: int = Field(default=3, ge=0, le=10)
    retry_backoff_seconds: float = Field(default=2.0, ge=0.1, le=60)
    http_timeout_seconds: float = Field(default=60.0, ge=5, le=600)
    temp_dir: Path = Path("/tmp/ecmwf-downloads")

    # Logging
    log_level: Literal[
        "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
    ] = "INFO"

    # FastAPI
    api_host: str = "0.0.0.0"
    api_port: int = Field(default=8080, ge=1, le=65535)

    @field_validator("time_slots")
    @classmethod
    def validate_time_slots(cls, value: list[str]) -> list[str]:
        allowed = {"00z", "06z", "12z", "18z"}
        normalized = [item.strip().lower() for item in value if item.strip()]

        if not normalized or not set(normalized).issubset(allowed):
            raise ValueError(
                f"time_slots must be a non-empty subset of {sorted(allowed)}"
            )

        return list(dict.fromkeys(normalized))

    @field_validator("target_folders")
    @classmethod
    def validate_target_folders(cls, value: list[str]) -> list[str]:
        allowed = {"oper", "wave", "enfo", "waef"}
        normalized = [item.strip().lower() for item in value if item.strip()]

        if not normalized or not set(normalized).issubset(allowed):
            raise ValueError(
                f"target_folders must be a non-empty subset of {sorted(allowed)}"
            )

        return list(dict.fromkeys(normalized))

    @property
    def normalized_base_url(self) -> str:
        return self.ecmwf_base_url.rstrip("/")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
