"""Configurações centralizadas carregadas do arquivo .env."""

from __future__ import annotations

import os
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()


class Settings:
    # PostgreSQL
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "bgp_mining")
    postgres_user: str = os.getenv("POSTGRES_USER", "bgp")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "changeme")

    # Redis
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Coleta
    collection_interval_minutes: int = int(
        os.getenv("COLLECTION_INTERVAL_MINUTES", "60")
    )
    http_delay_seconds: float = float(os.getenv("HTTP_DELAY_SECONDS", "1.0"))
    http_timeout_seconds: int = int(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "5"))

    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def asyncpg_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
