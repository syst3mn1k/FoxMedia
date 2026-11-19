
from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = "development"
    log_level: str = "INFO"

    database_url: str
    redis_url: str

    telegram_bot_token: SecretStr | None = None
    telegram_allowed_user_ids: set[int] = Field(default_factory=set)

    butler_api_url: str = "http://127.0.0.1:8000"

    sonarr_base_url: str | None = None
    sonarr_api_key: SecretStr | None = None
    sonarr_api_version: int = 3

    radarr_base_url: str | None = None
    radarr_api_key: SecretStr | None = None
    radarr_api_version: int = 3

    jellyfin_base_url: str | None = None
    jellyfin_api_key: SecretStr | None = None

    redis_stream: str = "foxmedia:jobs"
    redis_consumer_group: str = "foxmedia-workers"

    job_max_attempts: int = 5
    job_retry_base_seconds: float = 5.0
    job_retry_max_seconds: float = 300.0

    model_config = SettingsConfigDict(
        env_file=".env.dev",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
