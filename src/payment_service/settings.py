from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    environment: Literal["local", "test", "production"] = "local"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    api_key: SecretStr = Field(min_length=1)
    database_url: SecretStr = Field(min_length=1)
    rabbitmq_url: SecretStr = Field(min_length=1)
    outbox_batch_size: int = Field(default=10, ge=1, le=100)
    outbox_poll_interval: float = Field(default=0.5, gt=0)
    outbox_claim_seconds: int = Field(default=15, ge=1)
    outbox_publish_timeout: float = Field(default=5, gt=0)
    outbox_retry_delay: float = Field(default=1, gt=0)

    @model_validator(mode="after")
    def validate_outbox_claim_duration(self) -> Self:
        if self.outbox_claim_seconds <= self.outbox_publish_timeout:
            raise ValueError("OUTBOX_CLAIM_SECONDS must exceed OUTBOX_PUBLISH_TIMEOUT")

        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
