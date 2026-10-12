"""Validated settings with a stable instance shared by all modules."""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="TICK_VAULT_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    base_directory: Path = Path("./tick_vault_data")
    datafeed_base_url: str = "https://datafeed.dukascopy.com/datafeed/"
    fetch_max_retry_attempts: int = Field(default=3, ge=0, le=10)
    fetch_base_retry_delay: float = Field(default=1.0, gt=0, le=60, allow_inf_nan=False)
    fetch_timeout: float = Field(default=30.0, gt=0, allow_inf_nan=False)
    fetch_max_response_bytes: int = Field(
        default=64 * 1024 * 1024, ge=20, le=512 * 1024 * 1024
    )
    fetch_max_retry_delay: float = Field(
        default=60.0, gt=0, le=3600, allow_inf_nan=False
    )
    worker_per_proxy: int = Field(default=4, ge=1, le=100)
    worker_queue_timeout: float = Field(default=60.0, gt=0, allow_inf_nan=False)
    metadata_update_batch_timeout: float = Field(default=1.0, gt=0, allow_inf_nan=False)
    metadata_update_batch_size: int = Field(default=100, ge=1, le=10000)
    adaptive_workers: bool = True
    adaptive_min_workers: int = Field(default=1, ge=1, le=100)
    adaptive_adjustment_interval: int = Field(default=8, ge=1, le=10000)
    queue_size: int = Field(default=128, ge=1, le=100000)
    no_data_retry_after: float = Field(default=86400.0, ge=0, allow_inf_nan=False)
    provider_state_directory: Path | None = None
    provider_require_verified_access: bool = True
    provider_key: str = Field(
        default="dukascopy", min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$"
    )
    provider_request_interval: float = Field(default=2.0, ge=0, allow_inf_nan=False)
    provider_max_concurrency: int = Field(default=1, ge=1, le=100)
    provider_minute_budget: int = Field(default=30, ge=1, le=1000000)
    provider_hour_budget: int = Field(default=1800, ge=1, le=1000000)
    provider_no_header_cooldown: float = Field(default=900.0, gt=0, allow_inf_nan=False)
    provider_cooldown_jitter: float = Field(default=1.0, ge=0, allow_inf_nan=False)
    provider_request_lease_seconds: float = Field(
        default=90.0, gt=0, allow_inf_nan=False
    )
    provider_hour_lease_seconds: float = Field(default=180.0, gt=0, allow_inf_nan=False)
    provider_admission_wait_budget: float = Field(
        default=60.0, ge=0, allow_inf_nan=False
    )
    base_log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "DEBUG"
    console_log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    @field_validator("datafeed_base_url")
    @classmethod
    def validate_datafeed_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "datafeed_base_url must be an HTTPS URL without credentials, query or fragment"
            )
        return value.rstrip("/") + "/"

    # Early supported Pydantic stubs reject this valid instance-validator type.
    @model_validator(mode="after")  # type: ignore[arg-type, unused-ignore]
    def validate_settings(self) -> Self:
        if self.metadata_update_batch_timeout >= self.worker_queue_timeout:
            raise ValueError(
                "metadata_update_batch_timeout must be less than worker_queue_timeout"
            )
        if self.adaptive_min_workers > self.worker_per_proxy:
            raise ValueError("adaptive_min_workers must not exceed worker_per_proxy")
        levels = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3}
        if levels[self.console_log_level] < levels[self.base_log_level]:
            raise ValueError(
                "console_log_level cannot be more verbose than base_log_level"
            )
        return self

    @property
    def save_directory(self) -> Path:
        return self.base_directory / "downloads"

    @property
    def metadata_db_path(self) -> Path:
        return self.base_directory / "metadata.db"

    @property
    def log_file_path(self) -> Path:
        return self.base_directory / "logs.log"


CONFIG = Config()
_ACTIVE_CONFIG: ContextVar[Config | None] = ContextVar(
    "tick_vault_config", default=None
)


def get_config() -> Config:
    """Return the operation snapshot, or the shared user configuration."""
    return _ACTIVE_CONFIG.get() or CONFIG


def snapshot_config() -> Config:
    return get_config().model_copy(deep=True)


@contextmanager
def use_config(settings: Config) -> Iterator[None]:
    token = _ACTIVE_CONFIG.set(settings)
    try:
        yield
    finally:
        _ACTIVE_CONFIG.reset(token)


def reload_config(**kwargs: Any) -> None:
    """Reload defaults/environment plus explicit overrides without stale imports.

    ``env_file`` (or ``_env_file``) chooses a dotenv file; ``None`` disables it.
    Each call reloads all settings. Unknown explicit names fail immediately.
    Running download operations keep their captured configuration. Changes
    apply to subsequently started operations.
    """
    options: dict[str, Any] = {}
    for key, value in kwargs.items():
        name = key.lower()
        if name == "env_file":
            name = "_env_file"
        if name not in Config.model_fields and name != "_env_file":
            raise ValueError(f"Unknown TickVault setting: {key}")
        options[name] = value
    candidate = Config(**options)
    from .logger import setup_logger

    setup_logger(force=True, settings=candidate)
    CONFIG.__dict__.update(candidate.__dict__)
    CONFIG.__pydantic_fields_set__ = candidate.__pydantic_fields_set__.copy()
