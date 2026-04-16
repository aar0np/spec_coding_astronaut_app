from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import load_dotenv


class ConfigurationError(ValueError):
    """Raised when required application configuration is missing or invalid."""


@dataclass(frozen=True)
class Settings:
    astra_db_api_endpoint: str
    astra_db_application_token: str
    astra_db_keyspace: str


def _read_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigurationError(
            f"Missing required environment variable: {name}"
        )
    return value


def _validate_endpoint(value: str) -> str:
    if not value.startswith("https://"):
        raise ConfigurationError(
            "ASTRA_DB_API_ENDPOINT must start with 'https://'."
        )
    return value.rstrip("/")


def _validate_token(value: str) -> str:
    if len(value) < 20:
        raise ConfigurationError(
            "ASTRA_DB_APPLICATION_TOKEN appears invalid or too short."
        )
    return value


def _validate_keyspace(value: str) -> str:
    if " " in value:
        raise ConfigurationError(
            "ASTRA_DB_KEYSPACE must not contain spaces."
        )
    return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load, validate, and cache application settings from .env and environment."""
    load_dotenv()

    endpoint = _validate_endpoint(_read_required_env("ASTRA_DB_API_ENDPOINT"))
    token = _validate_token(_read_required_env("ASTRA_DB_APPLICATION_TOKEN"))
    keyspace = _validate_keyspace(_read_required_env("ASTRA_DB_KEYSPACE"))

    return Settings(
        astra_db_api_endpoint=endpoint,
        astra_db_application_token=token,
        astra_db_keyspace=keyspace,
    )

# Made with Bob
