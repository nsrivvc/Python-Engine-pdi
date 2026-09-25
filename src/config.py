"""
config.py
=========
All configuration comes from environment variables (loaded from a local .env
if python-dotenv is installed). Nothing is hardcoded.

WHERE TO CHANGE THINGS:
  * Point at a different database  -> DATABASE_URL (or the PG* parts) in .env
  * Rename the raw/curated schemas -> BRONZE_SCHEMA / SILVER_SCHEMA in .env
  * Rename the decomposition-output schema -> DECOMP_SCHEMA in .env
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

try:  # optional convenience for local runs
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


#: The DBAPI this repo installs (requirements.txt: psycopg2-binary). Named in
#: the URL explicitly because SQLAlchemy 2.1 changed what a bare
#: "postgresql://" means -- from psycopg2 to psycopg (v3) -- so a fresh
#: install on a CI runner resolved to a driver that was not there and died
#: with "No module named 'psycopg'". A URL that already names a driver
#: ("postgresql+pg8000://") is left alone.
_DRIVER = "psycopg2"


def _build_url() -> str:
    """Return a SQLAlchemy URL, either from DATABASE_URL or from PG* parts."""
    # .strip(): the repo's DATABASE_URL secret carries a trailing newline
    # (stage 2's connect() strips it for the same reason).
    url = (os.getenv("DATABASE_URL") or "").strip()
    if url:
        # SQLAlchemy wants the "postgresql://" scheme (not "postgres://").
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", f"postgresql+{_DRIVER}://", 1)
        return url

    # Fallback: assemble from individual parts (handy for a local/dummy DB).
    user = os.getenv("PGUSER", "postgres")
    pwd = os.getenv("PGPASSWORD", "postgres")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "pipeline")
    return f"postgresql+{_DRIVER}://{user}:{pwd}@{host}:{port}/{db}"


@dataclass
class Settings:
    database_url: str = field(default_factory=_build_url)
    bronze_schema: str = field(default_factory=lambda: os.getenv("BRONZE_SCHEMA", "bronze"))
    silver_schema: str = field(default_factory=lambda: os.getenv("SILVER_SCHEMA", "silver"))
    # Where the decomposition phase lands its output tables. The rec-del pairing
    # transformations read from here rather than from Bronze. Set DECOMP_SCHEMA
    # once the decomposition phase exists and writes somewhere else.
    decomp_schema: str = field(default_factory=lambda: os.getenv("DECOMP_SCHEMA", "silver_staging"))
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))


# Single shared instance imported across the codebase.
settings = Settings()
