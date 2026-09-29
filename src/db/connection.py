"""
connection.py
=============
The ONLY module that imports the database driver. Everything else works with
SQLAlchemy Connection objects (or plain SQL strings), so swapping drivers or
databases later is a change confined to this file.

SQLAlchemy is imported lazily inside get_engine() so that commands which don't
touch the database (run.py --list / --show-sql) work even without the driver
installed.
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, List

from ..config import settings
from ..logging_config import get_logger

if TYPE_CHECKING:  # for type hints only; not imported at runtime
    from sqlalchemy.engine import Engine

log = get_logger(__name__)


@lru_cache(maxsize=1)
def get_engine() -> "Engine":
    """Create (once) and return a pooled SQLAlchemy engine from settings."""
    from sqlalchemy import create_engine

    # pool_pre_ping avoids stale connections on serverless Postgres (e.g. Neon
    # scale-to-zero). future=True selects SQLAlchemy 2.0 semantics.
    engine = create_engine(
        settings.database_url,
        pool_pre_ping=True,
        future=True,
    )
    log.debug("Created SQLAlchemy engine for %s", engine.url.render_as_string(hide_password=True))
    return engine


def table_exists(conn, schema: str, table: str) -> bool:
    """Return True if schema.table exists. Used for pre-run dependency checks."""
    from sqlalchemy import text

    row = conn.execute(
        text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = :s AND table_name = :t"
        ),
        {"s": schema, "t": table},
    ).first()
    return row is not None


def table_columns(conn, schema: str, table: str) -> List[str]:
    """Column names of schema.table in ordinal order; empty if it does not exist.

    Used by the master capacity finals to check that a per-feed table still
    matches the shared model before UNIONing it -- a table created under an
    older model keeps its old shape until its feed reruns with --reload.
    """
    from sqlalchemy import text

    rows = conn.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = :s AND table_name = :t "
            "ORDER BY ordinal_position"
        ),
        {"s": schema, "t": table},
    ).all()
    return [r[0] for r in rows]
