"""
final_base.py
=============
Shared base for the three FINAL master capacity transformations. Not a
transformation itself -- it registers nothing.

WHAT THESE DO
-------------
Each of the four feeds produces its own per-grain master capacity table. The
FINAL tables tie all four into one consolidated table per grain:

    firm_core_master_capacity           \\
    interruptible_core_master_capacity   >--- UNION ALL ---> final_core_master_capacity
    awards_core_master_capacity         /
    ioc_core_master_capacity           /

...and the same shape again for `locations` and `rates`. Every consolidated row
carries a `source_type` column naming the feed it came from, so a consolidated
table can always be split back apart.

THE CONSOLIDATION ASSUMPTION
----------------------------
A UNION only works if the four per-feed tables share a column set at a given
grain. That is the assumption baked in here: per-feed tables are the same shape,
and the final adds `source_type`. If feeds end up differing, the per-feed
projection is the place to reconcile them -- see `projection_sql`.

HOOKS LEFT FOR THE BUSINESS RULES
---------------------------------
All marked `SPEC:` and currently holding neutral placeholders, so the tables and
the wiring exist before the rules are written:

  1. `columns`        the consolidated column set for this grain.
  2. `natural_key`    what makes a consolidated row unique (drives the upsert).
  3. `projection_sql` how one feed's rows map onto that column set. The
     placeholder assumes identical column names across feeds.
  4. `dedupe_note`    what to do when the same contract appears in more than one
     feed. The placeholder keeps both rows, distinguished by `source_type`.

CONSOLIDATES WHATEVER RAN
-------------------------
A workflow in the orchestration interface can pull any subset of sources -- Firm
alone, Firm + IT, all four. So these do NOT require every feed. At run time they
look up which per-feed tables actually exist and union exactly those, naming the
absent ones in the log.

Requiring all four would leave the FINAL tables permanently blocked for any
workflow that pulls a subset, which is the normal case rather than the exception.
With no feeds present at all, there is nothing to consolidate: it logs and
returns 0 rather than failing, because the workflow is simply ahead of its
inputs.

STALE PER-FEED TABLES
---------------------
"The table exists" is the proxy for "this feed ran", and it has a hole: a
per-feed table is only ever rebuilt by its own feed's stage 5 run (the
workflows pass --reload, which drops and recreates it). Change the shared model
in models.py and every per-feed table keeps its old shape until its feed
reruns. A feed that is not selected in the current workflow therefore leaves
behind a table in the OLD shape, and this UNION would select a column it does
not have. So `run()` compares each present table's columns to the model first:

  * stale and EMPTY  -> left out of the UNION with a warning. It contributes
                        nothing either way, so the final truthfully holds just
                        the feeds that have data (e.g. firm alone). It is
                        rebuilt the next time its own feed runs.
  * stale WITH rows  -> fail, naming the table, the missing columns and the
                        feed to rerun. Quietly leaving real rows out would
                        publish a final missing a whole feed and exit green,
                        which is exactly the hazard the finals workflow exists
                        to prevent.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

from ..master_base import _q
from .....core.base import PipelineTransformation
from .....db.connection import row_count, table_columns, table_exists
from .....logging_config import get_logger

log = get_logger(__name__)

#: The source feeds that can be consolidated, matching the toggles in the
#: orchestration interface: Firm, Interruptible, Awards, Index of Customers.
#: A feed listed here that has not run is simply absent from the UNION -- see
#: `run()` below -- so this can safely list every feed that MIGHT exist.
FEEDS = ("firm", "interruptible", "awards", "index")


class FinalMasterCapacityTransformation(PipelineTransformation):
    # --- set these in each subclass ------------------------------------------
    grain: str = ""                 # "core" | "locations" | "rates"

    # SPEC: the consolidated column set, as (column_name, sql_type) pairs.
    # `source_type` is added automatically and must not be repeated here.
    columns: List[Tuple[str, str]] = []

    # SPEC: what makes a consolidated row unique. Drives the UNIQUE constraint
    # and the ON CONFLICT upsert, so it must not be empty.
    natural_key: Tuple[str, ...] = ()

    # SPEC: how duplicates across feeds are resolved. Documentation only today.
    dedupe_note: str = "keep every feed's row, distinguished by source_type"

    # These are cross-feed by nature, so `source` stays empty and they file
    # under `_combined` rather than any one feed.
    #: A FINAL table consolidates whatever ran, rather than requiring
    #: every feed -- the interface allows pulling any subset.
    sources_required = False

    source: str = ""

    def __init__(self) -> None:
        if not self.grain:
            raise ValueError(f"{type(self).__name__} must set a `grain`.")
        if not self.columns:
            raise ValueError(f"{type(self).__name__} must set `columns`.")
        if not self.natural_key:
            raise ValueError(f"{type(self).__name__} must set a `natural_key`.")
        self.bronze_sources = list(self.source_tables.values())
        super().__init__()

    @property
    def source_tables(self) -> Dict[str, str]:
        """Feed -> its per-grain table name. Trim this to consolidate a subset."""
        return {feed: f"{feed}_{self.grain}_master_capacity" for feed in FEEDS}

    @property
    def source_schema(self) -> str:
        """The per-feed tables are Silver output, not Bronze."""
        return self.silver_schema

    # ------------------------------------------------------------------ hooks
    def projection_sql(self, feed: str, table: str) -> str:
        """SPEC: map one feed's rows onto the consolidated column set.

        The placeholder assumes every feed already uses the same column names,
        so it selects them straight through. When a feed differs, override this
        and do the renaming/casting here rather than in the target DDL.
        """
        cols = ",\n                   ".join(_q(name) for name, _ in self.columns)
        return f"""
            SELECT '{feed}' AS source_type,
                   {cols}
            FROM {self.source_schema}.{table}
        """

    # ------------------------------------------------------------------ DDL
    def create_table_sql(self) -> str:
        s = self.silver_schema
        cols = ",\n            ".join(f"{_q(name):<34} {sql_type}" for name, sql_type in self.columns)
        key = ", ".join(_q(k) for k in self.natural_key)
        return f"""
        CREATE SCHEMA IF NOT EXISTS {s};

        CREATE TABLE IF NOT EXISTS {s}.{self.table_name} (
            final_{self.grain}_id              BIGSERIAL PRIMARY KEY,

            -- which feed this row was consolidated from
            source_type                        TEXT NOT NULL,

            {cols},

            silver_loaded_ts                   TIMESTAMPTZ DEFAULT now(),

            -- Dedupe rule: {self.dedupe_note}
            -- NULLS NOT DISTINCT (PG15+) so rows with a NULL key part still
            -- collide on rerun instead of duplicating.
            CONSTRAINT uq_{self.table_name}
                UNIQUE NULLS NOT DISTINCT ({key})
        );
        """

    # ------------------------------------------------------------ transform
    def transform_sql(self) -> str:
        s = self.silver_schema
        col_names = [name for name, _ in self.columns]
        insert_cols = ",\n            ".join(["source_type", *(_q(c) for c in col_names)])

        # Only the feeds detected at run time by run(). Falls back to every known
        # feed so `--show-sql` still renders something meaningful offline.
        active = getattr(self, "_active_sources", None) or self.source_tables
        union = "\n            UNION ALL\n".join(
            self.projection_sql(feed, table).strip()
            for feed, table in active.items()
        )

        # Everything except the natural key is refreshed on conflict.
        # NB: these are built outside the f-string — backslashes are not allowed
        # inside f-string expressions before Python 3.12, and CI pins 3.11.
        sep = ",\n            "
        updatable = [c for c in col_names if c not in self.natural_key]
        updates = sep.join(f"{_q(c):<34} = EXCLUDED.{_q(c)}" for c in updatable)
        updates = updates + sep if updates else ""
        select_cols = sep.join(_q(c) for c in col_names)
        key_cols = ", ".join(_q(k) for k in self.natural_key)

        return f"""
        WITH consolidated AS (
            {union}
        )
        INSERT INTO {s}.{self.table_name} AS tgt (
            {insert_cols}
        )
        SELECT
            source_type,
            {select_cols}
        FROM consolidated
        ON CONFLICT ({key_cols}) DO UPDATE SET
            {updates}silver_loaded_ts                   = now();
        """

    # ------------------------------------------------------------------ run
    def run(self, conn) -> int:
        """Consolidate whichever feeds are present and usable, not all of them.

        A workflow in the orchestration interface can pull any subset of sources
        -- Firm only, Firm + IT, all four. Whatever ran produced its per-feed
        master capacity table; whatever did not, did not. So this looks up which
        of those tables actually exist and unions exactly those -- minus any
        table that is empty AND predates the current model, which cannot be
        selected from and has nothing to contribute (see the module docstring).

        The alternative -- requiring all four -- would leave the FINAL tables
        permanently blocked for any workflow that pulls a subset, which is the
        normal case rather than the exception.

        With nothing usable present there is nothing to consolidate, so it logs
        and returns 0 rather than failing: the workflow is legitimately ahead of
        its inputs.
        """
        present = {
            feed: table
            for feed, table in self.source_tables.items()
            if table_exists(conn, self.source_schema, table)
        }
        absent = [f for f in self.source_tables if f not in present]
        usable = self._usable_sources(conn, present)
        left_out = [f for f in present if f not in usable]
        if not usable:
            log.warning(
                "[%s] nothing to consolidate: no usable per-feed table (looked for %s in %s%s)",
                self.name, ", ".join(self.source_tables.values()), self.source_schema,
                f"; left out as empty and stale: {', '.join(left_out)}" if left_out else "")
            return 0
        log.info("[%s] consolidating %d feed(s): %s%s%s", self.name, len(usable),
                 ", ".join(usable),
                 f"  (absent: {', '.join(absent)})" if absent else "",
                 f"  (left out, empty and stale: {', '.join(left_out)})" if left_out else "")
        self._active_sources = usable
        try:
            return super().run(conn)
        finally:
            self._active_sources = None

    def stale_sources(self, conn, present: Dict[str, str]) -> Dict[str, List[str]]:
        """Per-feed tables that lack model columns: feed -> missing column names.

        A table built under an older models.py keeps that shape until its feed
        reruns with --reload; the UNION would then select a column it does not
        have. Extra columns (the BIGSERIAL id, silver_loaded_ts) are fine --
        only model columns the table is missing count.
        """
        wanted = [name for name, _ in self.columns]
        stale: Dict[str, List[str]] = {}
        for feed, table in present.items():
            have = set(table_columns(conn, self.source_schema, table))
            lacking = [c for c in wanted if c not in have]
            if lacking:
                stale[feed] = lacking
        return stale

    def _usable_sources(self, conn, present: Dict[str, str]) -> Dict[str, str]:
        """`present` minus stale-and-empty tables; fails on a stale table with rows."""
        stale = self.stale_sources(conn, present)
        blocking: Dict[str, str] = {}
        usable = dict(present)
        for feed, cols in stale.items():
            table = f"{self.source_schema}.{present[feed]}"
            n = row_count(conn, self.source_schema, present[feed])
            if n == 0:
                log.warning(
                    "[%s] leaving %s out: %s predates the current model (lacks %s) "
                    "but is empty, so nothing is lost; it is rebuilt the next time "
                    "%s runs", self.name, feed, table, ", ".join(cols), feed)
                del usable[feed]
            else:
                blocking[feed] = f"{table} has {n} rows but lacks {', '.join(cols)}"
        if blocking:
            raise RuntimeError(
                f"{len(blocking)} per-feed {self.grain} table(s) hold rows in a shape "
                f"that predates the current model (stage_5/master_capacity/models.py) "
                f"and cannot be consolidated: {'; '.join(blocking.values())}. "
                f"A per-feed table is only rebuilt by its own feed's stage 5 run with "
                f"--reload; rerun stage 5 {self.grain} for: {', '.join(blocking)}, then this."
            )
        return usable
