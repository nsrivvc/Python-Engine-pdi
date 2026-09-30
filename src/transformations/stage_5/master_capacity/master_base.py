"""
master_base.py
==============
Shared base for the per-feed master capacity transformations. Not a
transformation itself -- it registers nothing.

WHAT IT DOES
------------
Maps one feed's stage-3 output onto the common master capacity model:

    silver_staging.firm_core       ->  silver.firm_core_master_capacity
    silver_staging.firm_locations  ->  silver.firm_locations_master_capacity
    silver_staging.firm_rates      ->  silver.firm_rates_master_capacity

...and the same for interruptible and awards. The three FINAL transformations
then UNION every feed's table per grain into one consolidated table.

The point of this layer is normalisation: each feed names the same concept
differently (`kqtyk` vs `itqtyk`, `firmid` vs `interruptibleid`), and the master
capacity model names it once. Subclasses supply `column_map` -- target column ->
SQL expression over the source row -- and everything else is mechanical.

A target column with no entry in `column_map` is emitted as NULL of the right
type. That is deliberate: a feed that genuinely has no award number should say so
by omission rather than by inventing a column, and it keeps every feed's table
UNION-compatible without forcing fake values.

ROW FOR ROW, OR LATEST-WINS
---------------------------
By default the mapped rows pass through a latest-wins step per natural key, so
a source holding more than one row per key cannot make the upsert touch the
same target row twice. A subclass whose source is already one row per output
row sets `dedupe = False` and gets exactly the source's rows -- nothing is
collapsed, and a genuine key collision fails the run instead of silently
dropping a row. Every feed's LOCATIONS grain does this: it reads the feed's
stage 4 rec-del table (`silver.<feed>_rec_del_pair`) rather than stage 3, so
stage 5 holds the same rows stage 4 does and can carry the pairing columns
(`group`, `pair_group_id`). Core and rates still read stage 3.

WHERE THE COLUMNS COME FROM
---------------------------
`models.py`, shared with the finals, so the two can never drift apart.
"""

from __future__ import annotations

from typing import Dict, List

from .models import COLUMNS_BY_GRAIN, NATURAL_KEY_BY_GRAIN
from ....core.base import PipelineTransformation

#: Model columns that are SQL keywords ("index" is merely unreserved, "group"
#: is fully reserved) -- every identifier position quotes them.
KEYWORD_COLUMNS = {"index", "group"}


def _q(name: str) -> str:
    return f'"{name}"' if name in KEYWORD_COLUMNS else name


class MasterCapacityTransformation(PipelineTransformation):
    # --- set these in each subclass ------------------------------------------
    feed: str = ""                    # "firm" | "interruptible" | "awards"
    grain: str = ""                   # "core" | "locations" | "rates"
    source_table: str = ""            # stage-3 output in DECOMP_SCHEMA
    column_map: Dict[str, str] = {}   # target column -> SQL expression

    #: Collapse the mapped rows to the latest one per natural key before
    #: writing. Set False when the source is already one row per output row
    #: and every one of them must land (see the module docstring).
    dedupe: bool = True

    #: Optional source expression the rows are written in the order of, so the
    #: target's serial id follows the source's own order. Empty = unordered.
    source_order: str = ""

    def __init__(self) -> None:
        for attr in ("feed", "grain", "source_table"):
            if not getattr(self, attr):
                raise ValueError(f"{type(self).__name__} must set `{attr}`.")
        if self.grain not in COLUMNS_BY_GRAIN:
            raise ValueError(f"{type(self).__name__}: unknown grain {self.grain!r}")
        self.source = self.feed
        self.bronze_sources = [self.source_table]
        super().__init__()

    @property
    def source_schema(self) -> str:
        """Stage 3 writes to DECOMP_SCHEMA; this reads it."""
        return self.decomp_schema

    @property
    def columns(self):
        return COLUMNS_BY_GRAIN[self.grain]

    @property
    def natural_key(self):
        return NATURAL_KEY_BY_GRAIN[self.grain]

    def _expr(self, name: str, sql_type: str) -> str:
        """Source expression for a target column, or a typed NULL if unmapped."""
        return self.column_map.get(name, f"NULL::{sql_type}")

    # ------------------------------------------------------------------ DDL
    def create_table_sql(self) -> str:
        s = self.silver_schema
        cols = ",\n            ".join(f"{_q(n):<38} {t}" for n, t in self.columns)
        key = ", ".join(_q(k) for k in self.natural_key)
        return f"""
        CREATE SCHEMA IF NOT EXISTS {s};

        CREATE TABLE IF NOT EXISTS {s}.{self.table_name} (
            {self.feed}_{self.grain}_id{'':<12} BIGSERIAL PRIMARY KEY,

            {cols},

            silver_loaded_ts                       TIMESTAMPTZ DEFAULT now(),

            CONSTRAINT uq_{self.table_name}
                UNIQUE NULLS NOT DISTINCT ({key})
        );
        """

    # ------------------------------------------------------------ transform
    def transform_sql(self) -> str:
        s = self.silver_schema
        sep = ",\n            "
        names = [n for n, _ in self.columns]
        select = sep.join(
            f"{self._expr(n, t)} AS {_q(n)}" for n, t in self.columns)
        insert = sep.join(_q(n) for n in names)
        updatable = [n for n in names if n not in self.natural_key]
        updates = sep.join(f"{_q(n):<38} = EXCLUDED.{_q(n)}" for n in updatable)
        key = ", ".join(_q(k) for k in self.natural_key)

        # `_ord` rides along only to order the write; it is never inserted.
        order_col = f"{sep}{self.source_order} AS _ord" if self.source_order else ""
        order_by = "ORDER BY _ord" if self.source_order else ""

        if self.dedupe:
            # Latest wins per natural key, so a source holding more than one
            # row per key cannot make the upsert touch the same target row twice.
            dedupe_cte = f""",
        deduped AS (
            SELECT * FROM (
                SELECT m.*, row_number() OVER (
                    PARTITION BY {key}
                    ORDER BY update_date DESC NULLS LAST, posted_date DESC NULLS LAST) AS _rn
                FROM mapped m
            ) x WHERE _rn = 1
        )"""
            rows = "deduped"
        else:
            # Row for row: every source row lands. The ON CONFLICT below then
            # only ever fires on a rerun without --reload; two source rows
            # sharing a key fail the statement rather than losing one.
            dedupe_cte = ""
            rows = "mapped"

        return f"""
        WITH mapped AS (
            SELECT
            {select}{order_col}
            FROM {self.source_schema}.{self.source_table}
        ){dedupe_cte}
        INSERT INTO {s}.{self.table_name} AS tgt (
            {insert}
        )
        SELECT
            {insert}
        FROM {rows}
        {order_by}
        ON CONFLICT ({key}) DO UPDATE SET
            {updates},
            silver_loaded_ts                       = now();
        """
