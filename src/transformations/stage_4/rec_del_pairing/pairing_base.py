"""
pairing_base.py
===============
Shared base for the receipt/delivery ("rec-del") pairing transformations in this
package. Not a transformation itself -- it registers nothing.

WHAT THIS DOES
--------------
Each paired transport type (firm, interruptible, awards) produces a *locations*
table at the end of the decomposition phase, holding one row per location with a
purpose flag marking it a receipt (REC) or a delivery (DEL). This base turns that
flat list into one row per receipt->delivery **path**, then hangs the term
transform off each paired row.

    locations (flat)                    rec_del_pair (paths)
    ----------------                    --------------------
    EGTS-F167  70000  REC  25000   ->   EGTS-F167  70000 -> 80000   PAIRED
    EGTS-F167  80000  DEL  25000
    EGTS-F168  70010  REC  12000   ->   EGTS-F168  70010 -> NULL    RECEIPT

A one-sided location is kept, not dropped: `pair_status` is PAIRED when a
receipt and a delivery matched, and RECEIPT or DELIVERY when that side
stands alone -- so a one-sided contract is visible downstream rather than
silently vanishing.

TWO HOOKS ARE DELIBERATELY UNIMPLEMENTED
----------------------------------------
Both are marked `SPEC:` below and currently hold neutral placeholders, so the
pipeline is wired end-to-end and ready for the business rules to drop in:

  1. pair_predicate_sql()  -- how a receipt is matched to a delivery. The
     placeholder pairs every REC with every DEL on the same contract (a cross
     join within the contract). Replace with the real matching rule.

  2. term_columns_sql()    -- the term transform applied to each paired row.
     The placeholder passes the raw term window through as timestamps and leaves
     the derived fields NULL. Replace with the real term derivation.

COLUMN NAMES
------------
The decomposition tables do not exist yet, so `column_map` below declares which
source column backs each logical field, defaulting to the Bronze location
naming (locpurp / loc / locname / loczn / locqti). When the real tables land,
edit `column_map` on the affected subclass -- no SQL changes needed.
"""

from __future__ import annotations

from typing import Dict, Optional

from ....core.base import PipelineTransformation
from ....core.table_config import LocationsSource, RecDelPairing


class RecDelPairingTransformation(PipelineTransformation):
    # --- set these in each subclass ------------------------------------------
    entity: str = ""            # "firm" | "interruptible" | "awards"

    #: Source table in the decomposition schema. Left empty, it resolves from
    #: core/table_config.py (LocationsSource) by entity -- which is where the
    #: per-feed table names live; set it here only to override.
    locations_table: str = ""

    # Which `loc_purpose` values mean receipt vs delivery (compared
    # upper-cased). Configured in core/table_config.py.
    receipt_purpose: str = RecDelPairing.receipt_purpose
    delivery_purpose: str = RecDelPairing.delivery_purpose

    # Logical field -> source column. Override per subclass as the real
    # decomposition tables land with their own names.
    column_map: Dict[str, str] = {
        "contract_key": "firmid",
        "loc_code": "loc",
        # The element's position within the contract's Locations array. The
        # SAME loc code legitimately recurs at different indexes carrying
        # different quantities, so the index is part of what identifies a
        # location. None = the feed has no such column (awards); see `ref`.
        "loc_index": None,
        "loc_name": "locname",
        "loc_zone": "loczn",
        "loc_purpose": "locpurp",
        "loc_qti": "locqti",
        "loc_qty": "kqtyloc",
        "term_begin": "kentbegdatetime",
        "term_end": "kentenddatetime",
        "source_system": "source_system",
        "source_api": "source_api",
        "pipeline_run_id": "pipeline_run_id",
        "hash_key": "hash_key",
    }

    # Optional filter applied when reading the source table (e.g. keep only
    # successfully loaded rows). Configured in core/table_config.py; set to
    # None to read everything.
    source_filter: Optional[str] = RecDelPairing.source_filter

    # Keeps only the newest row per (contract, location, purpose) before pairing.
    # Without this, a re-ingested location appearing twice in the source would
    # make the upsert touch the same target row twice in one statement, which
    # Postgres rejects ("ON CONFLICT DO UPDATE command cannot affect row a
    # second time"). Configured in core/table_config.py; set to None only if
    # the source is already deduplicated.
    dedupe_order: Optional[str] = RecDelPairing.dedupe_order

    def __init__(self) -> None:
        if not self.entity:
            raise ValueError(f"{type(self).__name__} must set an `entity`.")
        if not self.locations_table:
            self.locations_table = LocationsSource.for_feed(self.entity)
        # The runner checks dependencies against this list.
        self.bronze_sources = [self.locations_table]
        # `entity` already names the JSON source feed, so --source filtering
        # works without each subclass repeating itself.
        if not self.source:
            self.source = self.entity
        super().__init__()

    @property
    def source_schema(self) -> str:
        """Read from the decomposition output, not from Bronze."""
        return self.decomp_schema

    def col(self, logical: str) -> str:
        """Source column backing a logical field.

        A mapping of None means the feed genuinely has no such column -- see
        `ref`. Only an ABSENT key is an error.
        """
        try:
            return self.column_map[logical]
        except KeyError:  # pragma: no cover - developer error
            raise KeyError(
                f"{type(self).__name__}.column_map is missing {logical!r}"
            ) from None

    def ref(self, alias: str, logical: str, cast: str = "TEXT") -> str:
        """`alias.column`, or a typed NULL when the feed has no such column.

        Not every feed carries every logical field: the awards locations grain
        has no zone at all, where firm and IT have `loczn`. Mapping it to None
        keeps the output's column contract identical across feeds -- the column
        exists and is NULL -- instead of forcing a fake source column or
        special-casing the SQL per feed.
        """
        column = self.col(logical)
        return f"{alias}.{column}" if column else f"NULL::{cast}"

    # ------------------------------------------------------------------ hooks
    def pair_predicate_sql(self) -> str:
        """SPEC: extra condition deciding whether receipt `r` pairs with delivery `d`.

        Both sides are already constrained to the same contract by the caller,
        so this is the *additional* rule on top of that. It is ANDed into a FULL
        OUTER JOIN, meaning anything that fails it stays in the output as an
        unpaired row rather than disappearing.

        Placeholder is TRUE -> every receipt pairs with every delivery on the
        contract. Replace with the real matching rule, e.g. quantity matching:

            return "NULLIF(r.loc_qty, '')::NUMERIC = NULLIF(d.loc_qty, '')::NUMERIC"
        """
        return "TRUE"

    def term_columns_sql(self) -> str:
        """SPEC: the term transform, as four SQL expressions in this exact order:

            term_begin_ts, term_end_ts, term_days, term_category

        The placeholder casts the source term window through unchanged and
        leaves the derived fields NULL, so the column contract is fixed and the
        table is ready before the rules are written.

        Keep comments *above* expressions, never trailing the last one -- the
        caller appends a separator after this block, and a trailing `--` comment
        would swallow it.
        """
        begin, end = self.col("term_begin"), self.col("term_end")
        return f"""
            -- term_begin_ts / term_end_ts: raw window, passed through
            NULLIF(COALESCE(r.{begin}, d.{begin}), '')::TIMESTAMPTZ,
            NULLIF(COALESCE(r.{end}, d.{end}), '')::TIMESTAMPTZ,
            -- SPEC: derive term_days
            NULL::INTEGER,
            -- SPEC: derive term_category
            NULL::TEXT"""

    # ------------------------------------------------------------------ DDL
    def create_table_sql(self) -> str:
        s, e = self.silver_schema, self.entity
        return f"""
        CREATE SCHEMA IF NOT EXISTS {s};

        CREATE TABLE IF NOT EXISTS {s}.{self.table_name} (
            rec_del_pair_id        BIGSERIAL PRIMARY KEY,

            entity_type            TEXT NOT NULL,
            contract_key           TEXT NOT NULL,
            pair_status            TEXT NOT NULL,  -- PAIRED | RECEIPT | DELIVERY

            -- receipt side
            receipt_loc_code       TEXT,
            receipt_loc_index      TEXT,
            receipt_loc_name       TEXT,
            receipt_zone           TEXT,
            receipt_qti            TEXT,
            receipt_qty_dth        NUMERIC,

            -- delivery side
            delivery_loc_code      TEXT,
            delivery_loc_index     TEXT,
            delivery_loc_name      TEXT,
            delivery_zone          TEXT,
            delivery_qti           TEXT,
            delivery_qty_dth       NUMERIC,

            -- SPEC: quantity reconciliation across the path
            path_qty_dth           NUMERIC,

            -- term transform outputs (see term_columns_sql)
            term_begin_ts          TIMESTAMPTZ,
            term_end_ts            TIMESTAMPTZ,
            term_days              INTEGER,
            term_category          TEXT,

            -- lineage
            source_system          TEXT,
            source_api             TEXT,
            pipeline_run_id        TEXT,
            receipt_hash_key       TEXT,
            delivery_hash_key      TEXT,
            silver_loaded_ts       TIMESTAMPTZ DEFAULT now(),

            -- NULLS NOT DISTINCT (PG15+) so unpaired rows, where one side is
            -- NULL, still collide on rerun instead of duplicating.
            CONSTRAINT uq_{e}_rec_del_pair
                UNIQUE NULLS NOT DISTINCT (contract_key, receipt_loc_code, receipt_loc_index,
                                           delivery_loc_code, delivery_loc_index)
        );
        """

    # ------------------------------------------------------------ transform
    def transform_sql(self) -> str:
        s = self.silver_schema
        src = f"{self.source_schema}.{self.locations_table}"
        where = f"WHERE {self.source_filter}" if self.source_filter else ""

        key = self.col("contract_key")
        code, nm = self.col("loc_code"), self.col("loc_name")
        # Zone is optional per feed (awards has none) -- see ref().
        zn_r, zn_d = self.ref("r", "loc_zone"), self.ref("d", "loc_zone")
        # Optional per feed, exactly like zone: awards carries no Index.
        ix_r, ix_d = self.ref("r", "loc_index"), self.ref("d", "loc_index")
        purp, qti, qty = self.col("loc_purpose"), self.col("loc_qti"), self.col("loc_qty")
        sys_, api = self.col("source_system"), self.col("source_api")
        run, hsh = self.col("pipeline_run_id"), self.col("hash_key")

        # Feeds that carry an Index dedupe per (contract, loc, purpose, index):
        # one loc code recurring at several indexes is several locations, not a
        # reloaded duplicate. Feeds without one keep the old three-part grain.
        idx_col = self.col("loc_index")
        idx_part = f", {idx_col}" if idx_col else ""

        if self.dedupe_order:
            base = f"""
        deduped AS (
            SELECT * FROM (
                SELECT s.*, row_number() OVER (
                    PARTITION BY {key}, {code}, upper({purp}){idx_part}
                    ORDER BY {self.dedupe_order}) AS _rn
                FROM src s
            ) x WHERE _rn = 1
        ),"""
            pool = "deduped"
        else:
            base = ""
            pool = "src"

        return f"""
        WITH src AS (
            SELECT * FROM {src}
            {where}
        ),{base}
        rec AS (
            SELECT * FROM {pool} WHERE upper({purp}) = '{self.receipt_purpose}'
        ),
        del AS (
            SELECT * FROM {pool} WHERE upper({purp}) = '{self.delivery_purpose}'
        )
        INSERT INTO {s}.{self.table_name} AS tgt (
            entity_type, contract_key, pair_status,
            receipt_loc_code, receipt_loc_index, receipt_loc_name, receipt_zone, receipt_qti, receipt_qty_dth,
            delivery_loc_code, delivery_loc_index, delivery_loc_name, delivery_zone, delivery_qti, delivery_qty_dth,
            path_qty_dth,
            term_begin_ts, term_end_ts, term_days, term_category,
            source_system, source_api, pipeline_run_id,
            receipt_hash_key, delivery_hash_key
        )
        SELECT
            '{self.entity}',
            COALESCE(r.{key}, d.{key}),
            CASE
                WHEN r.{key} IS NULL THEN 'DELIVERY'
                WHEN d.{key} IS NULL THEN 'RECEIPT'
                ELSE 'PAIRED'
            END,

            r.{code}, {ix_r}, r.{nm}, {zn_r}, r.{qti}, NULLIF(r.{qty}, '')::NUMERIC,
            d.{code}, {ix_d}, d.{nm}, {zn_d}, d.{qti}, NULLIF(d.{qty}, '')::NUMERIC,

            -- SPEC: reconcile receipt vs delivery qty into path_qty_dth
            NULL::NUMERIC,

            {self.term_columns_sql().strip()}

            -- Leading commas below: term_columns_sql() is overridable and may
            -- end in a comment, which would swallow a trailing separator.
            , COALESCE(r.{sys_}, d.{sys_})
            , COALESCE(r.{api}, d.{api})
            , COALESCE(r.{run}, d.{run})
            , r.{hsh}
            , d.{hsh}
        FROM rec r
        FULL OUTER JOIN del d
              ON r.{key} = d.{key}
             AND ({self.pair_predicate_sql()})
        ON CONFLICT (contract_key, receipt_loc_code, receipt_loc_index,
                     delivery_loc_code, delivery_loc_index) DO UPDATE SET
            entity_type            = EXCLUDED.entity_type,
            pair_status            = EXCLUDED.pair_status,
            receipt_loc_name       = EXCLUDED.receipt_loc_name,
            receipt_zone           = EXCLUDED.receipt_zone,
            receipt_qti            = EXCLUDED.receipt_qti,
            receipt_qty_dth        = EXCLUDED.receipt_qty_dth,
            delivery_loc_name      = EXCLUDED.delivery_loc_name,
            delivery_zone          = EXCLUDED.delivery_zone,
            delivery_qti           = EXCLUDED.delivery_qti,
            delivery_qty_dth       = EXCLUDED.delivery_qty_dth,
            path_qty_dth           = EXCLUDED.path_qty_dth,
            term_begin_ts          = EXCLUDED.term_begin_ts,
            term_end_ts            = EXCLUDED.term_end_ts,
            term_days              = EXCLUDED.term_days,
            term_category          = EXCLUDED.term_category,
            source_system          = EXCLUDED.source_system,
            source_api             = EXCLUDED.source_api,
            pipeline_run_id        = EXCLUDED.pipeline_run_id,
            receipt_hash_key       = EXCLUDED.receipt_hash_key,
            delivery_hash_key      = EXCLUDED.delivery_hash_key,
            silver_loaded_ts       = now();
        """
