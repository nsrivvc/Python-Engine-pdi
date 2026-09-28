"""
pairing_base.py
===============
Shared base for the receipt/delivery ("rec-del") pairing transformations in this
package. Not a transformation itself -- it registers nothing.

WHAT THIS DOES
--------------
Each paired transport type (firm, interruptible, awards) produces a *locations*
table at the end of the decomposition phase, holding one row per location with a
standardized purpose marking it a receipt or a delivery (anything else -- storage
withdrawal / injection / area -- takes no part). This base

  1. classes every receipt/delivery location R or D and spells each contract's
     FORMATION in location order ("R-D", "R-R-R-D-D-D", "R-D-R-D");
  2. finds the configured pattern that admits that formation (the dashboard's
     Rec-Del Pairings table -- RecDelPairing in core/table_config.py);
  3. writes ONE ROW PER LOCATION, carrying the contract's formation, the
     admitting pattern, a PAIRED / FAIL status, and a `pair_set_id` that
     restarts at 1 for every contract and is shared by the locations that
     belong together.

    locations (flat)                  rec_del_pair (one row per location)
    ----------------                  -----------------------------------
    F24  idx1 S9 WHARTON     R   ->   F24  R-D      PAIRED  set 1  pos 1  R  WHARTON
    F24  idx2 S8 ALGONQUIN   D   ->   F24  R-D      PAIRED  set 1  pos 2  D  ALGONQUIN
    F2   idx1 S9 ...         R   ->   F2   R-D-R-D  PAIRED  set 1  pos 1  R
    F2   idx2 S8 ...         D   ->                         set 1  pos 2  D
    F2   idx3 S9 ...         R   ->                         set 2  pos 3  R
    F2   idx4 S8 ...         D   ->                         set 2  pos 4  D

So a contract with one receipt and one delivery is TWO rows (set 1, 1), and ten
such contracts are twenty rows -- never one wide row per contract.

HOW THE SET ID IS ASSIGNED
--------------------------
The admitting pattern's regex says how a formation breaks into sets:

  * A regex of the form ^(X)+$ -- e.g. ^(R-D)+$ or ^(R-D-R)+$ -- repeats a
    fixed UNIT. Each repetition is one set, so R-D-R-D under ^(R-D)+$ reads
    1,1,2,2 ("first goes with second, third with fourth").
  * Any other regex -- e.g. ^R(?:-R)*-D(?:-D)*$ (n receipts feeding m
    deliveries) or ^R(?:-R)*$ (receipts only) -- admits the formation as ONE
    set, so R-R-R-D-D-D reads 1,1,1,1,1,1 and a lone R reads 1.

The dashes are cosmetic. Both the formation and the pattern regex have their
"-" removed before matching, so "R-D-R-D" is tested as "RDRD" against
"^(RD)+$". Without that, ^(R-D)+$ could only ever match a single "R-D" (the
next character in "R-D-R-D" is a dash, not an R), and every alternating
contract failed. A pattern may therefore be written with or without dashes.

A contract no pattern admits still appears: every one of its location rows is
written with pair_status FAIL, pair_set_id NULL and pairing_pattern NULL, so
nothing in scope vanishes from stage 4. With no patterns configured at all
every formation is admitted as one set (same convention as the pipeline
register).

ONE HOOK IS DELIBERATELY UNIMPLEMENTED
--------------------------------------
term_columns_sql() -- the term transform applied to each row, marked `SPEC:`
below. The placeholder passes the raw term window through as timestamps and
leaves the derived fields NULL, so the column contract is fixed and the table
is ready before the rule is written.

COLUMN NAMES
------------
`column_map` below declares which source column backs each logical field,
defaulting to the gTRAN location naming (locpurp / locpurpdesc / loc / locname /
loczn / locqti). A subclass overrides entries for a feed spelled differently
(awards) -- no SQL changes needed.
"""

from __future__ import annotations

from typing import Dict, Optional

from ....core.base import PipelineTransformation
from ....core.table_config import LocationsSource, RecDelPairing

#: Recognises a pattern regex of the form ^(X)+$ -- after its dashes are
#: removed -- and captures X, the repeated set unit ("RD", "RDR"); its length
#: is the number of locations per set. Postgres ARE syntax: \^ \( \) \+ \$
#: are literal characters of the pattern regex being inspected.
_UNIT_RE = r"^\^\(([RD]+)\)\+\$$"


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
        # location, and it is the order the formation is spelled in. None =
        # the feed has no such column (awards); see `ref`.
        "loc_index": None,
        "loc_name": "locname",
        "loc_zone": "loczn",
        # The raw code the feed posted (M2 / MQ / S8 / S9) -- carried through
        # untouched so a row can be traced back to the source contract.
        "loc_purpose_code": "locpurp",
        # The value classified as receipt / delivery: the standardized
        # description, which is what standardization(p4) rewrites on a
        # pipeline's segment ends (Transco S8 / S9).
        "loc_purpose": "locpurpdesc",
        "tsp_duns": "tspduns",
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

    # Keeps only the newest row per (contract, location, purpose, index) before
    # pairing. Without this, a re-ingested location appearing twice in the
    # source would make the upsert touch the same target row twice in one
    # statement, which Postgres rejects ("ON CONFLICT DO UPDATE command cannot
    # affect row a second time"). Configured in core/table_config.py; set to
    # None only if the source is already deduplicated.
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
    def term_columns_sql(self) -> str:
        """SPEC: the term transform, as four SQL expressions in this exact order:

            term_begin_ts, term_end_ts, term_days, term_category

        `l` is the location row being written. The placeholder casts the source
        term window through unchanged and leaves the derived fields NULL, so
        the column contract is fixed and the table is ready before the rules
        are written.

        Keep comments *above* expressions, never trailing the last one -- the
        caller appends a separator after this block, and a trailing `--` comment
        would swallow it.
        """
        begin, end = self.col("term_begin"), self.col("term_end")
        return f"""
            -- term_begin_ts / term_end_ts: raw window, passed through
            NULLIF(l.{begin}, '')::TIMESTAMPTZ,
            NULLIF(l.{end}, '')::TIMESTAMPTZ,
            -- SPEC: derive term_days
            NULL::INTEGER,
            -- SPEC: derive term_category
            NULL::TEXT"""

    # ------------------------------------------------------------------ DDL
    def create_table_sql(self) -> str:
        s, e = self.silver_schema, self.entity
        return f"""
        CREATE SCHEMA IF NOT EXISTS {s};
        {RecDelPairing.pattern_ddl}
        CREATE TABLE IF NOT EXISTS {s}.{self.table_name} (
            -- Surrogate row id only. The PAIR is identified by
            -- (contract_key, pair_set_id) below, never by this number.
            rec_del_row_id         BIGSERIAL PRIMARY KEY,

            entity_type            TEXT NOT NULL,
            contract_key           TEXT NOT NULL,
            tsp_duns               TEXT,

            -- the contract's R/D formation in location order ("R-D",
            -- "R-R-R-D-D-D") and the configured pattern that admitted it
            -- (NULL = nothing admitted it -> FAIL)
            formation              TEXT NOT NULL,
            pairing_pattern        TEXT,
            pair_status            TEXT NOT NULL,   -- PAIRED | FAIL

            -- which set within the contract this location belongs to. Restarts
            -- at 1 for every contract; rows sharing a value are one pairing.
            -- NULL on FAIL rows.
            pair_set_id            INTEGER,
            -- this location's 1-based position among the contract's R/D rows
            rd_position            INTEGER NOT NULL,
            rd                     TEXT NOT NULL,   -- R | D
            rd_purpose             TEXT NOT NULL,   -- RECEIPT | DELIVERY

            -- the location itself
            loc_index              TEXT,
            loc_code               TEXT,
            loc_name               TEXT,
            loc_zone               TEXT,
            loc_purpose_code       TEXT,            -- raw feed code: M2 / MQ / S8 / S9
            loc_purpose            TEXT,            -- standardized value classified on
            loc_qti                TEXT,
            loc_qty_dth            NUMERIC,

            -- term transform outputs (see term_columns_sql)
            term_begin_ts          TIMESTAMPTZ,
            term_end_ts            TIMESTAMPTZ,
            term_days              INTEGER,
            term_category          TEXT,

            -- lineage
            source_system          TEXT,
            source_api             TEXT,
            pipeline_run_id        TEXT,
            hash_key               TEXT,
            silver_loaded_ts       TIMESTAMPTZ DEFAULT now(),

            -- NULLS NOT DISTINCT (PG15+) so a feed with no loc_index (awards)
            -- still collides on rerun instead of duplicating.
            CONSTRAINT uq_{e}_rec_del_pair
                UNIQUE NULLS NOT DISTINCT (contract_key, loc_code, loc_index, rd)
        );
        """

    # ------------------------------------------------------------ transform
    def transform_sql(self) -> str:
        s = self.silver_schema
        src = f"{self.source_schema}.{self.locations_table}"
        where = f"WHERE {self.source_filter}" if self.source_filter else ""

        key = self.col("contract_key")
        code, nm = self.col("loc_code"), self.col("loc_name")
        purp, qti, qty = self.col("loc_purpose"), self.col("loc_qti"), self.col("loc_qty")
        sys_, api = self.col("source_system"), self.col("source_api")
        run, hsh = self.col("pipeline_run_id"), self.col("hash_key")
        # Optional per feed -- a typed NULL when the feed has no such column.
        zone = self.ref("l", "loc_zone")
        index = self.ref("l", "loc_index")
        duns = self.ref("l", "tsp_duns")
        raw_code = self.ref("l", "loc_purpose_code")

        idx_col = self.col("loc_index")
        idx_part = f", {idx_col}" if idx_col else ""
        order_col = idx_col or code     # location order within a contract

        pt = RecDelPairing

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
        -- Every location classed R (receipt), D (delivery) or neither, by its
        -- standardized purpose (RecDelPairing in core/table_config.py).
        pool AS (
            SELECT p.*,
                   CASE WHEN upper(btrim(coalesce(p.{purp}, ''))) = '{self.receipt_purpose}'  THEN 'R'
                        WHEN upper(btrim(coalesce(p.{purp}, ''))) = '{self.delivery_purpose}' THEN 'D'
                   END AS rd
            FROM {pool} p
        ),
        -- The receipt/delivery locations only, each with its 1-based position
        -- within its contract in location order. Anything else takes no part.
        ordered AS (
            SELECT p.*,
                   row_number() OVER (PARTITION BY p.{key}
                                      ORDER BY p.{order_col}, p.{code}) AS rd_position
            FROM pool p
            WHERE p.rd IS NOT NULL
        ),
        -- One row per contract: its R/D letters in location order, e.g. R-D.
        formation AS (
            SELECT l.{key} AS contract_key,
                   max({duns}) AS tsp_duns,
                   string_agg(l.rd, '-' ORDER BY l.rd_position) AS formation
            FROM ordered l
            GROUP BY l.{key}
        ),
        -- The configured patterns (dashboard Configuration tab -> Rec-Del
        -- Pairings). Dashes are stripped from the regex (they are stripped
        -- from the formation too, below), and the set UNIT the regex repeats
        -- is read off it: ^(RD)+$ is 2 locations per set, ^(RDR)+$ is 3, and
        -- any other shape (NULL) means the whole formation is one set.
        patterns AS (
            SELECT id,
                   "{pt.duns_col}"    AS duns,
                   "{pt.order_col}"   AS ord,
                   "{pt.pattern_col}" AS pattern,
                   replace("{pt.regex_col}", '-', '') AS regex,
                   char_length((regexp_match(replace("{pt.regex_col}", '-', ''), '{_UNIT_RE}'))[1]) AS unit_len
            FROM {pt.pattern_table}
            WHERE NULLIF(btrim("{pt.regex_col}"), '') IS NOT NULL
        ),
        -- The pattern that admits each formation: the pipeline's own rows
        -- (DUNS = the contract's TSP, leading zeros ignored) before the
        -- 'default' rows (DUNS 0), then by Order. NULL when nothing admits it.
        admitted AS (
            SELECT f.*, m.pattern AS pairing_pattern, m.unit_len
            FROM formation f
            LEFT JOIN LATERAL (
                SELECT p.pattern, p.unit_len
                  FROM patterns p
                 WHERE (p.duns = 0
                        OR p.duns::text = ltrim(regexp_replace(coalesce(f.tsp_duns, ''), '[^0-9]', '', 'g'), '0'))
                   AND replace(f.formation, '-', '') ~ p.regex
                 ORDER BY (p.duns <> 0) DESC, p.ord NULLS LAST, p.id
                 LIMIT 1
            ) m ON TRUE
        ),
        -- With no patterns configured at all, every formation is admitted.
        gate AS (
            SELECT NOT EXISTS (SELECT 1 FROM patterns) AS no_patterns
        )
        INSERT INTO {s}.{self.table_name} AS tgt (
            entity_type, contract_key, tsp_duns,
            formation, pairing_pattern, pair_status,
            pair_set_id, rd_position, rd, rd_purpose,
            loc_index, loc_code, loc_name, loc_zone, loc_purpose_code, loc_purpose, loc_qti, loc_qty_dth,
            term_begin_ts, term_end_ts, term_days, term_category,
            source_system, source_api, pipeline_run_id, hash_key
        )
        SELECT
            '{self.entity}',
            l.{key},
            {duns},

            a.formation,
            a.pairing_pattern,
            CASE WHEN a.pairing_pattern IS NOT NULL OR g.no_patterns THEN 'PAIRED' ELSE 'FAIL' END,

            -- set id: position within the contract folded by the pattern's
            -- unit; one set for the whole formation when there is no unit.
            CASE WHEN a.pairing_pattern IS NULL AND NOT g.no_patterns THEN NULL
                 WHEN a.unit_len IS NULL                               THEN 1
                 ELSE ((l.rd_position - 1) / a.unit_len) + 1
            END,
            l.rd_position,
            l.rd,
            CASE l.rd WHEN 'R' THEN 'RECEIPT' ELSE 'DELIVERY' END,

            {index}, l.{code}, l.{nm}, {zone}, {raw_code}, l.{purp}, l.{qti}, NULLIF(l.{qty}, '')::NUMERIC,

            {self.term_columns_sql().strip()}

            -- Leading commas below: term_columns_sql() is overridable and may
            -- end in a comment, which would swallow a trailing separator.
            , l.{sys_}
            , l.{api}
            , l.{run}
            , l.{hsh}
        FROM ordered l
        JOIN admitted a ON a.contract_key = l.{key}
        CROSS JOIN gate g
        ON CONFLICT (contract_key, loc_code, loc_index, rd) DO UPDATE SET
            entity_type            = EXCLUDED.entity_type,
            tsp_duns               = EXCLUDED.tsp_duns,
            formation              = EXCLUDED.formation,
            pairing_pattern        = EXCLUDED.pairing_pattern,
            pair_status            = EXCLUDED.pair_status,
            pair_set_id            = EXCLUDED.pair_set_id,
            rd_position            = EXCLUDED.rd_position,
            rd_purpose             = EXCLUDED.rd_purpose,
            loc_name               = EXCLUDED.loc_name,
            loc_zone               = EXCLUDED.loc_zone,
            loc_purpose_code       = EXCLUDED.loc_purpose_code,
            loc_purpose            = EXCLUDED.loc_purpose,
            loc_qti                = EXCLUDED.loc_qti,
            loc_qty_dth            = EXCLUDED.loc_qty_dth,
            term_begin_ts          = EXCLUDED.term_begin_ts,
            term_end_ts            = EXCLUDED.term_end_ts,
            term_days              = EXCLUDED.term_days,
            term_category          = EXCLUDED.term_category,
            source_system          = EXCLUDED.source_system,
            source_api             = EXCLUDED.source_api,
            pipeline_run_id        = EXCLUDED.pipeline_run_id,
            hash_key               = EXCLUDED.hash_key,
            silver_loaded_ts       = now();
        """
