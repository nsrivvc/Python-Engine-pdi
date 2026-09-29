"""
pairing_base.py
===============
Shared base for the receipt/delivery ("rec-del") pairing transformations in this
package. Not a transformation itself -- it registers nothing.

WHAT THIS DOES
--------------
Stage 4 takes the locations table stage 3 finishes with -- one row per
location, with the purpose fields standardization(p4) has already rewritten
(Transco S8 / S9 -> delivery / receipt) -- and writes it back out with the
rec-del pairing attached. Nothing else. Every column of the stage-3 row is
carried through untouched; the pairing adds six columns on the end:

    rd               R | D                  which side this location is
    rd_position      1, 2, 3 ...            its place among the contract's R/D rows
    formation        R-D, R-D-R-D, R-R-D    the contract's R/D letters in location order
    pairing_pattern  the configured pattern that admitted the formation (NULL = none)
    pair_status      PAIRED | FAIL
    pair_group_id    which pairing within the contract this row belongs to;
                     restarts at 1 for every contract, NULL on FAIL rows

    stage 3 locations (end)               stage 4 rec_del_pair
    -----------------------               --------------------
    F24  idx1 S9 WHARTON     receipt  ->  <same row>  R  pos1  R-D      PAIRED  group 1
    F24  idx2 S8 ALGONQUIN   delivery ->  <same row>  D  pos2  R-D      PAIRED  group 1
    F2   idx1 S9 ...         receipt  ->  <same row>  R  pos1  R-D-R-D  PAIRED  group 1
    F2   idx2 S8 ...         delivery ->  <same row>  D  pos2  R-D-R-D  PAIRED  group 1
    F2   idx3 S9 ...         receipt  ->  <same row>  R  pos3  R-D-R-D  PAIRED  group 2
    F2   idx4 S8 ...         delivery ->  <same row>  D  pos4  R-D-R-D  PAIRED  group 2

A contract with one receipt and one delivery is two rows sharing group 1; ten
such contracts are twenty rows. Locations that are neither a receipt nor a
delivery (storage withdrawal / injection / area) take no part and are not
written.

HOW THE GROUP ID IS ASSIGNED
----------------------------
The pattern that admits a formation (the dashboard's Rec-Del Pairings table --
RecDelPairing in core/table_config.py) also says how it breaks into groups:

  * A regex of the form ^(X)+$ -- e.g. ^(R-D)+$ or ^(R-D-R)+$ -- repeats a
    fixed UNIT. Each repetition is one group, so R-D-R-D under ^(R-D)+$ reads
    1,1,2,2 ("first goes with second, third with fourth").
  * Any other regex -- e.g. ^R(?:-R)*-D(?:-D)*$ (n receipts feeding m
    deliveries) or ^R(?:-R)*$ (receipts only) -- admits the formation as ONE
    group, so R-R-R-D-D-D reads 1,1,1,1,1,1 and a lone R reads 1.

A pipeline's own rows (DUNS = the contract's TSP) take precedence over the
'default' rows (DUNS 0); within those, Order decides. Dashes are cosmetic:
both the formation and the regex have theirs removed before matching, so
"R-D-R-D" is tested as "RDRD" against "^(RD)+$" (as written, ^(R-D)+$ could
only ever match a single "R-D" -- the next character is a dash, not an R).

A contract no pattern admits still appears: every one of its rows is written
with pair_status FAIL, pair_group_id NULL and pairing_pattern NULL, so nothing
in scope vanishes from stage 4. With no patterns configured at all every
formation is admitted as one group (same convention as the pipeline register).

HOW THE STAGE-3 ROW IS CARRIED
------------------------------
The target is declared `LIKE` the source, so it has exactly the source's
columns -- whatever they are when the table is (re)built -- followed by the six
pairing columns, then a surrogate row id and a load timestamp. The INSERT lists
no column names: it supplies the source row expanded `(loc).*` plus the six
pairing values, which fill the first N+6 columns positionally, and the two
trailing columns take their defaults. Adding a column to a locations table
therefore reaches stage 4 on the next --reload with nothing to change here.
The stage workflows run with --reload, so the table is rebuilt from the
current source every run; there is no upsert.

WHICH COLUMNS THE PAIRING READS
-------------------------------
`column_map` names the handful of source columns the pairing itself needs --
the contract key, the location code and its array index (the order the
formation is spelled in), the standardized purpose, and the TSP DUNS the
pattern table is keyed by. A subclass overrides entries for a feed spelled
differently (awards). Everything else is carried, not read.
"""

from __future__ import annotations

from typing import Dict, Optional

from ....core.base import PipelineTransformation
from ....core.table_config import LocationsSource, RecDelPairing

#: Recognises a pattern regex of the form ^(X)+$ -- after its dashes are
#: removed -- and captures X, the repeated group unit ("RD", "RDR"); its length
#: is the number of locations per group. Postgres ARE syntax: \^ \( \) \+ \$
#: are literal characters of the pattern regex being inspected.
_UNIT_RE = r"^\^\(([RD]+)\)\+\$$"


class RecDelPairingTransformation(PipelineTransformation):
    # --- set these in each subclass ------------------------------------------
    entity: str = ""            # "firm" | "interruptible" | "awards"

    #: Source table in the decomposition schema -- the locations table as
    #: stage 3 leaves it. Left empty, it resolves from core/table_config.py
    #: (LocationsSource) by entity; set it here only to override.
    locations_table: str = ""

    # Which `loc_purpose` values mean receipt vs delivery (compared
    # upper-cased). Configured in core/table_config.py.
    receipt_purpose: str = RecDelPairing.receipt_purpose
    delivery_purpose: str = RecDelPairing.delivery_purpose

    #: Logical field -> source column, for the columns the pairing READS.
    #: Defaults to the gTRAN location naming; a subclass overrides entries for
    #: a feed spelled differently (awards). None = the feed has no such column.
    column_map: Dict[str, Optional[str]] = {
        "contract_key": "firmid",
        "loc_code": "loc",
        # The element's position within the contract's Locations array. The
        # SAME loc code legitimately recurs at different indexes carrying
        # different quantities, so the index is part of what identifies a
        # location, and it is the order the formation is spelled in. None =
        # the feed has no such column (awards); the code orders instead.
        "loc_index": None,
        # The value classified as receipt / delivery: the standardized
        # description, which is what standardization(p4) rewrites on a
        # pipeline's segment ends (Transco S8 / S9).
        "loc_purpose": "locpurpdesc",
        # The pattern table is keyed by the TSP's DUNS.
        "tsp_duns": "tspduns",
    }

    # Optional filter applied when reading the source table (e.g. keep only
    # successfully loaded rows). Configured in core/table_config.py; set to
    # None to read everything.
    source_filter: Optional[str] = RecDelPairing.source_filter

    # Keeps only the newest row per (contract, location, purpose, index)
    # before pairing, so a location the source holds twice is paired once.
    # One column plus direction, e.g. "ingestion_timestamp DESC". Configured
    # in core/table_config.py; set to None only if the source is already
    # deduplicated.
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
        """Read from the end of stage 3 (the decomposition schema), not Bronze."""
        return self.decomp_schema

    def col(self, logical: str) -> Optional[str]:
        """Source column backing a logical field.

        A mapping of None means the feed genuinely has no such column -- see
        `field`. Only an ABSENT key is an error.
        """
        try:
            return self.column_map[logical]
        except KeyError:  # pragma: no cover - developer error
            raise KeyError(
                f"{type(self).__name__}.column_map is missing {logical!r}"
            ) from None

    def field(self, logical: str, cast: str = "TEXT") -> str:
        """`(d.loc).column` -- the field read off the carried source row -- or
        a typed NULL when the feed has no such column (awards has no TSP DUNS
        on its locations), so the SQL stays identical across feeds."""
        column = self.col(logical)
        return f"(d.loc).{column}" if column else f"NULL::{cast}"

    # ------------------------------------------------------------------ DDL
    def create_table_sql(self) -> str:
        s = self.silver_schema
        src = f"{self.source_schema}.{self.locations_table}"
        return f"""
        CREATE SCHEMA IF NOT EXISTS {s};
        {RecDelPairing.pattern_ddl}
        CREATE TABLE IF NOT EXISTS {s}.{self.table_name} (
            -- 1. the stage-3 locations row: every column, exactly as the
            --    source table stands when this table is (re)built
            LIKE {src},

            -- 2. the pairing. ORDER MATTERS: transform_sql fills these
            --    positionally, straight after the carried columns.
            rd                     TEXT NOT NULL,      -- R | D
            rd_position            INTEGER NOT NULL,   -- 1-based place among the contract's R/D rows
            formation              TEXT NOT NULL,      -- the contract's R/D letters in location order
            pairing_pattern        TEXT,               -- configured pattern that admitted it (NULL = none)
            pair_status            TEXT NOT NULL,      -- PAIRED | FAIL
            pair_group_id          INTEGER,            -- restarts at 1 per contract; NULL on FAIL rows

            -- 3. bookkeeping, filled by default. Surrogate row id only: a
            --    pairing is identified by (contract, pair_group_id).
            rec_del_row_id         BIGSERIAL PRIMARY KEY,
            silver_loaded_ts       TIMESTAMPTZ DEFAULT now()
        );
        """

    # ------------------------------------------------------------ transform
    def transform_sql(self) -> str:
        s = self.silver_schema
        src = f"{self.source_schema}.{self.locations_table}"
        where = f"WHERE {self.source_filter}" if self.source_filter else ""

        key, code, purp = self.col("contract_key"), self.col("loc_code"), self.col("loc_purpose")
        idx = self.col("loc_index")
        pt = RecDelPairing

        # What identifies one location within a contract: code + purpose, plus
        # the array index on the feeds that have one.
        grain = f"s.{key}, s.{code}, upper(s.{purp})" + (f", s.{idx}" if idx else "")
        # Location order within a contract: the array index, else the code.
        order_expr = f"(d.loc).{idx}" if idx else f"(d.loc).{code}"

        if self.dedupe_order:
            newest = ", ".join(f"s.{term.strip()}" for term in self.dedupe_order.split(","))
            source = f"""
        -- The stage-3 locations rows, newest copy per location, each carried
        -- WHOLE as `loc` (the source table's row type).
        deduped AS (
            SELECT DISTINCT ON ({grain}) s AS loc
            FROM {src} s
            {where}
            ORDER BY {grain}, {newest}
        ),"""
        else:
            source = f"""
        -- The stage-3 locations rows, each carried WHOLE as `loc` (the source
        -- table's row type).
        deduped AS (
            SELECT s AS loc
            FROM {src} s
            {where}
        ),"""

        return f"""
        WITH{source}
        -- Every location classed R (receipt), D (delivery) or neither, by its
        -- standardized purpose (RecDelPairing in core/table_config.py).
        pool AS (
            SELECT d.loc,
                   (d.loc).{key}            AS contract_key,
                   {order_expr}             AS loc_order,
                   (d.loc).{code}           AS loc_code,
                   {self.field("tsp_duns")} AS tsp_duns,
                   CASE WHEN upper(btrim(coalesce((d.loc).{purp}, ''))) = '{self.receipt_purpose}'  THEN 'R'
                        WHEN upper(btrim(coalesce((d.loc).{purp}, ''))) = '{self.delivery_purpose}' THEN 'D'
                   END AS rd
            FROM deduped d
        ),
        -- The receipt/delivery locations only, each with its 1-based position
        -- within its contract in location order. Anything else takes no part.
        ordered AS (
            SELECT p.*,
                   row_number() OVER (PARTITION BY p.contract_key
                                      ORDER BY p.loc_order, p.loc_code) AS rd_position
            FROM pool p
            WHERE p.rd IS NOT NULL
        ),
        -- One row per contract: its R/D letters in location order, e.g. R-D.
        formation AS (
            SELECT l.contract_key,
                   max(l.tsp_duns) AS tsp_duns,
                   string_agg(l.rd, '-' ORDER BY l.rd_position) AS formation
            FROM ordered l
            GROUP BY l.contract_key
        ),
        -- The configured patterns (dashboard Configuration tab -> Rec-Del
        -- Pairings). Dashes are stripped from the regex (they are stripped
        -- from the formation too, below), and the group UNIT the regex repeats
        -- is read off it: ^(RD)+$ is 2 locations per group, ^(RDR)+$ is 3, and
        -- any other shape (NULL) means the whole formation is one group.
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
        -- No column list, on purpose: the carried row fills the source's
        -- columns, the six pairing values fill the six declared after them,
        -- and the trailing row id and timestamp take their defaults (see the
        -- module docstring, "HOW THE STAGE-3 ROW IS CARRIED").
        INSERT INTO {s}.{self.table_name}
        SELECT (l.loc).*,
               l.rd,
               l.rd_position::INTEGER,
               a.formation,
               a.pairing_pattern,
               CASE WHEN a.pairing_pattern IS NOT NULL OR g.no_patterns THEN 'PAIRED' ELSE 'FAIL' END,
               -- group id: position within the contract folded by the
               -- pattern's unit; one group for the whole formation when there
               -- is no unit; NULL when nothing admitted the formation.
               (CASE WHEN a.pairing_pattern IS NULL AND NOT g.no_patterns THEN NULL
                     WHEN a.unit_len IS NULL                               THEN 1
                     ELSE ((l.rd_position - 1) / a.unit_len) + 1
                END)::INTEGER
        FROM ordered l
        JOIN admitted a ON a.contract_key = l.contract_key
        CROSS JOIN gate g
        ORDER BY l.contract_key, l.rd_position;
        """
