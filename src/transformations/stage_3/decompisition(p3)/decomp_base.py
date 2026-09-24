"""
decomp_base.py
==============
Shared base for the decomposition phase (p3 of stage 3). Not a transformation
itself -- it registers nothing.

WHAT IT DOES
------------
Takes the one row-per-contract table each feed has after deduplication(p1) /
ammendments(p2) and splits it into the grains stage 4 and 5 read:

    core       one row per contract      <- ammendments(p2) output (firm, IT)
                                            deduplication(p1) output (awards)
    locations  one row per location      <- exploded out of the contract's
    rates      one row per rate             nested JSON, see below

WHERE THE NESTED GRAINS COME FROM
---------------------------------
There is no Bronze locations or rates table. Ingestion lands each feed as ONE
raw table (gtran_firm, gtran_it, gawd) whose rows carry the contract header plus
nested `locations` and `rates` JSON arrays, and deduplication(p1) passes those
rows through whole. So the nested grains enter the pipeline HERE: `section`,
`parent_columns` and `element_keys` on a subclass describe one element, and
`NestedExplosion` turns the array into one row per element as part of the same
statement that projects it onto the grain's schema. No intermediate table.

COLUMNS COME FROM THE JSON. `element_keys` lists one element's keys verbatim
(CamelCase, as they appear in the payload); each becomes a column named
key.lower(). A key absent from some element yields NULL rather than failing.
`parent_columns` carries the contract-row fields that do not exist inside an
element but that the grain's schema needs (the contract id, posteddatetime, and
for rates tspduns/tspname).

WHY IT WRITES OUTSIDE THE SILVER SCHEMA
---------------------------------------
Stage 4 reads `DECOMP_SCHEMA` (default `silver_staging`), not `silver`, so this
overrides `target_schema`. The runner's load-once check and `--inspect` both
follow `target_schema`, so the table is still detected in the right place.

TWO DELIBERATE DEPARTURES FROM THE PYSPARK SCHEMA
-------------------------------------------------
  * `loczn` is ArrayType(StringType) there but TEXT in Bronze, so it stays TEXT
    here. Parsing it into an array is a decision for standardization (p4), which
    is where type normalisation belongs.
  * The audit columns below the business schema (`ingestion_status`,
    `ingestion_timestamp`, `hash_key`, `source_system`, `source_api`,
    `pipeline_run_id`) are carried through deliberately. Stage 4 filters on
    `ingestion_status` and dedupes on `ingestion_timestamp`; dropping them here
    would break pairing.

DEDUPE IS NOT OPTIONAL
----------------------
The p1 output accumulates one row per distinct contract content ever seen, so
the same (contract, uniqueid) pair can appear under several content versions.
Without latest-wins dedupe the upsert would touch the same target row twice in
one statement, which Postgres rejects outright.
"""

from __future__ import annotations

from typing import List, Tuple

from ....core.base import PipelineTransformation

#: Business columns, in the order the agreed schema lists them. `{id}` and
#: `{qty}` are substituted per feed. Everything is TEXT in Bronze except index.
LOCATION_COLUMNS: List[Tuple[str, str]] = [
    ("index", "BIGINT"),
    ("transactiontermbegindatetime", "TEXT"),
    ("transactiontermenddatetime", "TEXT"),
    ("segment", "TEXT"),
    ("{id}", "TEXT"),
    ("uniqueid", "TEXT"),
    ("pk", "TEXT"),
    ("{qty}", "TEXT"),
    ("seasnlst", "TEXT"),
    ("seasnlend", "TEXT"),
    ("uniquekey", "TEXT"),
    ("id", "TEXT"),
    ("posteddatetime", "TEXT"),
    ("kentbegdatetime", "TEXT"),
    ("kentenddatetime", "TEXT"),
    ("captypename", "TEXT"),
    ("loc", "TEXT"),
    ("locname", "TEXT"),
    ("locpurp", "TEXT"),
    ("locpurpdesc", "TEXT"),
    ("loczn", "TEXT"),
    ("locqti", "TEXT"),
    ("locqtidesc", "TEXT"),
    ("tspduns", "TEXT"),
    ("tspname", "TEXT"),
    ("createddatetime", "TEXT"),
]

#: Carried through for stage 4 (see module docstring).
AUDIT_COLUMNS: List[Tuple[str, str]] = [
    ("source_system", "TEXT"),
    ("source_api", "TEXT"),
    ("pipeline_run_id", "TEXT"),
    ("hash_key", "TEXT"),
    ("ingestion_status", "TEXT"),
    ("ingestion_timestamp", "TIMESTAMPTZ"),
]

#: Audit columns the explosion stamps onto every element row. All but two are
#: copied from the parent contract row; the exceptions are `hash_key` (the
#: ELEMENT's own content fingerprint) and `raw_payload` (the element's own JSON
#: object, not the whole contract's).
ELEMENT_AUDIT_COLUMNS: List[Tuple[str, str]] = [
    ("raw_record_id", "TEXT"),
    ("hash_key", "TEXT"),
    ("pipeline_run_id", "TEXT"),
    ("source_system", "TEXT"),
    ("source_api", "TEXT"),
    ("source_file_name", "TEXT"),
    ("ingestion_timestamp", "TIMESTAMPTZ"),
    ("updated_ts", "TIMESTAMPTZ"),
    ("ingestion_status", "TEXT"),
    ("raw_payload", "JSONB"),
]

#: `index` is a SQL keyword; quote every identifier that needs it.
_NEEDS_QUOTING = {"index"}

#: Columns whose Bronze type differs from the target type and so need an
#: explicit cast on the way in. Everything is TEXT in Bronze; `index` is
#: LongType in the agreed schema, hence BIGINT here. Empty strings become NULL
#: rather than failing the cast.
#:
#: The transaction term columns are renames: the agreed schema exposes the
#: contract's kbegdatetime / kenddatetime (carried onto every exploded location
#: row as a parent column) under these names.
SELECT_OVERRIDES = {
    "index": "NULLIF(\"index\", '')::BIGINT",
    "transactiontermbegindatetime": "kbegdatetime",
    "transactiontermenddatetime": "kenddatetime",
}


def _q(name: str) -> str:
    return f'"{name}"' if name in _NEEDS_QUOTING else name


def _select_expr(name: str) -> str:
    """How a column is read out of the source."""
    return SELECT_OVERRIDES.get(name, _q(name))


class NestedExplosion:
    """Turns a contract row's nested JSON array into one row per element.

    Mixed into the grain classes below. A subclass that sets `section` reads an
    exploded view of `source_table`; one that leaves it blank reads
    `source_table` directly, which is what the core grain does.
    """

    section: str = ""                # raw_payload key: "locations" | "rates"
    parent_columns: List[str] = []   # contract-row columns copied onto each element
    element_keys: List[str] = []     # JSON keys of one element; column = key.lower()

    #: Optional predicate on the source rows. The core grains read the
    #: ammendments(p2) output, which is a VERSION HISTORY -- they must see only
    #: the Current version of each contract, never the Void ones. The exploded
    #: grains read the p1 dedup table, which carries no versions, so they leave
    #: this empty.
    source_where: str = ""

    @property
    def exploded(self) -> bool:
        return bool(self.section)

    @property
    def exploded_columns(self) -> List[Tuple[str, str]]:
        """(name, type) for every column the explosion produces, in order."""
        return (
            [("bronze_row_id", "BIGINT")]
            + [(c, "TEXT") for c in self.parent_columns]
            + [(k.lower(), "TEXT") for k in self.element_keys]
            + ELEMENT_AUDIT_COLUMNS
        )

    def _section_expr(self) -> str:
        """The nested array, found CASE-INSENSITIVELY in `raw_payload`.

        `raw_payload` preserves the producer's original keys verbatim, and
        producers disagree on case: the mock fixtures ship `locations` /
        `rates`, the live NatGasHub export ships `Locations` / `Rates`. A plain
        `raw_payload -> 'locations'` silently matches nothing against the
        latter -- contracts would land in Bronze and then produce ZERO location
        and rate rows, with no error anywhere. That is the worst kind of
        failure, so the lookup matches on lower(key) instead.
        """
        return (
            "(SELECT v FROM jsonb_each(s.raw_payload) AS e(k, v) "
            f"WHERE lower(e.k) = '{self.section.lower()}' LIMIT 1)"
        )

    def _explode_sql(self) -> str:
        """A SELECT producing one row per array element, aliased to
        `exploded_columns`. Used in place of a source table."""
        select_parts = (
            ["s.bronze_row_id"]
            + [f"s.{c}" for c in self.parent_columns]
            + [f"el ->> '{k}' AS {_q(k.lower())}" for k in self.element_keys]
            + [
                "s.raw_record_id",
                "md5(el::text) AS hash_key",   # element-level content fingerprint
                "s.pipeline_run_id",
                "s.source_system",
                "s.source_api",
                "s.source_file_name",
                "s.ingestion_timestamp",
                "s.updated_ts",
                "s.ingestion_status",
                "el AS raw_payload",           # the element's JSON, not the contract's
            ]
        )
        sel = ",\n                   ".join(select_parts)
        return f"""SELECT {sel}
            FROM {self.source_schema}.{self.source_table} s
            CROSS JOIN LATERAL jsonb_array_elements({self._section_expr()}) AS el
            WHERE jsonb_typeof({self._section_expr()}) = 'array'"""

    def _source_sql(self) -> str:
        """What the grain reads: the exploded elements, or the table --
        narrowed by `source_where` when the grain sets one."""
        if self.exploded:
            return f"(\n            {self._explode_sql()}\n        ) s"
        if self.source_where:
            return (f"(SELECT * FROM {self.source_schema}.{self.source_table} "
                    f"WHERE {self.source_where}) s")
        return f"{self.source_schema}.{self.source_table} s"


class LocationsDecomposition(NestedExplosion, PipelineTransformation):
    """The locations grain, projected onto the agreed schema.

    Two feeds differ only in two column names, so they are class attributes:

        firm           firmid          / kqtyloc
        interruptible  interruptibleid / itqtyloc
    """

    # --- set these in each subclass ------------------------------------------
    feed: str = ""              # "firm" | "interruptible"
    source_table: str = ""      # the feed's one deduplicated contract table
    contract_id_col: str = ""   # firmid | interruptibleid
    qty_col: str = ""           # kqtyloc | itqtyloc

    #: What separates one location record from the next WITHIN a contract.
    #: `uniqueid` alone cannot: it is the CONTRACT's id, repeated verbatim on
    #: every element of its Locations array, so keying on it kept one row per
    #: contract and silently discarded the rest (13 locations -> 1). The
    #: array position is the only thing that differs between elements.
    #: Quoted because `index` is a SQL keyword.
    element_key_col: str = '"index"'

    @property
    def grain_key_sql(self) -> str:
        """The location grain: contract + element position."""
        return f"{self.contract_id_col}, uniqueid, {self.element_key_col}"

    def __init__(self) -> None:
        for attr in ("feed", "source_table", "contract_id_col", "qty_col",
                     "section", "element_keys"):
            if not getattr(self, attr):
                raise ValueError(f"{type(self).__name__} must set `{attr}`.")
        self.source = self.feed
        self.bronze_sources = [self.source_table]
        super().__init__()

    @property
    def target_schema(self) -> str:
        """Stage 4 reads DECOMP_SCHEMA, so the output lands there, not in Silver."""
        return self.decomp_schema

    @property
    def source_schema(self) -> str:
        """Reads the earlier stage-3 phases' output, which also lives in
        DECOMP_SCHEMA."""
        return self.decomp_schema

    @property
    def columns(self) -> List[Tuple[str, str]]:
        """The business schema with this feed's two variable column names filled in."""
        return [
            (name.format(id=self.contract_id_col, qty=self.qty_col), sql_type)
            for name, sql_type in LOCATION_COLUMNS
        ]

    # ------------------------------------------------------------------ DDL
    def create_table_sql(self) -> str:
        cols = ",\n            ".join(
            f"{_q(n):<24} {t}" for n, t in self.columns + AUDIT_COLUMNS)
        return f"""
        CREATE SCHEMA IF NOT EXISTS {self.target_schema};

        CREATE TABLE IF NOT EXISTS {self.target_schema}.{self.table_name} (
            {cols},
            decomp_loaded_ts         TIMESTAMPTZ DEFAULT now(),

            -- One row per contract per location record. NULLS NOT DISTINCT
            -- (PG15+) so a NULL key part still collides on rerun.
            CONSTRAINT uq_{self.table_name}
                UNIQUE NULLS NOT DISTINCT ({self.grain_key_sql})
        );
        """

    # ------------------------------------------------------------ transform
    def transform_sql(self) -> str:
        all_cols = self.columns + AUDIT_COLUMNS
        names = [n for n, _ in all_cols]
        sel = ",\n                   ".join(_select_expr(n) for n in names)
        ins = ",\n            ".join(_q(n) for n in names)
        key_names = {self.contract_id_col, "uniqueid",
                     self.element_key_col.strip('"')}
        updatable = [n for n in names if n not in key_names]
        sep = ",\n            "
        updates = sep.join(f"{_q(n):<24} = EXCLUDED.{_q(n)}" for n in updatable)

        return f"""
        WITH exploded AS (
            {self._explode_sql()}
        ),
        latest AS (
            SELECT * FROM (
                SELECT s.*, row_number() OVER (
                    PARTITION BY {self.grain_key_sql}
                    ORDER BY ingestion_timestamp DESC, bronze_row_id DESC) AS _rn
                FROM exploded s
            ) x WHERE _rn = 1
        )
        INSERT INTO {self.target_schema}.{self.table_name} AS tgt (
            {ins}
        )
        SELECT {sel}
        FROM latest
        ON CONFLICT ({self.grain_key_sql}) DO UPDATE SET
            {updates},
            decomp_loaded_ts         = now();
        """


class GrainDecomposition(NestedExplosion, PipelineTransformation):
    """Generic decomposition for the core and rates grains.

    Locations has an agreed PySpark schema and its own typed class above. Core
    and rates have no such schema yet, so this projects the source's columns
    straight through onto a keyed staging table -- the point being to produce one
    stable, keyed table per grain that stage 4 and 5 can rely on, rather than to
    retype the fields.

    Sources differ by grain, which is what the phase order dictates:

        core       <- ammendments(p2) output, so the contract history is already
                      folded into one current row per contract (awards have no
                      amendment marker, so they come straight from p1)
        rates      <- the same deduplicated contract table, exploded on its
                      nested `rates` array (set `section`)

    SPEC: when core/rates get an agreed schema like locations has, give them a
    typed subclass of their own and retire this passthrough.
    """

    feed: str = ""
    grain: str = ""             # "core" | "rates" | "locations"
    source_table: str = ""
    key_cols_list: List[str] = []
    columns: List[str] = []

    #: Columns present in the source that this grain deliberately does NOT
    #: carry. `LIKE` clones every source column, so listing a column in
    #: `columns` is not enough to keep one out of the target -- it would just
    #: sit there NULL. These are dropped after the clone.
    #:
    #: Only applies to a non-exploded grain: an exploded one builds its table
    #: from `columns` directly and so never gains a column it did not ask for.
    #: The awards core grain needs it -- its Bronze row keeps the nested
    #: `locations` and `rates` JSON alongside the contract fields, and the
    #: agreed schema excludes both (they become their own grains).
    drop_columns: List[str] = []

    def __init__(self) -> None:
        for attr in ("feed", "grain", "source_table", "key_cols_list", "columns"):
            if not getattr(self, attr):
                raise ValueError(f"{type(self).__name__} must set `{attr}`.")
        self.source = self.feed
        self.bronze_sources = [self.source_table]
        super().__init__()

    @property
    def source_schema(self) -> str:
        return self.decomp_schema

    @property
    def target_schema(self) -> str:
        return self.decomp_schema

    def create_table_sql(self) -> str:
        key = ", ".join(self.key_cols_list)

        if self.exploded:
            # No table to clone: the explosion is a query, so the grain's own
            # column list defines the target, typed from what it produces.
            types = dict(self.exploded_columns)
            cols = ",\n            ".join(
                f"{_q(c):<26} {types.get(c, 'TEXT')}" for c in self.columns)
            body = f"{cols},"
            drops = ""
        else:
            body = f"LIKE {self.source_schema}.{self.source_table},"
            drops = "".join(
                f"\n        ALTER TABLE {self.target_schema}.{self.table_name} "
                f"DROP COLUMN IF EXISTS {c};"
                for c in self.drop_columns
            )

        return f"""
        CREATE SCHEMA IF NOT EXISTS {self.target_schema};

        CREATE TABLE IF NOT EXISTS {self.target_schema}.{self.table_name} (
            {body}
            decomp_loaded_ts         TIMESTAMPTZ DEFAULT now(),
            CONSTRAINT uq_{self.table_name}
                UNIQUE NULLS NOT DISTINCT ({key})
        );
        {drops}
        """

    def transform_sql(self) -> str:
        key = ", ".join(self.key_cols_list)
        sep = ",\n            "
        cols = sep.join(_q(c) for c in self.columns)
        updatable = [c for c in self.columns if c not in self.key_cols_list]
        updates = sep.join(f"{_q(c):<26} = EXCLUDED.{_q(c)}" for c in updatable)
        # Latest wins per key, so a re-run cannot hit the same target row twice.
        return f"""
        WITH latest AS (
            SELECT * FROM (
                SELECT s.*, row_number() OVER (
                    PARTITION BY {key}
                    ORDER BY ingestion_timestamp DESC, bronze_row_id DESC) AS _rn
                FROM {self._source_sql()}
            ) x WHERE _rn = 1
        )
        INSERT INTO {self.target_schema}.{self.table_name} AS tgt (
            {cols}
        )
        SELECT
            {cols}
        FROM latest
        ON CONFLICT ({key}) DO UPDATE SET
            {updates},
            decomp_loaded_ts         = now();
        """
