"""
amend_base.py
=============
Shared base for the ammendments phase (p2 of stage 3). Not a transformation
itself -- it registers nothing. One subclass per feed (firm, interruptible,
awards) sets column names; the logic here is identical for all three.

WHAT IS BEING AMENDED
---------------------
A contract (or award), identified by (contract id, TSP id). The source
re-posts the same contract over time; this phase folds those postings into
ONE current row per contract, keeping superseded versions as Void.

THE RULES -- the whole phase in four sentences
----------------------------------------------
1. Only postings still marked 'fresh' are consumed (stage 2 stamps the
   marker; an empty target replays the full history instead).
2. The FIRST-EVER instance of a contract is ALWAYS APPENDED, no matter what
   its descriptor says -- it IS the initial contract.
3. Every LATER instance is treated per the TSP's declared reporting mode,
   joined from the PIPELINE ATTRIBUTES table on the TSP id: 'All Data'
   replaces the running state wholesale, 'Changes Only' overlays just its
   non-NULL fields. (No attributes row -> the posting's own descriptor; no
   usable descriptor either -> 'All Data' behaviour, latest posting wins.)
4. The LATEST resulting state (by posting timestamp) is the row that lands
   as the new Current version; the old Current flips to Void, and every
   consumed source row flips 'fresh' -> 'processed'.

In SQL that replay collapses to: the latest full restatement per contract is
the baseline ("anchor"), changes posted after it overlay it column-by-column
(latest non-NULL wins), and a contract sending only changes overlays its
previous Current row instead.

THE PIPELINE ATTRIBUTES TABLE
-----------------------------
`<DECOMP_SCHEMA>.pipeline_attributes` -- one row per TSP, keyed by DUNS,
declaring how that pipeline reports amendments:

    tspduns      tspname                   amendment_reporting
    -----------  ------------------------  -------------------
    007933021    Texas Gas Transmission    Changes Only

The DDL creates it empty (shared by every feed); populate it by hand as
pipelines' modes become known. A TSP missing from it falls back to
per-posting descriptors, so an empty table changes nothing. The AmendRptgDesc
field on the affected postings is rewritten to the joined value, so
downstream sees the label that was acted on. `assert_pipeline_attributes()`
runs before any row is written and fails the run if a row carries a mode the
join would not recognize. Once the table is specced, setting
PipelineAttributes.require_coverage makes the same assert also demand a row
for every TSP the run actually amends -- first instances excluded, since
those are appended regardless of any mode -- so a pipeline being amended on
descriptor guesswork stops the run rather than diverging quietly.

Its table name, column names, and the accepted mode spellings all live in
core/table_config.py (PipelineAttributes) -- change them there, and the DDL,
the join, the classify CASEs and the assert below all follow.

Because the target accumulates versions and the source markers flip, this
transformation sets `incremental = True`: the runner never drops the table on
--reload. To rebuild from scratch, drop the table manually -- the next run
replays the full history.
"""

from __future__ import annotations

from typing import List

from ....core.base import PipelineTransformation
from ....core.table_config import PipelineAttributes as PA


class ContractAmendments(PipelineTransformation):
    # --- set these in each subclass ------------------------------------------
    feed: str = ""              # "firm" | "interruptible" | "awards"
    source_table: str = ""      # deduplication(p1) output holding the postings
    raw_table: str = ""         # the Bronze raw table whose 'fresh' markers flip
    contract_id_col: str = ""   # firmid | interruptibleid | awardnumber
    columns: List[str] = []     # every column of the source table, in order

    #: Second key component, and the column joined to pipeline_attributes.
    #: firm/it: "tspduns"; awards: its TSP identifier column.
    partner_col: str = "tspduns"

    #: The freshness marker stage 2 stamps ('fresh' -> 'processed' here).
    #: Awards: "record_status" -- gawd's `status` is the award's own data.
    status_col: str = "status"

    #: The posting timestamp that orders the replay. Awards: "postdatetime".
    posted_col: str = "posteddatetime"

    #: The amendment descriptor column, or None for a feed that has no such
    #: column (awards): later postings then rely entirely on the pipeline
    #: attributes join, defaulting to latest-posting-wins.
    desc_col: str | None = "amendrptgdesc"

    #: The target is version history -- see the module docstring and runner.
    #: (The pipeline attributes table itself is configured in
    #: core/table_config.py, imported above as PA.)
    incremental = True

    def __init__(self) -> None:
        for attr in ("feed", "source_table", "raw_table", "contract_id_col",
                     "columns", "status_col"):
            if not getattr(self, attr):
                raise ValueError(f"{type(self).__name__} must set `{attr}`.")
        self.source = self.feed
        self.bronze_sources = [self.source_table]
        super().__init__()

    @property
    def source_schema(self) -> str:
        """Reads deduplication(p1) output, which lives in DECOMP_SCHEMA."""
        return self.decomp_schema

    @property
    def target_schema(self) -> str:
        """Writes to DECOMP_SCHEMA, where decompisition(p3) picks it up."""
        return self.decomp_schema

    @property
    def key_cols(self) -> List[str]:
        return [self.contract_id_col, self.partner_col]

    @property
    def folded_cols(self) -> List[str]:
        """Columns resolved by latest-non-null. The keys are excluded -- they
        are what we group by, so they cannot vary within a group."""
        return [c for c in self.columns if c not in self.key_cols]

    # ----------------------------------------------------------- SQL helpers
    def _key_match(self, a: str, b: str) -> str:
        """NULL-safe join on the contract key between two row aliases."""
        return " AND ".join(
            f"{a}.{c} IS NOT DISTINCT FROM {b}.{c}" for c in self.key_cols
        )

    def _tmp(self, name: str) -> str:
        return f"_amend_{self.feed}_{name}"

    # ------------------------------------------------------------------ DDL
    def create_table_sql(self) -> str:
        tgt = f"{self.target_schema}.{self.table_name}"
        pat = f"{self.target_schema}.{PA.table}"
        key = ", ".join(self.key_cols)
        return f"""
        CREATE SCHEMA IF NOT EXISTS {self.target_schema};

        -- THE PIPELINE ATTRIBUTES TABLE (shared by every feed): one row per
        -- TSP, keyed by DUNS, declaring how that pipeline reports amendments
        -- ('All Data' or 'Changes Only'). Created empty -- populate by hand
        -- as pipelines' modes become known. Names come from table_config.py.
        CREATE TABLE IF NOT EXISTS {pat} (
            {PA.duns_col}         TEXT PRIMARY KEY,
            {PA.name_col}         TEXT,
            {PA.mode_col}         TEXT NOT NULL,
            {PA.noted_col}        TIMESTAMPTZ DEFAULT now()
        );

        -- LIKE clones the source's columns; versioning appended, amend_-
        -- prefixed so it can never shadow a business column (gawd carries
        -- its own version_status). The source's freshness marker is dropped:
        -- the target speaks amend_version_status, not fresh/processed.
        CREATE TABLE IF NOT EXISTS {tgt} (
            LIKE {self.source_schema}.{self.source_table},

            amend_kind               TEXT,        -- latest applied posting's kind
            amended_ts               TIMESTAMPTZ DEFAULT now(),
            amend_version_status     TEXT NOT NULL DEFAULT 'Current',
            amend_voided_ts          TIMESTAMPTZ
        );
        ALTER TABLE {tgt} DROP COLUMN IF EXISTS {self.status_col};

        CREATE UNIQUE INDEX IF NOT EXISTS uq_{self.table_name}_current
            ON {tgt} ({key}) NULLS NOT DISTINCT
            WHERE amend_version_status = 'Current';
        """

    # ------------------------------------------- pipeline attributes assert
    def assert_pipeline_attributes(self, conn) -> None:
        """Assert the pipeline attributes table is usable BEFORE any row is
        written: every row's amendment_reporting must be a mode the CLASSIFY
        join recognizes ('All Data' / 'Changes Only'). An unrecognized mode
        would silently fall back to per-posting descriptors -- a typo would
        change results without a sound -- so fail loudly instead.

        Then, when the table is specced enough to require it
        (PipelineAttributes.require_coverage), assert COVERAGE -- see
        `_assert_attribute_coverage` below."""
        pat = f"{self.target_schema}.{PA.table}"
        bad = conn.exec_driver_sql(f"""
            SELECT {PA.duns_col}, {PA.mode_col} FROM {pat}
            WHERE lower(trim(coalesce({PA.mode_col}, ''))) NOT IN
                  ({PA.sql_list(PA.ALL_DATA)},
                   {PA.sql_list(PA.CHANGES_ONLY)})
        """).all()
        if bad:
            listing = "; ".join(f"{duns}: {mode!r}" for duns, mode in bad)
            raise ValueError(
                f"{pat} declares amendment_reporting mode(s) this phase does "
                f"not recognize (expected 'All Data' or 'Changes Only'): "
                f"{listing}")

        self._assert_attribute_coverage(conn)

    def _assert_attribute_coverage(self, conn) -> None:
        """Assert every TSP this run actually AMENDS declares a mode.

        Coverage is only about the postings whose treatment the table
        decides -- the NON-FIRST ones. A first instance is always appended
        no matter what any descriptor or mode says (rule 2), so a TSP that
        only ever sent first instances needs no row and is not demanded one.
        The predicate below therefore mirrors CLASSIFY's `_is_first` exactly:
        change one and change the other.

        Postings with no TSP id are skipped -- no attributes row could cover
        them, so failing here would only report an unfixable source problem
        that belongs upstream.

        Off unless `PipelineAttributes.require_coverage` is set, because
        until the table is fully specced a missing row is a legitimate
        fallback to the posting's own descriptor rather than a mistake."""
        if not PA.require_coverage:
            return

        pat = f"{self.target_schema}.{PA.table}"
        src = f"{self.source_schema}.{self.source_table}"
        tgt = f"{self.target_schema}.{self.table_name}"
        pc = self.posted_col

        # Pipelines knowingly left on descriptor fallback.
        exempt = ""
        if PA.coverage_exempt_duns:
            exempt = ("  AND btrim(c._duns) NOT IN "
                      f"({PA.sql_list(PA.coverage_exempt_duns)})")

        missing = conn.exec_driver_sql(f"""
            WITH consumed AS (
                SELECT s.{self.partner_col} AS _duns,
                       (row_number() OVER (
                            PARTITION BY s.{self.contract_id_col}, s.{self.partner_col}
                            ORDER BY NULLIF(s.{pc}, '')::TIMESTAMPTZ ASC NULLS FIRST,
                                     s.bronze_row_id ASC) = 1
                        AND NOT EXISTS (SELECT 1 FROM {tgt} t
                                        WHERE t.amend_version_status = 'Current'
                                          AND {self._key_match('t', 's')})
                       ) AS _is_first
                FROM {src} s
                WHERE lower(coalesce(s.{self.status_col}, '')) = 'fresh'
                   OR NOT EXISTS (SELECT 1 FROM {tgt})
            )
            SELECT c._duns, count(*) AS postings
            FROM consumed c
            WHERE NOT c._is_first
              AND coalesce(btrim(c._duns), '') <> ''
              AND NOT EXISTS (SELECT 1 FROM {pat} pa
                              WHERE pa.{PA.duns_col} = c._duns){exempt}
            GROUP BY c._duns
            ORDER BY postings DESC, c._duns
        """).all()

        if missing:
            listing = "; ".join(f"{duns} ({n} posting{'s' if n != 1 else ''})"
                                for duns, n in missing)
            raise ValueError(
                f"{pat} is missing a row for {len(missing)} TSP(s) whose "
                f"{self.feed} amendments this run would decide: {listing}. "
                f"Add each one's amendment_reporting mode, or list it in "
                f"PipelineAttributes.coverage_exempt_duns to keep it on "
                f"per-posting descriptor fallback.")

    # ------------------------------------------------------------ transform
    def _stage_statements(self) -> List[str]:
        """Everything up to (not including) the final INSERT of new versions."""
        part = f"{self.contract_id_col}, {self.partner_col}"
        sep = ",\n            "
        cols = sep.join(self.columns)
        src = f"{self.source_schema}.{self.source_table}"
        tgt = f"{self.target_schema}.{self.table_name}"
        pat = f"{self.target_schema}.{PA.table}"
        classified = self._tmp("classified")
        anchors, units = self._tmp("anchors"), self._tmp("units")
        combined = self._tmp("combined")
        pc = self.posted_col

        # The TSP's declared mode from the attributes join (NULL = no row).
        # Column and spellings come from table_config.PipelineAttributes.
        pa_kind = f"""CASE
                WHEN lower(trim(pa.{PA.mode_col})) IN ({PA.sql_list(PA.ALL_DATA)})
                    THEN 'ALL_DATA'
                WHEN lower(trim(pa.{PA.mode_col})) IN ({PA.sql_list(PA.CHANGES_ONLY)})
                    THEN 'CHANGES_ONLY'
            END"""

        # The posting's own descriptor, for a TSP with no attributes row.
        # Anything unrecognized (including NULL, and feeds with no descriptor
        # column at all) acts as a full restatement: latest posting wins.
        if self.desc_col:
            own_kind = f"""CASE
                WHEN lower(trim(s.{self.desc_col})) IN ({PA.sql_list(PA.CHANGES_ONLY)})
                    THEN 'CHANGES_ONLY'
                ELSE 'ALL_DATA'
            END"""
        else:
            own_kind = "'ALL_DATA'"

        # AmendRptgDesc is REWRITTEN to the joined mode on the postings the
        # attributes table decided for, so downstream sees the label acted on.
        select_cols = sep.join(
            f"CASE WHEN NOT f._is_first AND f._pa_kind IS NOT NULL "
            f"THEN f._pa_mode ELSE f.{c} END AS {c}"
            if c == self.desc_col else f"f.{c}"
            for c in self.columns
        )

        # Latest non-null per column, changes ranked above the baseline
        # (_is_base), newest posting first. Postgres has no IGNORE NULLS on
        # window functions, so this filters the nulls out of an ordered
        # array_agg and takes the head.
        folded = sep.join(
            f"(array_agg(u.{c} ORDER BY u._is_base, u._posted_ts DESC NULLS LAST, "
            f"u.bronze_row_id DESC NULLS LAST) "
            f"FILTER (WHERE u.{c} IS NOT NULL))[1] AS {c}"
            for c in self.folded_cols
        )

        return [
            # 1. CLASSIFY the fresh postings (an empty target replays all).
            #
            #    THE FIRST INSTANCE IS ALWAYS APPENDED -- _is_first marks the
            #    earliest fresh posting of a contract that has no Current row
            #    yet; it becomes the baseline ('FIRST') NO MATTER WHAT its
            #    own descriptor says.
            #
            #    THE PIPELINE ATTRIBUTES JOIN -- every OTHER posting LEFT
            #    JOINs to the attributes table on the TSP id, and the TSP's
            #    declared mode (e.g. Texas Gas Transmission -> 'Changes
            #    Only') ENSURES how all its later instances are treated; no
            #    attributes row falls back to the posting's own descriptor.
            f"""
        CREATE TEMP TABLE {classified} ON COMMIT DROP AS
        SELECT {select_cols},
               f._posted_ts,
               CASE WHEN f._is_first THEN 'FIRST'
                    ELSE coalesce(f._pa_kind, f._own_kind)
               END AS amend_kind
        FROM (
            SELECT s.*,
                   NULLIF(s.{pc}, '')::TIMESTAMPTZ AS _posted_ts,
                   {own_kind} AS _own_kind,
                   pa.{PA.mode_col} AS _pa_mode,
                   {pa_kind} AS _pa_kind,
                   (row_number() OVER (PARTITION BY s.{self.contract_id_col}, s.{self.partner_col}
                        ORDER BY NULLIF(s.{pc}, '')::TIMESTAMPTZ ASC NULLS FIRST,
                                 s.bronze_row_id ASC) = 1
                    AND NOT EXISTS (SELECT 1 FROM {tgt} t
                                    WHERE t.amend_version_status = 'Current'
                                      AND {self._key_match('t', 's')})
                   ) AS _is_first
            FROM {src} s
            LEFT JOIN {pat} pa
              ON pa.{PA.duns_col} = s.{self.partner_col}
            WHERE lower(coalesce(s.{self.status_col}, '')) = 'fresh'
               OR NOT EXISTS (SELECT 1 FROM {tgt})
        ) f""",

            # 2. BASELINE -- one anchor per contract: the LATEST restatement
            # (the always-appended FIRST instance, or an ALL_DATA posting).
            # "First instance appended, every later All Data replaces it
            # wholesale" collapses to exactly this row; everything older is
            # superseded by it.
            f"""
        CREATE TEMP TABLE {anchors} ON COMMIT DROP AS
        SELECT DISTINCT ON ({part})
            {cols},
            amend_kind,
            _posted_ts
        FROM {classified}
        WHERE amend_kind IN ('FIRST', 'ALL_DATA')
        ORDER BY {part}, _posted_ts DESC NULLS LAST, bronze_row_id DESC""",

            # 3. FOLD UNITS -- per contract, what the replay reduces to: the
            # anchor as base plus every CHANGES_ONLY posting AFTER it; a
            # contract with no anchor in this batch (an already-known
            # contract sending only changes) uses its previous Current row
            # as base instead. _is_base ranks the base below every change.
            f"""
        CREATE TEMP TABLE {units} ON COMMIT DROP AS
        SELECT {cols},
            _posted_ts, amend_kind, FALSE AS _is_base
        FROM {classified} c
        WHERE c.amend_kind = 'CHANGES_ONLY'
          -- keep a change only when its contract has no anchor, or the
          -- change was posted after it (NULL timestamps sort oldest)
          AND NOT EXISTS (
              SELECT 1 FROM {anchors} a
              WHERE {self._key_match('a', 'c')}
                AND NOT ((a._posted_ts IS NULL AND c._posted_ts IS NOT NULL)
                         OR c._posted_ts > a._posted_ts))
        UNION ALL
        SELECT {cols},
            _posted_ts, amend_kind, TRUE
        FROM {anchors}
        UNION ALL
        SELECT {cols},
            NULLIF({pc}, '')::TIMESTAMPTZ, 'BASE', TRUE
        FROM {tgt} t
        WHERE t.amend_version_status = 'Current'
          AND NOT EXISTS (SELECT 1 FROM {anchors} a
                          WHERE {self._key_match('a', 't')})
          AND EXISTS (SELECT 1 FROM {classified} c
                      WHERE c.amend_kind = 'CHANGES_ONLY'
                        AND {self._key_match('c', 't')})""",

            # 4. COMBINE -- the fold: one candidate per contract, each column
            # taking its latest non-NULL value with the base as fallback --
            # the contract's latest state, the row that lands. amend_kind is
            # the latest applied posting's kind ('BASE' marks the previous
            # Current row, which never donates one).
            f"""
        CREATE TEMP TABLE {combined} ON COMMIT DROP AS
        SELECT {self.contract_id_col},
            {self.partner_col},
            {folded},
            (array_agg(u.amend_kind ORDER BY u._is_base, u._posted_ts DESC NULLS LAST,
                       u.bronze_row_id DESC NULLS LAST)
                 FILTER (WHERE u.amend_kind <> 'BASE'))[1] AS amend_kind
        FROM {units} u
        GROUP BY {part}""",

            # 5. VOID -- the Current rows being replaced.
            f"""
        UPDATE {tgt} t
        SET amend_version_status = 'Void',
            amend_voided_ts      = now()
        WHERE t.amend_version_status = 'Current'
          AND EXISTS (SELECT 1 FROM {combined} c WHERE {self._key_match('c', 't')})""",
        ]

    def _insert_sql(self) -> str:
        """The new Current versions -- the run's one row-producing insert,
        kept separate so run() can count what actually landed."""
        sep = ",\n            "
        cols = sep.join(self.columns)
        return f"""
        INSERT INTO {self.target_schema}.{self.table_name} (
            {cols},
            amend_kind,
            amend_version_status
        )
        SELECT
            {cols},
            amend_kind,
            'Current'
        FROM {self._tmp('combined')}
        """

    def _finalize_statements(self) -> List[str]:
        """Flip every consumed source row 'fresh' -> 'processed', stamping
        updated_ts -- in the staging table this phase reads AND in the Bronze
        raw table, matched by contract so rows this run never saw stay fresh."""
        status = self.status_col
        classified = self._tmp("classified")
        return [
            f"""
        UPDATE {self.source_schema}.{self.source_table} s
        SET {status}   = 'processed',
            updated_ts = now()
        WHERE lower(coalesce(s.{status}, '')) = 'fresh'
          AND EXISTS (SELECT 1 FROM {classified} f
                      WHERE f.bronze_row_id = s.bronze_row_id)""",

            f"""
        UPDATE {self.bronze_schema}.{self.raw_table} b
        SET {status}   = 'processed',
            updated_ts = now()
        WHERE lower(coalesce(b.{status}, '')) = 'fresh'
          AND EXISTS (SELECT 1 FROM {classified} f
                      WHERE {self._key_match('f', 'b')})""",
        ]

    def transform_sql(self) -> str:
        """The whole flow as one printable script (--show-sql). run() below
        executes the same statements individually."""
        statements = (
            self._stage_statements() + [self._insert_sql()]
            + self._finalize_statements()
        )
        return ";\n".join(s.rstrip().rstrip(";") for s in statements) + ";"

    # ------------------------------------------------------------------ run
    def run(self, conn) -> int:
        """Multi-statement override of the base single-statement load. Same
        contract: DDL first, then the pipeline-attributes assert, then the
        transform. All inside the runner's transaction, so a failure rolls
        back versions and status flips together."""
        conn.exec_driver_sql(self.create_table_sql())
        self.assert_pipeline_attributes(conn)
        for stmt in self._stage_statements():
            conn.exec_driver_sql(stmt)

        result = conn.exec_driver_sql(self._insert_sql().rstrip().rstrip(";"))
        written = result.rowcount if result.rowcount is not None else -1

        for stmt in self._finalize_statements():
            conn.exec_driver_sql(stmt)
        return written
