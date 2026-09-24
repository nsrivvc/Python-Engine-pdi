"""
dedup.py -- phase 1 of stage 3: drop the rows we have already seen.
==================================================================
Every fetch lands a fresh batch in Bronze, duplicates and all. This phase
copies Bronze into staging and keeps ONE copy of each distinct row:

    firm           bronze.gtran_firm -> silver_staging.firm_dedup
    interruptible  bronze.gtran_it   -> silver_staging.interruptible_dedup
    awards         bronze.gawd       -> silver_staging.awards_dedup

(IOC has no dedup on purpose -- deduplication covers these three feeds only.)

HOW A DUPLICATE IS DECIDED -- right here, nowhere else
------------------------------------------------------
Two rows are duplicates when EVERY DATA FIELD MATCHES EXACTLY. The comparison
is self-contained in transform_sql below; it does not depend on anything any
earlier stage computed.

The one wrinkle: every Bronze row also carries the pipeline's own bookkeeping
(row id, run id, load timestamps, file name...). Two copies of the same
contract always differ there, because they arrived in different runs. So those
stamp columns are set aside (LOAD_STAMPS below) and the comparison covers
everything else -- the actual data.

In SQL that reads:

    to_jsonb(row) - LOAD_STAMPS      -- the row as {column: value}, stamps removed

    * DISTINCT ON (that)             -- one copy per distinct content in the batch
    * WHERE NOT EXISTS (same content -- and no copy already in the staging table
      already in staging)               from an earlier run

Rows that survive both checks are inserted into the staging table on Neon.
Nothing is ever deleted: Bronze keeps its duplicates (it is the full history),
and staging simply never receives a second copy.

WHY `LIKE`
----------
The target is created with `CREATE TABLE ... (LIKE <bronze table>)`, cloning
the source's columns without naming them, so this file needs no column list
and keeps working as the Bronze schema evolves.

Output goes to DECOMP_SCHEMA (default `silver_staging`), which is what
ammendments(p2), decompisition(p3) and stage 4 read.
"""

from __future__ import annotations #purely affects annotations.

from ....core import pipeline_scope, shipper_scope
from ....core.base import PipelineTransformation
from ....core.registry import register

#: The pipeline's own bookkeeping columns -- set aside before comparing, so
#: "duplicate" means "same DATA", not "same data loaded at the same moment".
LOAD_STAMPS = (
    "bronze_row_id",        # serial row number, unique by construction
    "raw_record_id",        # copy of the record's own id (also a data column)
    "hash_key",             # stage 2's fingerprint; carried, never compared
    "pipeline_run_id",      # new uuid every run
    "source_system",
    "source_api",
    "source_file_name",
    "ingestion_timestamp",  # load time, differs every run
    "updated_ts",
    "ingestion_status",
    "status",               # freshness marker (firm/IT); flipped by ammendments(p2)
    "record_status",        # the same marker on gawd ("status" is business data there)
    "raw_payload",          # the whole record again, as JSON
)


class Deduplication(PipelineTransformation):
    """One Bronze feed table -> one deduplicated staging table.

    A feed is the four declarations below it: what the transformation is
    called, the table it writes, the feed it belongs to, and the Bronze table
    it reads.
    """

    feed: str = ""             # "firm" | "interruptible" | "awards"
    source_table: str = ""     # the Raw table this deduplicates

    def __init__(self) -> None:
        for attr in ("feed", "source_table"):
            if not getattr(self, attr):
                raise ValueError(f"{type(self).__name__} must set `{attr}`.")
        self.source = self.feed #we define what source
        self.bronze_sources = [self.source_table] # we define the raw table
        super().__init__()

    #used to see what schema we're landing into within the Neon instance
    @property
    def target_schema(self) -> str:
        """Staging, not Silver -- p2/p3 and stage 4 all read from here."""
        return self.decomp_schema

    #this creates an instance of the table before duplication
    def create_table_sql(self) -> str:
        return f"""
        CREATE SCHEMA IF NOT EXISTS {self.target_schema}; -- create staging schema if missing

        CREATE TABLE IF NOT EXISTS {self.target_schema}.{self.table_name} (
            LIKE {self.source_schema}.{self.source_table}
        );

        -- Migration for tables created by the older hash-based version: the
        -- comparison now lives entirely in transform_sql, so the constraint
        -- is retired. No-op on a fresh table.
        ALTER TABLE {self.target_schema}.{self.table_name}
            DROP CONSTRAINT IF EXISTS uq_{self.table_name}_hash;

        -- The shipper scope mapping table provisions itself here so the
        -- predicate in transform_sql can never reference a missing table.
        {shipper_scope.ddl(self.source_schema)}

        -- Same for the pipeline_attributes register the onboarding gate reads.
        -- It lives in the staging schema, where ammendments(p2) also expects
        -- it; whichever phase runs first creates it.
        {pipeline_scope.ddl(self.target_schema)}
        """

    def run(self, conn) -> int:
        """Report anything the onboarding gate holds back, then load as usual.

        The report runs BEFORE the insert and inside the same transaction, so
        what it names is exactly what the predicate in transform_sql then
        excludes -- the two cannot disagree about the register's contents.
        """
        pipeline_scope.report_uncovered(
            conn, self.target_schema, self.source_schema, self.source_table, self.feed
        )
        return super().run(conn)

    #handles the actual transformation / logic for deduplicated rows.
    #done entirley in sql
    def transform_sql(self) -> str:
        # to_jsonb(s) turns a whole row into {column: value, ...}; subtracting
        # LOAD_STAMPS leaves only the data fields. Comparing those compares
        # every data field of two rows at once.
        stamps = ", ".join(f"'{c}'" for c in LOAD_STAMPS)      # the bookkeeping columns, quoted for SQL
        content = f"(to_jsonb(s) - ARRAY[{stamps}]::text[])"   # an incoming Bronze row, data fields only
        existing = f"(to_jsonb(t) - ARRAY[{stamps}]::text[])"  # a staging row, data fields only
        return f"""
        -- Copy rows from Bronze into staging. Nothing is ever deleted:
        -- a duplicate is simply not copied in.
        INSERT INTO {self.target_schema}.{self.table_name}
        -- One copy per distinct content within this batch: rows whose data
        -- fields all match collapse to the earliest one (lowest bronze_row_id).
        SELECT DISTINCT ON ({content}) s.*
        -- Read every row Bronze holds for this feed...
        FROM {self.source_schema}.{self.source_table} s
        -- ...and keep only content the staging table does not already hold from
        -- an earlier run. Same data fields = duplicate = not copied again.
        WHERE NOT EXISTS (
            SELECT 1 FROM {self.target_schema}.{self.table_name} t
            WHERE {existing} = {content}
        )
        -- Shipper scope: rows in {self.source_schema}.shipper_mapping narrow
        -- the feed to the configured DUNS. No rows = unscoped, all pass.
        {shipper_scope.and_where(self.source_schema, self.source_table, self.feed, "s")}
        -- Pipeline onboarding gate: a TSP with no row in the
        -- {self.target_schema}.pipeline_attributes register is held back, while
        -- registered TSPs in the same load pass. Off unless
        -- PipelineAttributes.require_known_pipeline; empty register = all pass.
        {pipeline_scope.and_where(self.target_schema, self.source_table, "s")}
        -- Ties within the batch: the ORDER BY makes DISTINCT ON keep the row
        -- with the lowest bronze_row_id, i.e. the earliest-loaded copy.
        ORDER BY {content}, s.bronze_row_id
        """


@register
class SilverFirmDedup(Deduplication):
    name = "silver_firm_dedup"
    table_name = "firm_dedup"
    feed = "firm"
    source_table = "gtran_firm"


@register
class SilverInterruptibleDedup(Deduplication):
    name = "silver_interruptible_dedup"
    table_name = "interruptible_dedup"
    feed = "interruptible"
    source_table = "gtran_it"


# Awards carry no amendment marker, so ammendments(p2) has nothing to do for
# this feed and decompisition(p3) reads awards_dedup directly.
@register
class SilverAwardsDedup(Deduplication):
    name = "silver_awards_dedup"
    table_name = "awards_dedup"
    feed = "awards"
    source_table = "gawd"

# No IOC class: gindex goes to Silver without a deduplication phase.
