"""
table_config.py
===============
The pipeline's configurable tables, in ONE contained place.

Every class below is plain values -- table names, column names, accepted
spellings, matching knobs -- for a component that is expected to change as the
source system firms up. Change the value here and every consumer follows;
nothing else in the codebase hardcodes these.

    PipelineAttributes   the per-TSP amendment-reporting table that
                         ammendments(p2) joins on and asserts before writing
    RecDelPairing        how stage 4 splits locations into receipts and
                         deliveries and pre-filters them
    LocationsSource      which decomposition locations table feeds each
                         feed's rec-del pairing
    ShipperMapping       the dashboard's shipper scoping table that
                         deduplication(p1) filters Bronze through

These are VALUES, not logic: the SQL that uses them stays with its phase
(amend_base.py, pairing_base.py, shipper_scope.py). Schema names are not here
either -- they stay env-driven in config.py (BRONZE_SCHEMA / DECOMP_SCHEMA /
SILVER_SCHEMA).
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple


class PipelineAttributes:
    """`<DECOMP_SCHEMA>.pipeline_attributes` -- one row per TSP, keyed by
    DUNS, declaring how that pipeline reports amendments:

        tspduns      tspname                   amendment_reporting
        -----------  ------------------------  -------------------
        007933021    Texas Gas Transmission    Changes Only

    ammendments(p2) joins every non-first posting to this table and ENSURES
    the declared treatment; assert_pipeline_attributes() fails the run if a
    row's mode is not spelled like one of the tuples below."""

    #: Table name (created in DECOMP_SCHEMA by the amendments DDL).
    table = "pipeline_attributes"

    #: Column names.
    duns_col = "tspduns"
    name_col = "tspname"
    mode_col = "amendment_reporting"
    noted_col = "noted_ts"

    #: Accepted spellings of the two reporting modes, matched lower-cased and
    #: trimmed. Extend a tuple if a source spells a mode a new way.
    ALL_DATA = ("all data", "alldata", "all")
    CHANGES_ONLY = ("changes only", "changesonly", "changes")

    #: COVERAGE -- whether every TSP whose amendments this phase actually
    #: decides must have a row here. Off while the table is still being
    #: specced: a TSP with no row legitimately falls back to its postings'
    #: own descriptors, so an empty table changes nothing. Flip to True once
    #: the modes are known, and a TSP being amended on descriptor guesswork
    #: fails the run instead of quietly diverging.
    require_coverage = False

    #: TSP DUNS knowingly left on descriptor fallback -- exempt from the
    #: coverage assert even when `require_coverage` is on.
    coverage_exempt_duns: Tuple[str, ...] = ()

    #: THE ONBOARDING GATE -- whether this table is treated as the register of
    #: pipelines the warehouse is allowed to process at all.
    #:
    #:   False  the table only informs amendment treatment (the behaviour this
    #:          pipeline had before the gate existed). Every TSP loads.
    #:   True   a contract whose (DUNS, name) has no row here is HELD BACK at
    #:          deduplication(p1) and reported at ERROR, while registered TSPs
    #:          in the same load process exactly as normal.
    #:
    #: This is per-TSP, not per-run: an unregistered pipeline never costs a
    #: registered one its load. An EMPTY register still lets everything
    #: through, so turning this on before populating the table cannot silently
    #: reject an entire feed. The predicate lives in core/pipeline_scope.py.
    require_known_pipeline = True

    #: Whether the gate matches on DUNS *and* name, or DUNS alone. On, a known
    #: DUNS reporting under an unlisted name is treated as unregistered --
    #: the pair is what identifies a pipeline.
    match_name = True

    @staticmethod
    def sql_list(spellings: Tuple[str, ...]) -> str:
        """The spellings as a quoted SQL IN-list: `'all data', 'alldata', ...`"""
        return ", ".join(f"'{s}'" for s in spellings)


class RecDelPairing:
    """Stage 4's receipt/delivery split and pre-filtering. The pairing SQL
    itself (and its two SPEC hooks) lives in pairing_base.py."""

    #: Which location-purpose values mean receipt vs delivery. Compared
    #: upper-cased against the feed's purpose column.
    receipt_purpose = "REC"
    delivery_purpose = "DEL"

    #: Filter applied when reading the locations source (None reads all).
    source_filter: Optional[str] = "ingestion_status = 'LOADED'"

    #: Keeps only the newest row per (contract, location, purpose) before
    #: pairing (None only if the source is already deduplicated).
    dedupe_order: Optional[str] = "ingestion_timestamp DESC"


class LocationsSource:
    """Which decomposition locations table each feed's rec-del pairing reads
    (in DECOMP_SCHEMA). Rename a phase-3 output table -> update it here."""

    by_feed: Dict[str, str] = {
        "firm": "firm_locations",
        "interruptible": "interruptible_locations",
        "awards": "awards_locations",
    }

    @classmethod
    def for_feed(cls, feed: str) -> str:
        try:
            return cls.by_feed[feed]
        except KeyError:
            raise KeyError(
                f"No locations table configured for feed {feed!r} in "
                f"core/table_config.py (known: {', '.join(cls.by_feed)})"
            ) from None


class ShipperMapping:
    """`<BRONZE_SCHEMA>.shipper_mapping` -- the dashboard's shipper (DUNS)
    scoping rows that deduplication(p1) filters Bronze through. The DDL and
    predicate live in core/shipper_scope.py."""

    table = "shipper_mapping"

    #: Row actions the dashboard writes.
    ADD = "add"
    REMOVE = "remove"

    #: Bronze table -> (DUNS column, name column). The dashboard always calls
    #: these "KHolderNumber" and "KHolderName"; the feeds do not. Add a row
    #: when a new Bronze feed arrives; a table absent here is NOT filtered.
    keys: Dict[str, Tuple[str, str]] = {
        "gtran_firm": ("kholder", "kholdername"),
        "gtran_it": ("kholder", "kholdername"),
        "gawd": ("bidderduns", "biddername"),
        "gindex": ("shipperduns", "shipper"),
    }
