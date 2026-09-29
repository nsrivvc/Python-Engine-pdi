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
    """Stage 4's receipt/delivery split, pre-filtering and pattern gate. The
    pairing SQL itself (and its two SPEC hooks) lives in pairing_base.py."""

    #: Which location-purpose values mean receipt vs delivery. Compared
    #: upper-cased and trimmed against the feed's purpose column -- by default
    #: the standardized DESCRIPTION (`locpurpdesc`), which is what the feed
    #: puts on its own M2 / MQ rows and what standardization(p4) writes onto a
    #: pipeline's segment ends (Transco S8 / S9). Anything else (storage
    #: withdrawal / injection / area, ...) is neither and takes no part.
    receipt_purpose = "RECEIPT LOCATION"
    delivery_purpose = "DELIVERY LOCATION"

    #: THE PATTERN GATE -- the dashboard's Rec-Del Pairings reference table.
    #: Each contract's locations, in location order, spell a formation of R
    #: and D letters ("R-D", "R-R-D-D", "R"). A contract is PAIRED when a
    #: configured pattern's regex admits that formation, and every one of its
    #: receipt/delivery locations is written as its own row carrying a
    #: `pair_set_id` that restarts at 1 per contract. The pattern's regex also
    #: fixes the set: ^(X)+$ (e.g. ^(R-D)+$) makes each repetition of X one
    #: set (R-D-R-D -> 1,1,2,2); any other shape admits the whole formation as
    #: one set (R-R-R-D-D-D -> all 1). A pipeline's own rows (DUNS = the
    #: contract's TSP) take precedence over the 'default' rows (DUNS 0);
    #: within those, `Order` decides which pattern is recorded. A contract no
    #: pattern admits still appears, every row marked FAIL with no set id.
    #: With NO patterns configured at all every formation passes as one set
    #: (same convention as the pipeline register).
    pattern_table = "public.rec_del_pairings"
    pipeline_col = "Pipeline"
    duns_col = "DUNS"
    order_col = "Order"
    pattern_col = "Pattern"
    regex_col = "Regex"

    #: The dashboard owns this table; this DDL is identical to its own, so
    #: whichever side runs first creates it and the other's is a no-op.
    pattern_ddl = f"""
        CREATE TABLE IF NOT EXISTS {pattern_table} (
            id        serial PRIMARY KEY,
            "{pipeline_col}" text NOT NULL DEFAULT 'default',
            "{duns_col}"     bigint NOT NULL DEFAULT 0,
            "{order_col}"    integer,
            "{pattern_col}"  text,
            "{regex_col}"    text
        );
        """

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


class LocationStandardization:
    """Per-pipeline remaps of what a location-purpose code MEANS, applied by
    stage 3 standardization(p4) to each feed's decomposed locations table
    (`<DECOMP_SCHEMA>.<feed>_locations`) in place.

    Some pipelines report a segment endpoint under a code whose feed-level
    description does not say which end it is. Transco posts both ends of a
    pipeline segment as S8 / S9 ("Pipeline Segment defined by ... locations");
    the business reads S8 as the delivery end and S9 as the receipt end. This
    table says so, keyed by the TSP's DUNS so it applies to that pipeline's
    rows and nobody else's.

    The raw `locpurp` code is left untouched -- it is what the feed said, and
    stage 4 keys on it. What changes are the descriptive fields the feed
    leaves ambiguous: `locpurpdesc`, `locqti`, `locqtidesc`, which stage 5
    carries into `location_purpose_code` / `location_qti` on the master
    capacity model. The values are the ones the same feed uses on its own
    explicit receipt (M2) and delivery (MQ) rows, so a standardized segment
    end reads exactly like a point the feed labelled itself.
    """

    #: Audit column added to the locations table: which rule rewrote the row
    #: (`<duns>:<code>`), NULL when no rule applied.
    rule_col = "loc_std_rule"

    #: TSP DUNS -> { locpurp code -> (locpurpdesc, locqti, locqtidesc) }.
    #: Codes are compared upper-cased and trimmed; DUNS trimmed.
    by_duns: Dict[str, Dict[str, Tuple[str, str, str]]] = {
        # Transcontinental Gas Pipe Line Company, LLC
        "007933021": {
            "S8": ("Delivery Location", "2", "Delivery point (s) quantity"),
            "S9": ("Receipt Location", "1", "Receipt point (s) quantity"),
        },
    }


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
