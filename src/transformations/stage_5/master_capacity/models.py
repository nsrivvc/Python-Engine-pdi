"""
models.py
=========
The master capacity column model, defined once.

Both sides of stage 5 depend on these lists agreeing exactly:

    <feed>/<grain>/    maps a feed's stage-3 output onto the model
    final/<grain>/     UNIONs every feed's table into one

A UNION only works if every input has the same columns in the same order, so if
these lived in each subclass they could silently drift and the finals would break
at run time. They live here instead, and both sides import them.

CORE mirrors `public.final_core_master_capacity` (27 columns), snake_cased --
which matches the agreed Contracts mapping sheet.

LOCATIONS and RATES follow the agreed mapping sheet (Locations / Rates tabs,
target names snake_cased). Beyond the sheet each model keeps a small pipeline
block -- `ngh_contract_id` (the contract join key the sheet expresses via
GS_ID), `rate_unique_id` for rates (the natural row key), and
posted_date / update_date / source lineage columns the upsert's latest-wins
dedupe orders by.
"""

from __future__ import annotations

from typing import List, Tuple

Column = Tuple[str, str]

#: Mirrors public.final_core_master_capacity, snake_cased.
CORE_COLUMNS: List[Column] = [
    ("ngh_contract_id", "TEXT"),
    ("pipeline_duns", "TEXT"),
    ("pipeline_name", "TEXT"),
    ("contract_number", "TEXT"),
    ("award_number", "TEXT"),
    ("offer_number", "TEXT"),
    ("bid_number", "TEXT"),
    ("releaser_contract_number", "TEXT"),
    ("posted_date", "TIMESTAMPTZ"),
    ("begin_date", "TIMESTAMPTZ"),
    ("end_date", "TIMESTAMPTZ"),
    ("contract_quantity", "NUMERIC"),
    ("rate_schedule", "TEXT"),
    ("contract_holder", "TEXT"),
    ("contract_holder_duns", "TEXT"),
    ("releaser_name", "TEXT"),
    ("releaser_duns", "TEXT"),
    # term fields -- what the rec-del term transform ultimately feeds
    ("evergreen", "TEXT"),
    ("notice_period_days", "INTEGER"),
    ("calculated_end_date", "TIMESTAMPTZ"),
    ("replacement_shipper_role_indicator", "TEXT"),
    ("term_notes", "TEXT"),
    ("contract_type", "TEXT"),
    ("created_date", "TIMESTAMPTZ"),
    ("update_date", "TIMESTAMPTZ"),
    ("source", "TEXT"),
]

#: The agreed Locations mapping sheet, target names snake_cased, in sheet
#: order. `index` and `group` are Postgres keywords -- the DDL/DML builders
#: quote them (see _q in master_base / final_base). `group` has no source in
#: any feed yet and stays NULL until one is mapped.
LOCATIONS_COLUMNS: List[Column] = [
    ("ngh_contract_id", "TEXT"),          # contract join key (sheet: GS_ID linkage)
    ("location", "TEXT"),
    ("location_name", "TEXT"),
    ("zone", "TEXT"),
    ("location_qti", "TEXT"),
    ("location_purpose_code", "TEXT"),
    ("capacity_type", "TEXT"),
    ("quantity", "NUMERIC"),
    ("beg_date", "TIMESTAMPTZ"),
    ("end_date", "TIMESTAMPTZ"),
    ("season_beg_date", "TIMESTAMPTZ"),
    ("season_end_date", "TIMESTAMPTZ"),
    ("transaction_term_begin_datetime", "TIMESTAMPTZ"),
    ("transaction_term_end_datetime", "TIMESTAMPTZ"),
    ("segment", "TEXT"),
    ("index", "BIGINT"),
    ("group", "TEXT"),
    # pipeline lineage, not on the sheet: the upsert's latest-wins dedupe
    # orders by update_date / posted_date, and source names the feed.
    ("posted_date", "TIMESTAMPTZ"),
    ("update_date", "TIMESTAMPTZ"),
    ("source", "TEXT"),
]

#: The agreed Rates mapping sheet, target names snake_cased, in sheet order.
RATES_COLUMNS: List[Column] = [
    ("ngh_contract_id", "TEXT"),          # contract join key (sheet: GS_ID linkage)
    ("rate_unique_id", "TEXT"),           # natural row key (stage-3 uniqueid)
    ("rate_sequence", "INTEGER"),         # position of the rate in the contract's rates array
    ("rate_identification_code", "TEXT"),
    ("reporting_level", "TEXT"),
    ("rate_charged", "NUMERIC"),
    ("rate_charged_reference", "TEXT"),
    ("maximum_tariff_rate", "NUMERIC"),
    ("reservation_rate_basis_desc", "TEXT"),
    ("award_percentage_of_max_tariff", "NUMERIC"),
    ("beg_date", "TIMESTAMPTZ"),
    ("end_date", "TIMESTAMPTZ"),
    ("receipt_location", "TEXT"),
    ("receipt_location_name", "TEXT"),
    ("receipt_zone", "TEXT"),
    ("receipt_location_purpose", "TEXT"),
    ("delivery_location", "TEXT"),
    ("delivery_location_name", "TEXT"),
    ("delivery_zone", "TEXT"),
    ("delivery_location_purpose", "TEXT"),
    ("discount_beg_date", "TIMESTAMPTZ"),
    ("discount_end_date", "TIMESTAMPTZ"),
    ("season_beg_date", "TIMESTAMPTZ"),
    ("season_end_date", "TIMESTAMPTZ"),
    ("max_tariff_rate_reference", "TEXT"),
    ("market_based_rate_indicator", "TEXT"),
    ("negotiated_rate_indicator", "TEXT"),
    ("surcharge_identification_description", "TEXT"),
    ("surcharge_indicator", "TEXT"),
    ("surcharge_indicator_description", "TEXT"),
    # pipeline lineage, not on the sheet (see LOCATIONS_COLUMNS note).
    ("posted_date", "TIMESTAMPTZ"),
    ("update_date", "TIMESTAMPTZ"),
    ("source", "TEXT"),
]

COLUMNS_BY_GRAIN = {
    "core": CORE_COLUMNS,
    "locations": LOCATIONS_COLUMNS,
    "rates": RATES_COLUMNS,
}

#: Natural key per grain, used by both the per-feed tables and the finals.
#: Locations: contract + location + purpose mirrors the feed's own UniqueKey
#: ("TCO-F000001|100011|REC"), so it is unique by construction.
NATURAL_KEY_BY_GRAIN = {
    "core": ("ngh_contract_id",),
    "locations": ("ngh_contract_id", "location", "location_purpose_code"),
    "rates": ("ngh_contract_id", "rate_unique_id", "rate_sequence"),
}
