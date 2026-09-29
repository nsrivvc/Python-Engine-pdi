"""
silver_final_rates_master_capacity.py
=====================================
FINAL Rates — Master Capacity. Consolidates all four feeds' rate master capacity
tables into one.

NO EXISTING TARGET TO MIRROR: there is no `final_rates_master_capacity` table in
the database yet, so the column set below is a SPEC placeholder built from the
rate fields this pipeline already handles (bronze.gtran_rates and
silver.firm_transport_rate). Replace it with the real model when it's settled —
the DDL and the UNION both follow from this list.
"""

from __future__ import annotations

from ..final_base import FinalMasterCapacityTransformation
from ...models import RATES_COLUMNS
from ......core.registry import register


@register
class SilverFinalRatesMasterCapacity(FinalMasterCapacityTransformation):
    name = "silver_final_rates_master_capacity"
    table_name = "final_rates_master_capacity"
    grain = "rates"

    # Shared with the per-feed transformations so the UNION can never break
    # on a column mismatch. Single definition lives in ../../models.py.
    columns = RATES_COLUMNS

    # SPEC: one row per contract per rate record per feed.
    natural_key = ("source_type", "ngh_contract_id", "rate_unique_id", "rate_sequence")

    dedupe_note = (
        "one row per contract/rate per feed; seasonal rates may need the season "
        "window in the key rather than a single row per rate_unique_id"
    )

    # SPEC: placeholder model — no real target table exists yet.
