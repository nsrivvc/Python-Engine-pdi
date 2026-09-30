"""
silver_final_locations_master_capacity.py
=========================================
FINAL Locations — Master Capacity. Consolidates all four feeds' location master
capacity tables into one.

The column set follows the agreed Locations mapping sheet and lives in
../../models.py, shared with the per-feed transformations so the UNION can
never break on a column mismatch.
"""

from __future__ import annotations

from ..final_base import FinalMasterCapacityTransformation
from ...models import LOCATIONS_COLUMNS
from ......core.registry import register


@register
class SilverFinalLocationsMasterCapacity(FinalMasterCapacityTransformation):
    name = "silver_final_locations_master_capacity"
    table_name = "final_locations_master_capacity"
    grain = "locations"

    # Shared with the per-feed transformations so the UNION can never break
    # on a column mismatch. Single definition lives in ../../models.py.
    columns = LOCATIONS_COLUMNS

    # One row per contract/location/purpose/index per feed, mirroring the
    # per-feed natural key in models.py. Purpose is in the key because a
    # location can serve both receipt and delivery on one contract; index is
    # in it because the same location and purpose recur at several positions
    # of one contract's Locations array, and each is its own row.
    natural_key = ("source_type", "ngh_contract_id", "location", "location_purpose_code", "index")

    dedupe_note = "one row per contract/location/purpose/index per feed"
