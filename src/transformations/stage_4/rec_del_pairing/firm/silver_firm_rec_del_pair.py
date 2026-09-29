"""
silver_firm_rec_del_pair.py
===========================
Rec-del pairing for FIRM transport:
`<DECOMP_SCHEMA>.firm_locations` -> `silver.firm_rec_del_pair`.

Source: the firm locations table as stage 3 leaves it -- decomposed by
decompisition(p3), then standardized in place by standardization(p4), which
rewrites Transco's S8 / S9 segment ends into the feed's own delivery / receipt
descriptions. Every column of that row is carried through; the base appends
the pairing and its group id (see pairing_base.py).
"""

from __future__ import annotations

from ..pairing_base import RecDelPairingTransformation
from .....core.registry import register


@register
class SilverFirmRecDelPair(RecDelPairingTransformation):
    name = "silver_firm_rec_del_pair"
    table_name = "firm_rec_del_pair"
    entity = "firm"

    column_map = {
        **RecDelPairingTransformation.column_map,
        "contract_key": "firmid",
        # Quoted: `index` is a SQL keyword.
        "loc_index": '"index"',
    }
