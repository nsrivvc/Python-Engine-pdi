"""
silver_interruptible_rec_del_pair.py
====================================
Rec-del pairing for INTERRUPTIBLE transport:
`<DECOMP_SCHEMA>.interruptible_locations` -> `silver.interruptible_rec_del_pair`.

Source: the interruptible locations table as stage 3 leaves it. Every column
of that row is carried through; the base appends the pairing and its group id
(see pairing_base.py).
"""

from __future__ import annotations

from ..pairing_base import RecDelPairingTransformation
from .....core.registry import register


@register
class SilverInterruptibleRecDelPair(RecDelPairingTransformation):
    name = "silver_interruptible_rec_del_pair"
    table_name = "interruptible_rec_del_pair"
    entity = "interruptible"

    column_map = {
        **RecDelPairingTransformation.column_map,
        "contract_key": "interruptibleid",
        # Quoted: `index` is a SQL keyword.
        "loc_index": '"index"',
    }
