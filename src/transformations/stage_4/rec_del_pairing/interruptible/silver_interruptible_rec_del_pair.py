"""
silver_interruptible_rec_del_pair.py
====================================
Rec-del pairing for INTERRUPTIBLE transport.

Source: the interruptible locations table produced by the decomposition phase.
All pairing and term logic lives in the shared base -- see pairing_base.py in this package,
where the two `SPEC:` hooks are waiting for the business rules.

TODO(confirm): the `column_map` overrides below are set to
the expected decomposition output. Verify both once that phase lands.
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
        "loc_qty": "itqtyloc",
    }
