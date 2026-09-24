"""
silver_firm_rec_del_pair.py
===========================
Rec-del pairing for FIRM transport.

Source: the firm locations table produced by the decomposition phase.
All pairing and term logic lives in the shared base -- see pairing_base.py in this package,
where the two `SPEC:` hooks are waiting for the business rules.

TODO(confirm): the `column_map` overrides below are set to
the expected decomposition output. Verify both once that phase lands.
"""

from __future__ import annotations

from ..pairing_base import RecDelPairingTransformation
from .....core.registry import register


@register
class SilverFirmRecDelPair(RecDelPairingTransformation):
    name = "silver_firm_rec_del_pair"
    table_name = "firm_rec_del_pair"
    entity = "firm"

    # The firm feed spells location purpose with the NAESB codes, not the
    # REC/DEL default in table_config: M2 is a receipt point, MQ a delivery.
    # Scoped to this subclass -- interruptible and awards still carry the
    # default and need the same call made against their own data.
    receipt_purpose = "M2"
    delivery_purpose = "MQ"

    column_map = {
        **RecDelPairingTransformation.column_map,
        "contract_key": "firmid",
        # Quoted: `index` is a SQL keyword.
        "loc_index": '"index"',
        "loc_qty": "kqtyloc",
    }
