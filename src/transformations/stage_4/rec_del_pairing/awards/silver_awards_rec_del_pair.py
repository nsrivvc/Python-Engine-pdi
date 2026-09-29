"""
silver_awards_rec_del_pair.py
=============================
Rec-del pairing for the AWARDS feed: `<DECOMP_SCHEMA>.awards_locations` ->
`silver.awards_rec_del_pair`.

Every column of the stage-3 row is carried through; the base appends the
pairing and its group id (see pairing_base.py).

COLUMN MAP
----------
The awards locations grain has its own agreed schema and shares no column names
with firm/IT, so the columns the pairing READS are remapped:

    contract_key   awardnumber          the award, not the location. Each element
                                        carries its own `Id`
                                        ("...-AWARD-2026-000001-LOC-01"), which
                                        identifies the LOCATION -- pairing needs
                                        the thing both sides have in common.
    loc_code       locationpropcode
    loc_index      None                 no array index; locations order by code
    loc_purpose    locationpurposecode  already 'REC' / 'DEL' (see below)
    tsp_duns       None                 awards locations carry no TSP, so only
                                        the 'default' patterns (DUNS 0) apply
"""

from __future__ import annotations

from ..pairing_base import RecDelPairingTransformation
from .....core.registry import register


@register
class SilverAwardsRecDelPair(RecDelPairingTransformation):
    name = "silver_awards_rec_del_pair"
    table_name = "awards_rec_del_pair"
    entity = "awards"

    # The awards feed labels purpose by code, not by the gTRAN description.
    receipt_purpose = "REC"
    delivery_purpose = "DEL"

    column_map = {
        **RecDelPairingTransformation.column_map,
        "contract_key": "awardnumber",
        "loc_code": "locationpropcode",
        "loc_index": None,                      # no array index in the awards feed
        "loc_purpose": "locationpurposecode",
        "tsp_duns": None,                       # awards locations carry no TSP
    }
