"""
silver_awards_rec_del_pair.py
=============================
Rec-del pairing for the AWARDS feed: `<DECOMP_SCHEMA>.awards_locations` ->
`silver.awards_rec_del_pair`.

See ../pairing_base.py for the pairing itself and the SPEC term hook.

COLUMN MAP
----------
The awards locations grain has its own agreed schema and shares no column names
with firm/IT, so every logical field is remapped:

    contract_key   awardnumber          the award, not the location. Each element
                                        carries its own `Id`
                                        ("...-AWARD-2026-000001-LOC-01"), which
                                        identifies the LOCATION -- pairing needs
                                        the thing both sides have in common.
    loc_code       locationpropcode
    loc_name       locationname
    loc_zone       None                 awards carries no zone at all; the output
                                        column stays NULL rather than borrowing
                                        an unrelated field (see pairing_base.ref)
    loc_purpose    locationpurposecode  already 'REC' / 'DEL', so the base's
                                        default purpose values apply unchanged
    loc_qti        locationquantitytypeindicator
    loc_qty        awardquantitylocation
    term_begin     releasetermstartdate the award-level release term, carried
    term_end       releasetermenddate   onto each element as a parent column
                                        (elements have only a SEASONAL window)

TODO(confirm): `term_begin` / `term_end` use the release term rather than the
element's SeasonalStartDate / SeasonalEndDate. The release term is the award's
actual contract window and matches what firm/IT pass through; the seasonal
dates are a narrower sub-window. Confirm which the term transform should see.
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
        "loc_name": "locationname",
        "loc_zone": None,                       # not present in the awards feed
        # Awards posts the code itself as REC / DEL, so the raw code and the
        # value classified on are the same column.
        "loc_purpose_code": "locationpurposecode",
        "loc_purpose": "locationpurposecode",
        "tsp_duns": None,                       # awards locations carry no TSP
        "loc_qti": "locationquantitytypeindicator",
        "loc_qty": "awardquantitylocation",
        "term_begin": "releasetermstartdate",
        "term_end": "releasetermenddate",
    }
