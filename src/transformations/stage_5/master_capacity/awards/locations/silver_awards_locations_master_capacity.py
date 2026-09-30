"""
silver_awards_locations_master_capacity.py
==========================================
Maps the AWARDS feed's locations onto the shared master capacity model,
producing `silver.awards_locations_master_capacity`.

Same shape as the firm grain (see ../../firm/locations/): the source is stage
4's `silver.awards_rec_del_pair` -- the stage-3 locations row carried whole,
plus the pairing -- read row for row with no latest-wins step, written in
stage 4's order. `group` numbers the awards in this table (1..N in award-number
order, every row of an award carrying its number) and `pair_group_id` is stage
4's receipt/delivery pairing id, carried as is. As for firm, the seasonal
window falls back to the award's release term when the element posts none,
and a row with no quantity of its own takes its pairing's.

That model lives in ../../models.py and is shared with the FINAL
transformations, which UNION every feed's table for this grain into one.

`ngh_contract_id` is **AwardNumber** for all three awards grains. The award's own
`Id` identifies the core row, but the locations and rates elements do not carry
it -- they carry OfferNumber / BidNumber / AwardNumber. AwardNumber is the only
key present at every grain, so it is what lets the three join.

Unmapped target columns are emitted as typed NULLs. `SPEC:` markers flag the
mappings that are inferred rather than confirmed against the mapping sheet.

NOT MAPPED: `zone`, `segment` and `index` -- the awards locations schema has no
zone, no segment and no index, where firm/IT have loczn / segment / Index.
"""

from __future__ import annotations

from ...master_base import MasterCapacityTransformation
from ......core.registry import register

#: The location's own quantity, as posted.
_QTY = "NULLIF(awardquantitylocation, '')::NUMERIC"


@register
class SilverAwardsLocationsMasterCapacity(MasterCapacityTransformation):
    name = "silver_awards_locations_master_capacity"
    table_name = "awards_locations_master_capacity"
    feed = "awards"
    grain = "locations"

    # Stage 4's output, in the Silver schema (see source_schema below).
    source_table = "awards_rec_del_pair"

    # One row out per stage-4 row, in stage 4's order. Nothing is collapsed.
    dedupe = False
    source_order = "rec_del_row_id"

    @property
    def source_schema(self) -> str:
        """Stage 4 writes to the Silver schema; this reads it."""
        return self.silver_schema

    column_map = {
        "ngh_contract_id": "awardnumber",
        "group": "dense_rank() OVER (ORDER BY awardnumber)::TEXT",
        "pair_group_id": "pair_group_id",
        "location": "locationpropcode",
        "location_name": "locationname",
        "location_qti": "locationquantitytypeindicator",
        "location_purpose_code": "locationpurposecode",
        "capacity_type": "capacitytypelocationindicator",
        # the pairing's quantity: own value, else the pair's
        "quantity": (
            f"CASE WHEN pair_group_id IS NULL THEN {_QTY} "
            f"ELSE coalesce({_QTY}, max({_QTY}) OVER (PARTITION BY awardnumber, pair_group_id)) END"
        ),
        # Elements carry only a SEASONAL window; the contract window is the
        # award-level release term, carried down as a parent column, which is
        # also what the seasonal window falls back to when an element has none.
        "beg_date": "NULLIF(releasetermstartdate, '')::TIMESTAMPTZ",
        "end_date": "NULLIF(releasetermenddate, '')::TIMESTAMPTZ",
        "season_beg_date": "coalesce(NULLIF(seasonalstartdate, '')::TIMESTAMPTZ, NULLIF(releasetermstartdate, '')::TIMESTAMPTZ)",
        "season_end_date": "coalesce(NULLIF(seasonalenddate, '')::TIMESTAMPTZ, NULLIF(releasetermenddate, '')::TIMESTAMPTZ)",
        "transaction_term_begin_datetime": "NULLIF(releasetermstartdate, '')::TIMESTAMPTZ",
        "transaction_term_end_datetime": "NULLIF(releasetermenddate, '')::TIMESTAMPTZ",
        "posted_date": "NULLIF(postdatetime, '')::TIMESTAMPTZ",
        "update_date": "updated_ts",
        "source": "'gAWD'",
    }
