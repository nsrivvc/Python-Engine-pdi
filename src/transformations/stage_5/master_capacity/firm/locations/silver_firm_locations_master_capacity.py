"""
silver_firm_locations_master_capacity.py
========================================
Maps the FIRM feed's locations onto the shared master capacity model, producing
`silver.firm_locations_master_capacity`.

ROW FOR ROW WITH STAGE 4
------------------------
The source is stage 4's `silver.firm_rec_del_pair` -- the stage-3 locations
row carried whole, plus the pairing -- not the stage-3 table itself. Every row
stage 4 holds becomes exactly one row here: there is no latest-wins step
(`dedupe = False`), so a contract that lists the same delivery point at four
indexes keeps all four. The rows are written in stage 4's own order, so
`firm_locations_id` lines up with `rec_del_row_id`.

Every other column it reads is a stage-3 locations column, which stage 4
carries through untouched. `group` is the one column that comes from the
pairing itself: stage 4's `pair_group_id`, which restarts at 1 for every
contract and is shared by the locations of one receipt/delivery pairing
(R-D-R-D -> 1,1,2,2). It is NULL on a row no pattern admitted (pair_status
FAIL).

That model lives in ../../models.py and is shared with the FINAL
transformations, which UNION every feed's table for this grain into one.

Unmapped target columns are emitted as typed NULLs -- this feed has no award,
offer, bid or capacity-release fields.
"""

from __future__ import annotations

from ...master_base import MasterCapacityTransformation
from ......core.registry import register


@register
class SilverFirmLocationsMasterCapacity(MasterCapacityTransformation):
    name = "silver_firm_locations_master_capacity"
    table_name = "firm_locations_master_capacity"
    feed = "firm"
    grain = "locations"

    # Stage 4's output, in the Silver schema (see source_schema below).
    source_table = "firm_rec_del_pair"

    # One row out per stage-4 row, in stage 4's order. Nothing is collapsed.
    dedupe = False
    source_order = "rec_del_row_id"

    @property
    def source_schema(self) -> str:
        """Stage 4 writes to the Silver schema; this reads it."""
        return self.silver_schema

    # Mappings follow the agreed Locations sheet (gTRAN FIRM column):
    # Location <- Loc, Location Purpose Code <- LocPurpDesc, Capacity Type <-
    # CapTypeName, Quantity <- KQtyLoc, and so on. `ngh_contract_id` is the
    # contract key (firmid) tying this grain back to core; `group` is the
    # rec-del pairing group id stage 4 assigned within that contract.
    column_map = {
        "ngh_contract_id": "firmid",
        "group": "pair_group_id::TEXT",
        "location": "loc",
        "location_name": "locname",
        "zone": "loczn",
        "location_qti": "locqti",
        "location_purpose_code": "locpurpdesc",
        "capacity_type": "captypename",
        "quantity": "NULLIF(kqtyloc, '')::NUMERIC",
        "beg_date": "NULLIF(kentbegdatetime, '')::TIMESTAMPTZ",
        "end_date": "NULLIF(kentenddatetime, '')::TIMESTAMPTZ",
        "season_beg_date": "NULLIF(seasnlst, '')::TIMESTAMPTZ",
        "season_end_date": "NULLIF(seasnlend, '')::TIMESTAMPTZ",
        "transaction_term_begin_datetime": "NULLIF(transactiontermbegindatetime, '')::TIMESTAMPTZ",
        "transaction_term_end_datetime": "NULLIF(transactiontermenddatetime, '')::TIMESTAMPTZ",
        "segment": "segment",
        "index": '"index"',
        "posted_date": "NULLIF(posteddatetime, '')::TIMESTAMPTZ",
        "update_date": "ingestion_timestamp",
        "source": "'gTRAN FIRM'",
    }
