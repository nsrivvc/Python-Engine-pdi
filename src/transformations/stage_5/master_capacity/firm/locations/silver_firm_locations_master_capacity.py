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

Every column it reads is a stage-3 locations column, which stage 4 carries
through untouched, except the two grouping columns below.

THE TWO GROUPING COLUMNS
------------------------
`group` numbers the CONTRACTS in this table: 1 for the first contract, 2 for
the next, and so on, with every location row of a contract carrying its
contract's number. Ten contracts give exactly ten group ids, 1 to 10, however
many rows each contract has (three rows of group 1, four of group 2, ...).
Contracts are numbered in contract-id order, so the numbering is stable across
rebuilds as long as the contract set is. Computed here.

`pair_group_id` is stage 4's receipt/delivery pairing id, carried as is. It
restarts at 1 inside every contract and tells which receipt goes with which
delivery (R-D-R-D -> 1,1,2,2); NULL when no pattern admitted the contract.

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
    # contract key (firmid) tying this grain back to core; `group` and
    # `pair_group_id` are the two grouping columns described above.
    column_map = {
        "ngh_contract_id": "firmid",
        "group": "dense_rank() OVER (ORDER BY firmid)::TEXT",
        "pair_group_id": "pair_group_id",
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
