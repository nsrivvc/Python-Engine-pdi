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

TWO DERIVED VALUES
------------------
`season_beg_date` / `season_end_date` come from the location's own seasonal
window (SeasnlSt / SeasnlEnd) when the feed posts one, else from the
CONTRACT's term (KBegDateTime / KEndDateTime, carried onto every location row
as transactiontermbegindatetime / transactiontermenddatetime). Transco posts
no seasonal window at all, so every one of its rows shows the contract term.

`quantity` is the pairing's quantity. Transco puts the volume on the delivery
(S8) row and leaves the receipt (S9) row blank, so a row with no quantity of
its own takes its pairing's (same contract, same pair_group_id): both rows of
pair 1 read the same number, both rows of pair 2 the same number. A row that
has its own quantity keeps it; a row no pattern paired (pair_group_id NULL)
is left as posted.

That model lives in ../../models.py and is shared with the FINAL
transformations, which UNION every feed's table for this grain into one.

Unmapped target columns are emitted as typed NULLs -- this feed has no award,
offer, bid or capacity-release fields.
"""

from __future__ import annotations

from ...master_base import MasterCapacityTransformation
from ......core.registry import register

#: The location's own quantity, as posted.
_QTY = "NULLIF(kqtyloc, '')::NUMERIC"


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
        # the pairing's quantity: own value, else the pair's (see above)
        "quantity": (
            f"CASE WHEN pair_group_id IS NULL THEN {_QTY} "
            f"ELSE coalesce({_QTY}, max({_QTY}) OVER (PARTITION BY firmid, pair_group_id)) END"
        ),
        "beg_date": "NULLIF(kentbegdatetime, '')::TIMESTAMPTZ",
        "end_date": "NULLIF(kentenddatetime, '')::TIMESTAMPTZ",
        # the location's seasonal window, else the contract's term (see above)
        "season_beg_date": "coalesce(NULLIF(seasnlst, '')::TIMESTAMPTZ, NULLIF(transactiontermbegindatetime, '')::TIMESTAMPTZ)",
        "season_end_date": "coalesce(NULLIF(seasnlend, '')::TIMESTAMPTZ, NULLIF(transactiontermenddatetime, '')::TIMESTAMPTZ)",
        "transaction_term_begin_datetime": "NULLIF(transactiontermbegindatetime, '')::TIMESTAMPTZ",
        "transaction_term_end_datetime": "NULLIF(transactiontermenddatetime, '')::TIMESTAMPTZ",
        "segment": "segment",
        "index": '"index"',
        "posted_date": "NULLIF(posteddatetime, '')::TIMESTAMPTZ",
        "update_date": "ingestion_timestamp",
        "source": "'gTRAN FIRM'",
    }
