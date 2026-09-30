"""
silver_interruptible_locations_master_capacity.py
=================================================
Maps the INTERRUPTIBLE feed's locations onto the shared master capacity model,
producing `silver.interruptible_locations_master_capacity`.

Same shape as the firm grain (see ../../firm/locations/): the source is stage
4's `silver.interruptible_rec_del_pair` -- the stage-3 locations row carried
whole, plus the pairing -- read row for row with no latest-wins step, written
in stage 4's order. `group` numbers the contracts in this table (1..N in
contract-id order, every row of a contract carrying its number) and
`pair_group_id` is stage 4's receipt/delivery pairing id, carried as is.

That model lives in ../../models.py and is shared with the FINAL
transformations, which UNION every feed's table for this grain into one.

Unmapped target columns are emitted as typed NULLs -- this feed has no award,
offer, bid or capacity-release fields.
"""

from __future__ import annotations

from ...master_base import MasterCapacityTransformation
from ......core.registry import register


@register
class SilverInterruptibleLocationsMasterCapacity(MasterCapacityTransformation):
    name = "silver_interruptible_locations_master_capacity"
    table_name = "interruptible_locations_master_capacity"
    feed = "interruptible"
    grain = "locations"

    # Stage 4's output, in the Silver schema (see source_schema below).
    source_table = "interruptible_rec_del_pair"

    # One row out per stage-4 row, in stage 4's order. Nothing is collapsed.
    dedupe = False
    source_order = "rec_del_row_id"

    @property
    def source_schema(self) -> str:
        """Stage 4 writes to the Silver schema; this reads it."""
        return self.silver_schema

    # Mirror of the firm map (see ../../firm/locations/), per the agreed
    # Locations sheet's gTRAN IT column: contract key interruptibleid,
    # quantity itqtyloc; everything else shares the firm feed's names.
    column_map = {
        "ngh_contract_id": "interruptibleid",
        "group": "dense_rank() OVER (ORDER BY interruptibleid)::TEXT",
        "pair_group_id": "pair_group_id",
        "location": "loc",
        "location_name": "locname",
        "zone": "loczn",
        "location_qti": "locqti",
        "location_purpose_code": "locpurpdesc",
        "capacity_type": "captypename",
        "quantity": "NULLIF(itqtyloc, '')::NUMERIC",
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
        "source": "'gTRAN IT'",
    }
