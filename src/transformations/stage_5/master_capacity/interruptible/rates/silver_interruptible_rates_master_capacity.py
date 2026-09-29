"""
silver_interruptible_rates_master_capacity.py
=============================================
Maps the INTERRUPTIBLE feed's stage-3 `interruptible_rates` onto the shared master capacity
model, producing `silver.interruptible_rates_master_capacity`.

That model lives in ../../models.py and is shared with the FINAL
transformations, which UNION every feed's table for this grain into one.

Unmapped target columns are emitted as typed NULLs -- this feed has no award,
offer, bid or capacity-release fields. `SPEC:` markers below flag mappings that
are inferred rather than confirmed.
"""

from __future__ import annotations

from ...master_base import MasterCapacityTransformation
from ......core.registry import register


@register
class SilverInterruptibleRatesMasterCapacity(MasterCapacityTransformation):
    name = "silver_interruptible_rates_master_capacity"
    table_name = "interruptible_rates_master_capacity"
    feed = "interruptible"
    grain = "rates"
    source_table = "interruptible_rates"


    # Mirror of the firm map (see ../../firm/rates/), per the agreed Rates
    # sheet's gTRAN IT column. The IT-only MaxDQ / MinDQ fields have no target
    # on the sheet and are not carried.
    column_map = {
        "ngh_contract_id": "interruptibleid",
        "rate_unique_id": "uniqueid",
        "rate_sequence": "element_index",
        "rate_identification_code": "rateiddesc",
        "reporting_level": "rptlvldesc",
        "rate_charged": "NULLIF(ratechgd, '')::NUMERIC",
        "rate_charged_reference": "ratechgdrefdesc",
        "maximum_tariff_rate": "NULLIF(maxtrfrate, '')::NUMERIC",
        "reservation_rate_basis_desc": "resratebasisdesc",
        "beg_date": "NULLIF(kentbegdatetime, '')::TIMESTAMPTZ",
        "end_date": "NULLIF(kentenddatetime, '')::TIMESTAMPTZ",
        "receipt_location": "recloc",
        "receipt_location_name": "reclocname",
        "receipt_zone": "recloczn",
        "receipt_location_purpose": "reclocpurp",
        "delivery_location": "delloc",
        "delivery_location_name": "dellocname",
        "delivery_zone": "delloczn",
        "delivery_location_purpose": "dellocpurp",
        "discount_beg_date": "NULLIF(discbegdatetime, '')::TIMESTAMPTZ",
        "discount_end_date": "NULLIF(discenddatetime, '')::TIMESTAMPTZ",
        "season_beg_date": "NULLIF(seasnlst, '')::TIMESTAMPTZ",
        "season_end_date": "NULLIF(seasnlend, '')::TIMESTAMPTZ",
        "max_tariff_rate_reference": "maxtrfraterefdesc",
        "market_based_rate_indicator": "mktbasedrateind",
        "negotiated_rate_indicator": "ngtdrateindrates",
        "surcharge_identification_description": "surchgiddesc",
        "surcharge_indicator": "surchgind",
        "surcharge_indicator_description": "surchginddesc",
        "posted_date": "NULLIF(posteddatetime, '')::TIMESTAMPTZ",
        "update_date": "updated_ts",
        "source": "'gTRAN IT'",
    }
