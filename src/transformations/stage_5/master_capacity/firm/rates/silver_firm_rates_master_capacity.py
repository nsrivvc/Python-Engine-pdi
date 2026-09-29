"""
silver_firm_rates_master_capacity.py
====================================
Maps the FIRM feed's stage-3 `firm_rates` onto the shared master capacity
model, producing `silver.firm_rates_master_capacity`.

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
class SilverFirmRatesMasterCapacity(MasterCapacityTransformation):
    name = "silver_firm_rates_master_capacity"
    table_name = "firm_rates_master_capacity"
    feed = "firm"
    grain = "rates"
    source_table = "firm_rates"


    # Mappings follow the agreed Rates sheet (gTRAN FIRM column). Note the
    # sheet chooses the *description* fields for the code-ish targets: Rate
    # Identification Code <- RateIdDesc, Reporting Level <- RptLvlDesc, Rate
    # Charged Reference <- RateChgdRefDesc, Max Tariff Rate Reference <-
    # MaxTrfRateRefDesc. `negotiated_rate_indicator` reads the rate-level
    # NgtdRateIndRates the feed nests per rate element. Award-only targets
    # stay NULL for this feed.
    column_map = {
        "ngh_contract_id": "firmid",
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
        "source": "'gTRAN FIRM'",
    }
