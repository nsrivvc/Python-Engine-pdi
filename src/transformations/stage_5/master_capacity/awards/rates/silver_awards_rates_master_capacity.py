"""
silver_awards_rates_master_capacity.py
======================================
Maps the AWARDS feed's stage-3 `awards_rates` onto the shared master capacity
model, producing `silver.awards_rates_master_capacity`.

That model lives in ../../models.py and is shared with the FINAL
transformations, which UNION every feed's table for this grain into one.

`ngh_contract_id` is **AwardNumber** for all three awards grains. The award's own
`Id` identifies the core row, but the locations and rates elements do not carry
it -- they carry OfferNumber / BidNumber / AwardNumber. AwardNumber is the only
key present at every grain, so it is what lets the three join.

Unmapped target columns are emitted as typed NULLs. `SPEC:` markers flag the
mappings that are inferred rather than confirmed against the mapping sheet.

ONE ROW PER LOCATION, NOT PER PATH. A firm rates row already names both ends of
the path (recloc / delloc); an awards rates element names ONE location and flags
it REC or DEL via `LocationPurpose`. The receipt_* / delivery_* columns are
therefore filled by CASE on that flag, leaving the other side NULL, rather than
inventing a pairing this grain does not express.

NOT MAPPED: `reporting_level`, `negotiated_rate_indicator`, the discount window
and the seasonal window -- none exist on an awards rate element.
"""

from __future__ import annotations

from ...master_base import MasterCapacityTransformation
from ......core.registry import register


@register
class SilverAwardsRatesMasterCapacity(MasterCapacityTransformation):
    name = "silver_awards_rates_master_capacity"
    table_name = "awards_rates_master_capacity"
    feed = "awards"
    grain = "rates"
    source_table = "awards_rates"

    column_map = {
        "ngh_contract_id": "awardnumber",
        # The element's own Id is the natural row key.
        "rate_unique_id": "id",
        "rate_sequence": "element_index",
        "rate_identification_code": "awardrateidentificationcode",
        # SPEC: AwardRate is the rate actually charged for the release;
        # ChargeRate / ChargeCode describe the charge component and are not mapped.
        "rate_charged": "NULLIF(awardrate, '')::NUMERIC",
        "maximum_tariff_rate": "NULLIF(maximumtariffrate, '')::NUMERIC",
        "max_tariff_rate_reference": "maximumtariffrateidentificationcode",
        "reservation_rate_basis_desc": "reservationratebasis",
        "award_percentage_of_max_tariff":
            "NULLIF(awardpercentageofmaximumtariffrate, '')::NUMERIC",
        "beg_date": "NULLIF(releasetermstartdate, '')::TIMESTAMPTZ",
        "end_date": "NULLIF(releasetermenddate, '')::TIMESTAMPTZ",
        # See the note above: one side is populated, the other stays NULL.
        "receipt_location":
            "CASE WHEN upper(locationpurpose) = 'REC' THEN locationpropcode END",
        "receipt_location_name":
            "CASE WHEN upper(locationpurpose) = 'REC' THEN locationname END",
        "receipt_location_purpose":
            "CASE WHEN upper(locationpurpose) = 'REC' THEN locationpurposecodevalue END",
        "delivery_location":
            "CASE WHEN upper(locationpurpose) = 'DEL' THEN locationpropcode END",
        "delivery_location_name":
            "CASE WHEN upper(locationpurpose) = 'DEL' THEN locationname END",
        "delivery_location_purpose":
            "CASE WHEN upper(locationpurpose) = 'DEL' THEN locationpurposecodevalue END",
        "market_based_rate_indicator": "marketbasedrateindicator",
        "surcharge_indicator": "surchargeindicator",
        "surcharge_indicator_description": "surchargeindicatorcodevalue",
        "posted_date": "NULLIF(postdatetime, '')::TIMESTAMPTZ",
        "update_date": "updated_ts",
        "source": "'gAWD'",
    }
