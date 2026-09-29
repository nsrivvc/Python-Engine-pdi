"""
silver_awards_rates.py
======================
Decomposes the AWARDS feed's rates into `<DECOMP_SCHEMA>.awards_rates`.

Source: `awards_dedup`, exploded on its nested `rates` JSON array.
Key:    (id, locationpropcode, locationpurpose)

Rates carry no amendment marker, so this reads deduplication(p1) directly.

Column list is explicit rather than introspected so `--show-sql` works without a
database. If the upstream table gains a column, add it here or it is not carried.
"""

from __future__ import annotations

from ...decomp_base import GrainDecomposition
from ......core.registry import register


@register
class SilverAwardsRates(GrainDecomposition):
    name = "silver_awards_rates"
    table_name = "awards_rates"
    feed = "awards"
    grain = "rates"
    source_table = "awards_dedup"
    key_cols_list = ["id", "locationpropcode", "locationpurpose", "element_index"]

    section = "rates"
    parent_columns = ["postdatetime", "capacityawarddatetime", "releasetermstartdate", "releasetermenddate"]

    element_keys = [
        "GS_ID",
        "Id",
        "OfferNumber",
        "BidNumber",
        "AwardNumber",
        "BidderDuns",
        "ReleaserDuns",
        "TransportationServiceProviderPropCode",
        "LocationPurpose",
        "LocationPurposeCodeValue",
        "LocationName",
        "LocationPropCode",
        "IdentificationCodeQualifier",
        "ReservationRateBasis",
        "MarketBasedRateIndicator",
        "SurchargeIndicatorCodeValue",
        "SurchargeIndicator",
        "ChargeInformationReferenceNumber",
        "ChargeCode",
        "ChargeRate",
        "AwardRate",
        "AwardRateIdentificationCode",
        "MaximumTariffRate",
        "MaximumTariffRateIdentificationCode",
        "AwardPercentageOfMaximumTariffRate",
        "AwardPercentageOfMaximumTariffRateIdentificationCode",
        "MinimumVolumetricCommitmentPercentage",
        "IBRAllowableDifferential",
        "IBRAllowableDifferentialRateFloor",
        "IBRBidValuePercent",
        "CreatedDate",
        "Version_Status",
        "UpdatedDateTime",
    ]

    columns = [
        "bronze_row_id",
        "element_index",
        "postdatetime",
        "capacityawarddatetime",
        "releasetermstartdate",
        "releasetermenddate",
        "gs_id",
        "id",
        "offernumber",
        "bidnumber",
        "awardnumber",
        "bidderduns",
        "releaserduns",
        "transportationserviceproviderpropcode",
        "locationpurpose",
        "locationpurposecodevalue",
        "locationname",
        "locationpropcode",
        "identificationcodequalifier",
        "reservationratebasis",
        "marketbasedrateindicator",
        "surchargeindicatorcodevalue",
        "surchargeindicator",
        "chargeinformationreferencenumber",
        "chargecode",
        "chargerate",
        "awardrate",
        "awardrateidentificationcode",
        "maximumtariffrate",
        "maximumtariffrateidentificationcode",
        "awardpercentageofmaximumtariffrate",
        "awardpercentageofmaximumtariffrateidentificationcode",
        "minimumvolumetriccommitmentpercentage",
        "ibrallowabledifferential",
        "ibrallowabledifferentialratefloor",
        "ibrbidvaluepercent",
        "createddate",
        "version_status",
        "updateddatetime",
        "raw_record_id",
        "hash_key",
        "pipeline_run_id",
        "source_system",
        "source_api",
        "source_file_name",
        "ingestion_timestamp",
        "updated_ts",
        "ingestion_status",
        "raw_payload",
    ]
