"""
silver_interruptible_rates.py
=============================
Decomposes the INTERRUPTIBLE feed's rates into `<DECOMP_SCHEMA>.interruptible_rates`.

Source: `interruptible_dedup` (deduplication(p1) output -- rates carry no
        amendment marker), exploded on its nested `rates` JSON array.
Key:    (interruptibleid, uniqueid, element_index)

Column list is explicit rather than introspected so `--show-sql` works without a
database. If the upstream table gains a column, add it here or it is not carried.
"""

from __future__ import annotations

from ...decomp_base import GrainDecomposition
from ......core.registry import register


@register
class SilverInterruptibleRates(GrainDecomposition):
    name = "silver_interruptible_rates"
    table_name = "interruptible_rates"
    feed = "interruptible"
    grain = "rates"
    source_table = "interruptible_dedup"
    key_cols_list = ["interruptibleid", "uniqueid", "element_index"]

    section = "rates"
    parent_columns = ["interruptibleid", "posteddatetime", "tspduns", "tspname"]

    element_keys = [
        "MaxDQ",
        "MinDQ",
        "SeasnlSt",
        "SeasnlEnd",
        "UniqueId",
        "Pk",
        "RateFormType",
        "RateFormTypeDesc",
        "ResRateBasis",
        "ResRateBasisDesc",
        "LocKMaxPress",
        "LocKMinPress",
        "MinVolPctNonCapRel",
        "MinVolQtyNonCapRel",
        "CapType",
        "CapTypeName",
        "CapTypeLoc",
        "CapTypeLocDesc",
        "ItQtyLoc",
        "UniqueKey",
        "Id",
        "CreatedDateTime",
        "KEntBegDateTime",
        "KEntEndDateTime",
        "RecLoc",
        "RecLocName",
        "RecLocPurp",
        "RecLocPurpDesc",
        "RecLocZn",
        "DelLoc",
        "DelLocName",
        "DelLocPurp",
        "DelLocPurpDesc",
        "DelLocZn",
        "LocQTI",
        "LocQTIDesc",
        "RateId",
        "RateIdDesc",
        "RateChgd",
        "RateChgdRef",
        "RateChgdRefDesc",
        "MaxTrfRate",
        "MaxTrfRateRef",
        "MaxTrfRateRefDesc",
        "MktBasedRateInd",
        "SurchgId",
        "SurchgIdDesc",
        "SurchgInd",
        "SurchgIndDesc",
        "TotSurchg",
        "DiscBegDateTime",
        "DiscEndDateTime",
        "RptLvl",
        "RptLvlDesc",
        "NgtdRateIndRates",
    ]

    columns = [
        "bronze_row_id",
        "element_index",
        "maxdq",
        "mindq",
        "seasnlst",
        "seasnlend",
        "interruptibleid",
        "uniqueid",
        "pk",
        "rateformtype",
        "rateformtypedesc",
        "resratebasis",
        "resratebasisdesc",
        "lockmaxpress",
        "lockminpress",
        "minvolpctnoncaprel",
        "minvolqtynoncaprel",
        "captype",
        "captypename",
        "captypeloc",
        "captypelocdesc",
        "itqtyloc",
        "uniquekey",
        "id",
        "createddatetime",
        "posteddatetime",
        "kentbegdatetime",
        "kentenddatetime",
        "recloc",
        "reclocname",
        "reclocpurp",
        "reclocpurpdesc",
        "recloczn",
        "delloc",
        "dellocname",
        "dellocpurp",
        "dellocpurpdesc",
        "delloczn",
        "locqti",
        "locqtidesc",
        "rateid",
        "rateiddesc",
        "ratechgd",
        "ratechgdref",
        "ratechgdrefdesc",
        "maxtrfrate",
        "maxtrfrateref",
        "maxtrfraterefdesc",
        "mktbasedrateind",
        "surchgid",
        "surchgiddesc",
        "surchgind",
        "surchginddesc",
        "totsurchg",
        "discbegdatetime",
        "discenddatetime",
        "rptlvl",
        "rptlvldesc",
        "tspduns",
        "tspname",
        "ngtdrateindrates",
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
