"""
silver_firm_rates.py
====================
Decomposes the FIRM feed's rates into `<DECOMP_SCHEMA>.firm_rates`.

Source: `firm_dedup` (deduplication(p1) output -- rates carry no amendment
        marker), exploded on its nested `rates` JSON array: one row per rate.
Key:    (firmid, uniqueid, element_index)

`element_keys` is the schema of one rate object in the payload, verbatim; each
becomes the lowercase column named in `columns` below. `firmid`,
`posteddatetime`, `tspduns` and `tspname` live on the contract row rather than
in the element, so they are carried via `parent_columns`.

Column list is explicit rather than introspected so `--show-sql` works without a
database. If the upstream table gains a column, add it here or it is not carried.
"""

from __future__ import annotations

from ...decomp_base import GrainDecomposition
from ......core.registry import register


@register
class SilverFirmRates(GrainDecomposition):
    name = "silver_firm_rates"
    table_name = "firm_rates"
    feed = "firm"
    grain = "rates"
    source_table = "firm_dedup"
    key_cols_list = ["firmid", "uniqueid", "element_index"]

    section = "rates"
    parent_columns = ["firmid", "posteddatetime", "tspduns", "tspname"]

    element_keys = [
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
        "KQtyLoc",
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
        "seasnlst",
        "seasnlend",
        "firmid",
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
        "kqtyloc",
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
