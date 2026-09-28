"""
std_base.py
===========
Shared base for the standardization phase (p4 of stage 3). Not a
transformation itself -- it registers nothing.

WHAT IT DOES
------------
Rewrites, IN PLACE, the descriptive location-purpose fields of a feed's
decomposed locations table for the pipelines that need it:

    silver_staging.firm_locations   (rows where tspduns has a rule)
        locpurpdesc, locqti, locqtidesc  <-  LocationStandardization.by_duns

The raw `locpurp` code is never changed. It is what the feed said, and stage 4
keys on it; what is standardized is the MEANING the feed left ambiguous, which
is what stage 5 carries onto the master capacity model.

WHY IN PLACE, AND WHY INCREMENTAL
---------------------------------
Stage 4 reads `<DECOMP_SCHEMA>.<feed>_locations` by name (LocationsSource in
core/table_config.py). Writing a second, standardized table would mean either
repointing stage 4 or carrying two copies of every location; updating the
existing rows keeps one table and no downstream change.

That makes the target a table this transformation does not own, so it is
`incremental = True`: the runner never drops it on --reload (that would throw
away decompisition(p3)'s output) and never load-once skips it. The UPDATE is
self-healing rather than marker-based -- it rewrites any row whose fields
differ from the rule, so a rerun after p3 has refreshed the raw values
re-applies the standardization instead of trusting a stale audit column.
Rerunning with nothing to change updates 0 rows.

WHERE THE RULES LIVE
--------------------
core/table_config.py (LocationStandardization): DUNS -> code -> values. Add a
pipeline there; nothing here changes.
"""

from __future__ import annotations

from typing import List

from ....core.base import PipelineTransformation
from ....core.table_config import LocationStandardization as LS


def _lit(value: str) -> str:
    """A SQL string literal, quotes doubled."""
    return "'" + value.replace("'", "''") + "'"


class LocationPurposeStandardization(PipelineTransformation):
    # --- set these in each subclass ------------------------------------------
    feed: str = ""               # "firm" | "interruptible"
    locations_table: str = ""    # the decomposed locations table this rewrites

    #: Column carrying the TSP's DUNS on the locations row (same on every
    #: gTRAN feed; awards/index locations carry no TSP identity and have no
    #: standardization class).
    duns_col: str = "tspduns"

    #: The target is decompisition(p3)'s table -- see the module docstring.
    incremental = True

    def __init__(self) -> None:
        for attr in ("feed", "locations_table"):
            if not getattr(self, attr):
                raise ValueError(f"{type(self).__name__} must set `{attr}`.")
        self.source = self.feed
        self.table_name = self.locations_table       # written in place
        self.bronze_sources = [self.locations_table]  # and required to exist
        super().__init__()

    @property
    def source_schema(self) -> str:
        return self.decomp_schema

    @property
    def target_schema(self) -> str:
        return self.decomp_schema

    # ------------------------------------------------------------------ rules
    @staticmethod
    def rule_rows() -> List[str]:
        """The rule table as SQL VALUES rows: (duns, code, purpdesc, qti, qtidesc, rule)."""
        rows = []
        for duns, codes in LS.by_duns.items():
            for code, (purpdesc, qti, qtidesc) in codes.items():
                rows.append("(%s, %s, %s, %s, %s, %s)" % (
                    _lit(duns.strip()), _lit(code.strip().upper()),
                    _lit(purpdesc), _lit(qti), _lit(qtidesc),
                    _lit(f"{duns.strip()}:{code.strip().upper()}"),
                ))
        return rows

    # ------------------------------------------------------------------ DDL
    def create_table_sql(self) -> str:
        """The table already exists (p3 made it); only the audit column is ours."""
        return f"""
        ALTER TABLE {self.target_schema}.{self.table_name}
            ADD COLUMN IF NOT EXISTS {LS.rule_col} TEXT;
        """

    # ------------------------------------------------------------ transform
    def transform_sql(self) -> str:
        tbl = f"{self.target_schema}.{self.table_name}"
        rows = self.rule_rows()
        if not rows:
            # No pipeline configured: a no-op that still parses and returns 0.
            return f"UPDATE {tbl} SET {LS.rule_col} = {LS.rule_col} WHERE FALSE"
        values = ",\n            ".join(rows)
        return f"""
        -- One VALUES row per (pipeline, code) rule from core/table_config.py.
        -- A row is rewritten only when its fields differ from the rule, so the
        -- statement is idempotent and re-applies after p3 refreshes the raw
        -- values (see std_base.py).
        UPDATE {tbl} s
        SET locpurpdesc  = v.purpdesc,
            locqti       = v.qti,
            locqtidesc   = v.qtidesc,
            {LS.rule_col} = v.rule
        FROM (VALUES
            {values}
        ) AS v(duns, code, purpdesc, qti, qtidesc, rule)
        WHERE btrim(coalesce(s.{self.duns_col}, '')) = v.duns
          AND upper(btrim(coalesce(s.locpurp, ''))) = v.code
          AND (   s.locpurpdesc IS DISTINCT FROM v.purpdesc
               OR s.locqti      IS DISTINCT FROM v.qti
               OR s.locqtidesc  IS DISTINCT FROM v.qtidesc
               OR s.{LS.rule_col} IS DISTINCT FROM v.rule)
        """
