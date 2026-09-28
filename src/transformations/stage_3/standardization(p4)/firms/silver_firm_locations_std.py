"""
silver_firm_locations_std.py
============================
Standardizes the FIRM feed's location-purpose descriptors in
`<DECOMP_SCHEMA>.firm_locations`, in place, for the pipelines listed in
core/table_config.py (LocationStandardization).

Today that is Transco (DUNS 007933021): its S8 segment ends are read as the
delivery location and its S9 ends as the receipt location, taking the same
`locpurpdesc` / `locqti` / `locqtidesc` the feed puts on its own MQ / M2 rows.
Every other pipeline's rows are untouched.

Runs as the fourth stage-3 phase, after decompisition(p3) has built the table
and before stage 4 reads it. Reruns are no-ops once the rows match the rule.
"""

from __future__ import annotations

from ..std_base import LocationPurposeStandardization
from .....core.registry import register


@register
class SilverFirmLocationsStandardized(LocationPurposeStandardization):
    name = "silver_firm_locations_std"
    feed = "firm"
    locations_table = "firm_locations"
