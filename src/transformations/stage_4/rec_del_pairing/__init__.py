"""
rec_del_pairing
===============
Receipt/delivery pairing: takes the locations table stage 3 finishes with
(decomposed, then standardized in place) and writes it back out row for row
with the pairing attached -- which side each location is (R / D), the
contract's formation, the configured pattern that admitted it, a PAIRED / FAIL
status, and a `pair_group_id` that restarts at 1 per contract and ties the
locations of one pairing together.

PAIRING COVERS FIRM, INTERRUPTIBLE AND AWARDS. IOC has no stage 4 -- it is not
represented here and picks up again at stage 5.

    pairing_base.py           the pairing itself, shared by every feed
    firm/                     firm pairing
    interruptible/            interruptible (IT) pairing
    awards/                   awards pairing (dormant: no feed yet)

Each feed is its own package so their rules can diverge -- a subclass names
the source columns the pairing reads (`column_map`) and the purpose values
that mean receipt / delivery, and the other feeds are untouched. The parent
package discovers these automatically.
"""
