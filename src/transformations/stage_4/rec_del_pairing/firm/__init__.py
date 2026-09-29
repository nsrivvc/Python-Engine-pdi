"""
firm -- rec-del pairing
========================

Receipt/delivery pairing for the FIRM feed.

Its own package so this feed's pairing rules can diverge from the
others': the subclass here names the source columns the pairing reads and
the purpose values that mean receipt / delivery, and it affects only firm.
Shared mechanics stay in ../pairing_base.py.

Workflow:  (stage4)rec_del_pairing_firm.yml
           python run.py --group rec_del_pairing/firm
"""
