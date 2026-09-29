"""
awards -- rec-del pairing
=========================

Receipt/delivery pairing for the AWARDS feed.

Its own package so this feed's pairing rules can diverge from the others:
the subclass here names the source columns the pairing reads and the purpose
values that mean receipt / delivery, and it affects only awards. Shared
mechanics stay in ../pairing_base.py.

Workflow:  (stage4)rec_del_pairing_awards.yml
           python run.py --group rec_del_pairing/awards
"""
