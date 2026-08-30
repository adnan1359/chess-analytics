"""Transform helpers shared by the batch and streaming paths.

Division of labour, so nothing is implemented twice:

* **Batch (Silver/Gold)** is pure ELT — all aggregate derivation happens in
  BigQuery SQL under ``sql/``. Python does not transform batch rows.
* **Streaming (Sprint 3)** needs the *move sequence* out of a PGN to replay a
  game event-by-event. That's what :mod:`chess_analytics.transforms.pgn`
  provides, plus the small scalar derivations the simulator must stamp on each
  event (outcome, termination, time control, Elo bracket).

The scalar derivations mirror CASE expressions in the Silver SQL. Both read
their vocabulary from ``config/mappings.yaml`` and
``tests/test_mapping_drift.py`` guards them against drifting apart.
"""
