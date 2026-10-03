"""Regression: a batch marker only proves a write if no destructive commit happened after it.

Found by the backfill experiment: after Silver was wiped, a backfill chunk whose event set equalled an
earlier micro-batch was skipped because the old marker was still in the snapshot history.
"""

from __future__ import annotations

from spark.common.pipeline import already_committed


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class FakeSpark:
    def __init__(self, snapshots_newest_first):
        self.snaps = [{"k": k, "operation": op} for k, op in snapshots_newest_first]

    def sql(self, _q):
        return _Rows(self.snaps)


def test_marker_found_in_current_lineage():
    assert already_committed(FakeSpark([("b2", "append"), ("b1", "append")]), "silver.x", "b1")


def test_marker_before_a_delete_does_not_count():
    assert not already_committed(FakeSpark([(None, "delete"), ("b1", "overwrite")]), "silver.x", "b1")


def test_marker_before_an_unkeyed_overwrite_does_not_count():
    assert not already_committed(FakeSpark([(None, "overwrite"), ("b1", "append")]), "silver.x", "b1")


def test_compaction_does_not_invalidate_marker():
    assert already_committed(FakeSpark([(None, "replace"), ("b1", "append")]), "silver.x", "b1")


def test_our_own_keyed_overwrite_is_lineage():
    assert already_committed(FakeSpark([("b2", "overwrite"), ("b1", "overwrite")]), "silver.x", "b1")


def test_unknown_key_is_not_committed():
    assert not already_committed(FakeSpark([("b2", "append")]), "silver.x", "b9")
