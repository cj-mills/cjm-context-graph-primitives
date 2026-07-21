"""The journal core: envelope-agnostic append (append_op) + the append_write wrapper."""

from cjm_context_graph_primitives.journal import (append_op, append_write, journal_segments,
                                                  maybe_rotate, read_journal, rotate_journal)


def test_append_op_envelope_rides_verbatim(tmp_path):
    """Domain envelope fields (actor / set / anchor / explicit ts) are never stomped."""
    j = str(tmp_path / "w.jsonl")
    op = {"verb": "boundary-shift", "id": "op-1", "actor": "import:pass1-baseline",
          "set": "sess-9", "anchor": {"source": "sha256:x", "spans": [[1.0, 2.5]]},
          "ts": 123.0, "args": {"direction": "pull"}}
    assert append_op(j, op)
    rec = read_journal(j)[0]
    assert rec["actor"] == "import:pass1-baseline" and rec["ts"] == 123.0
    assert rec["anchor"]["spans"] == [[1.0, 2.5]] and rec["set"] == "sess-9"


def test_append_op_dedup_by_id_then_exact(tmp_path):
    """`id` wins dedup when present; otherwise exact (verb, args); dedup=False always appends."""
    j = str(tmp_path / "w.jsonl")
    assert append_op(j, {"verb": "v", "id": "op-1", "args": {"a": 1}})
    assert not append_op(j, {"verb": "v", "id": "op-1", "args": {"a": 2}})  # same id, changed args
    assert append_op(j, {"verb": "v", "args": {"a": 1}})        # no id -> exact-match lane
    assert not append_op(j, {"verb": "v", "args": {"a": 1}})    # exact duplicate
    assert append_op(j, {"verb": "v", "args": {"a": 1}}, dedup=False)  # bulk path skips the rescan
    assert len(read_journal(j)) == 3


def test_append_write_rides_append_op(tmp_path):
    """The dev-graph wrapper keeps its historical contract: (verb, args) dedup + ts stamp."""
    j = str(tmp_path / "w.jsonl")
    assert append_write(j, "decide", {"statement": "x"})
    assert not append_write(j, "decide", {"statement": "x"})
    recs = read_journal(j)
    assert len(recs) == 1 and recs[0]["verb"] == "decide" and "ts" in recs[0]


def test_rotation_family_reads_as_one_journal(tmp_path):
    """Rotation is a FILE-level repartition: family read == unrotated append order."""
    j = str(tmp_path / "w.writes.jsonl")
    append_op(j, {"verb": "v", "id": "op-1", "args": {"n": 1}})
    cold1 = rotate_journal(j)
    assert cold1.endswith("w.writes.0001.jsonl")
    append_op(j, {"verb": "v", "id": "op-2", "args": {"n": 2}})
    cold2 = rotate_journal(j)
    assert cold2.endswith("w.writes.0002.jsonl")
    append_op(j, {"verb": "v", "id": "op-3", "args": {"n": 3}})
    assert journal_segments(j) == [cold1, cold2, j]
    assert [op["id"] for op in read_journal(j)] == ["op-1", "op-2", "op-3"]
    assert rotate_journal(str(tmp_path / "missing.jsonl")) is None  # empty/missing tail -> no-op


def test_maybe_rotate_budget_and_cross_segment_dedup(tmp_path):
    """The post-append check closes an over-budget tail; dedup sees cold segments."""
    j = str(tmp_path / "w.writes.jsonl")
    append_op(j, {"verb": "v", "id": "op-1", "args": {"blob": "x" * 64}})
    assert maybe_rotate(j, max_bytes=32) is not None   # tail over budget -> segment closed
    assert maybe_rotate(j, max_bytes=32) is None       # no tail -> nothing to close
    append_op(j, {"verb": "v", "id": "op-2", "args": {}})
    assert not append_op(j, {"verb": "v", "id": "op-1", "args": {}})  # id dedup spans segments
    assert len(journal_segments(j)) == 2 and len(read_journal(j)) == 2
