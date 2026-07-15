"""The journal core: envelope-agnostic append (append_op) + the append_write wrapper."""

from cjm_context_graph_primitives.journal import append_op, append_write, read_journal


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
