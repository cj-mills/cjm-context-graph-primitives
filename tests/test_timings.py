"""The instrument's collector (design detail 681604c1): phases, the storage table, series, the event."""

import asyncio
import json

import pytest
from cjm_context_graph_primitives.journal import append_op, read_journal
from cjm_context_graph_primitives.timings import (append_event, begin_run, CURRENT_RUN, end_run,
                                                  io_delta, read_io, record_append, record_replay,
                                                  record_series, record_storage, Run, span, timed,
                                                  timings_path)


def test_unarmed_is_a_no_op():
    """With no run armed every span / record is a no-op and nothing is retained."""
    assert CURRENT_RUN.get() is None
    with span("x") as counters:
        counters["rows"] = 3
    record_storage("query_nodes", 0.1, rows_out=5)
    record_series("replay:assert", 10, 0.1)
    assert CURRENT_RUN.get() is None


def test_spans_nest_and_aggregate_by_path():
    """Same-path spans fold into one row (calls, inclusive seconds, summed counters)."""
    run = begin_run("cli", "show")
    try:
        with span("verb"):
            for _ in range(3):
                with span("read_journal") as counters:
                    counters["ops"] = 2
    finally:
        end_run(run)
    assert CURRENT_RUN.get() is None
    assert run.phases["verb"]["calls"] == 1
    inner = run.phases["verb/read_journal"]
    assert inner["calls"] == 3 and inner["ops"] == 6
    assert run.phases["verb"]["s"] >= inner["s"]


def test_timed_wraps_sync_and_async():
    """The decorator measures each call as one phase, sync and async alike."""
    @timed("sync_step")
    def f(x):
        return x + 1

    @timed("async_step")
    async def g(x):
        return x * 2

    run = begin_run("cli", "t")
    try:
        assert f(1) == 2
        assert asyncio.run(g(3)) == 6
    finally:
        end_run(run)
    assert run.phases["sync_step"]["calls"] == 1
    assert run.phases["async_step"]["calls"] == 1


def test_storage_by_method_and_whole_scope_by_phase():
    """A storage call folds by method; a whole-scope one is also kept under its phase."""
    run = begin_run("cli", "t")
    try:
        with span("verb"):
            record_storage("query_nodes", 0.5, whole=True, rows_out=10, bytes_in=100)
            record_storage("query_nodes", 0.25, rows_in=2, rows_out=2)
    finally:
        end_run(run)
    row = run.storage["query_nodes"]
    assert row["calls"] == 2 and row["rows_out"] == 12 and row["bytes_in"] == 100
    assert run.whole == {"verb :: query_nodes": {"calls": 1, "s": 0.5}}


def test_series_bins_by_position():
    """A positioned cost lands in its bin, so a slope against position is readable."""
    run = begin_run("cli", "replay")
    try:
        for pos in (0, 5, 999, 1000, 2500):
            record_series("replay:assert", pos, 0.01)
    finally:
        end_run(run)
    bins = run.series["replay:assert"]
    assert {k: v["calls"] for k, v in bins.items()} == {"0": 3, "1000": 1, "2000": 1}


def test_event_shape_and_ledger_round_trip(tmp_path, monkeypatch):
    """The event carries identity, outcome, tables and covariates; it appends as one line."""
    monkeypatch.setenv("CJM_RUN_ID", "run-7")
    monkeypatch.setenv("CJM_ACTOR", "agent:test")
    run = begin_run("cli", "show", lane="dev")
    with span("verb"):
        pass
    end_run(run)
    ev = run.event(outcome=0, covariates={"nodes": 10})
    assert ev["verb"] == "show" and ev["kind"] == "cli" and ev["run_id"] == "run-7"
    assert ev["outcome"] == 0 and ev["covariates"] == {"nodes": 10}
    assert ev["attrs"] == {"lane": "dev"} and ev["actor"] == "agent:test"
    ledger = timings_path(str(tmp_path / "dev-graph.db"))
    assert ledger.endswith("dev-graph.timings.jsonl")
    assert append_op(ledger, ev, dedup=False)
    assert read_journal(ledger)[0]["phases"]["verb"]["calls"] == 1


def test_launch_and_import_from_the_wrapper_clock(monkeypatch):
    """A wrapper's CJM_T0 before this process started becomes the launch phase."""
    monkeypatch.setenv("CJM_T0", "1")  # long before this process: the launch is the whole gap
    run = begin_run("cli", "t")
    end_run(run)
    assert run.phases["launch"]["s"] > 0 and "import" in run.phases


def test_journal_reads_and_appends_are_phases(tmp_path):
    """The journal core measures its own whole-history reads (ops, bytes) and appends."""
    j = str(tmp_path / "w.jsonl")
    run = begin_run("cli", "t")
    try:
        append_op(j, {"verb": "v", "args": {"a": 1}})
        read_journal(j)
    finally:
        end_run(run)
    assert run.phases["append_op"]["calls"] == 1
    assert run.phases["append_op/read_journal"]["calls"] == 1   # the dedup scan, under its append
    top = run.phases["read_journal"]
    assert top["ops"] == 1 and top["bytes"] == len(json.dumps(read_journal(j)[0], sort_keys=True)) + 1


def test_every_run_carries_its_bench_tier(monkeypatch):
    """A scale-harness run is tagged at birth, whoever begins it (a CLI invocation, a gesture)."""
    monkeypatch.setenv("CJM_BENCH_TIER", "8")
    monkeypatch.setenv("CJM_BENCH_LANE", "dev-graph")
    assert Run("gesture", "app:g").attrs == {"tier": 8, "lane": "dev-graph"}
    monkeypatch.delenv("CJM_BENCH_TIER")
    assert Run("gesture", "app:g", app="app").attrs == {"app": "app"}


def test_append_event_is_o1_and_fail_open(tmp_path, capsys):
    """Every run is one event (dedup off); an unwritable ledger warns and never raises."""
    ledger = str(tmp_path / "g.timings.jsonl")
    ev = Run("gesture", "app:g").event(outcome="ok")
    assert append_event(ledger, ev) and append_event(ledger, ev)
    assert len(read_journal(ledger)) == 2
    assert append_event(str(tmp_path), ev) is False  # a directory: the open fails
    assert "timings-ledger append failed" in capsys.readouterr().err


def test_io_counters_and_journal_appends_ride_the_event(tmp_path):
    """Design detail 2f5033bf (1): a run's event carries the process's I/O over the run and its
    journal appends (ops and bytes per file) -- a duplicate append is never counted."""
    io = read_io()
    if io is None:
        pytest.skip("no /proc/self/io (Linux)")
    assert {"read_bytes", "write_bytes", "rchar", "wchar"} <= set(io)
    assert io_delta({"wchar": 5}, {"wchar": 12, "rchar": 3}) == {"wchar": 7, "rchar": 3}
    assert io_delta(None, io) is None
    journal = tmp_path / "x.writes.jsonl"
    run = begin_run("cli", "decide")
    try:
        append_op(str(journal), {"verb": "decide", "args": {"statement": "s"}})
        append_op(str(journal), {"verb": "decide", "args": {"statement": "s"}})  # a duplicate
        (tmp_path / "blob").write_bytes(b"x" * 4096)
    finally:
        end_run(run)
    ev = run.event(outcome=0)
    assert ev["appended"] == {"x.writes.jsonl": {"ops": 1, "bytes": journal.stat().st_size}}
    assert ev["io"]["wchar"] >= 4096 + journal.stat().st_size
    record_append(str(journal), 10)  # nothing armed: a no-op


def test_replayed_ops_ride_the_event_at_their_journaled_size(tmp_path):
    """Ruling 9af0e42e (3): a replay's logical size is the journal bytes of the ops it applied --
    each op folded at its journaled line's size, per journal file; a no-op when nothing is armed."""
    journal = tmp_path / "x.writes.jsonl"
    append_op(str(journal), {"verb": "decide", "args": {"statement": "s", "n": 1.5}})
    append_op(str(journal), {"verb": "assert", "args": {"subject": "a", "value": "é"}})
    ops = read_journal(str(journal))
    run = begin_run("cli", "replay")
    try:
        for op in ops:
            record_replay(str(journal), op)
    finally:
        end_run(run)
    ev = run.event(outcome=0)
    assert ev["replayed"] == {"x.writes.jsonl": {"ops": 2, "bytes": journal.stat().st_size}}
    assert "appended" not in ev
    record_replay(str(journal), ops[0])  # nothing armed: a no-op
