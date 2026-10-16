"""The write order (design detail 65f4a6c9): plan -> append -> apply -> advance under the family's
lock; a crash at either point is healed by the next writer to the uncrashed run's state."""

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from cjm_context_graph_primitives.journal import read_journal, rotate_journal
from cjm_context_graph_primitives.writer import (CursorMismatch, FAULT_EXIT, FAULT_HOOK,
                                                 JOURNAL_FAULT_ENV, journal_lag, JournalPosition,
                                                 JournalWriter, NoCursor, ORIGIN, read_tail,
                                                 stamp_cursor)


class MemCursors:
    """An in-memory cursor store."""

    def __init__(self):
        self.held = {}

    async def read(self, family):
        return self.held.get(family)

    async def record(self, family, position):
        self.held[family] = position


class FileCursors:
    """A cursor store in a JSON file -- shared across processes, written under the family lock."""

    def __init__(self, path):
        self.path = Path(path)

    def _load(self):
        return json.loads(self.path.read_text()) if self.path.exists() else {}

    async def read(self, family):
        d = self._load()
        return JournalPosition.from_dict(d[family]) if family in d else None

    async def record(self, family, position):
        d = self._load()
        d[family] = position.to_dict()
        tmp = f"{self.path}.tmp"
        Path(tmp).write_text(json.dumps(d))
        os.replace(tmp, self.path)


class MemDb:
    """A projection: an idempotent upsert per op id, plus the order applies ran in."""

    def __init__(self):
        self.rows, self.log = {}, []

    async def apply(self, record):
        await asyncio.sleep(0)
        self.rows[record["id"]] = record["args"]
        self.log.append(record["id"])


class Crash(BaseException):
    """A simulated crash: nothing on the write path catches it."""


def crash_at(point):
    def hook(p):
        if p == point:
            raise Crash(p)
    return hook


def put(i, prefix="op"):
    return {"verb": "put", "id": f"{prefix}-{i}", "args": {"i": i}}


async def write_all(j, cursors, db, ops):
    for op in ops:
        async with JournalWriter(j, cursors, db.apply) as w:
            await w.write(op)


def test_write_appends_applies_and_advances(tmp_path):
    j, cursors, db = str(tmp_path / "w.jsonl"), MemCursors(), MemDb()
    assert asyncio.run(stamp_cursor(j, cursors)) == ORIGIN
    asyncio.run(write_all(j, cursors, db, [put(i) for i in range(3)]))
    assert [r["id"] for r in read_journal(j)] == db.log == ["op-0", "op-1", "op-2"]
    end = cursors.held["w.jsonl"]
    assert end.ordinal == 3 and end.offset == os.path.getsize(j) and read_tail(j, end) == []


@pytest.mark.parametrize("point", ["after_append", "after_apply"])
def test_a_crash_is_healed_by_the_next_writer_to_the_uncrashed_state(tmp_path, point):
    # The proof shape of 65f4a6c9: crash between the steps, heal, compare with a run that never
    # crashed -- the journal, the db and the cursor all equal
    clean_j, clean_c, clean_db = str(tmp_path / "clean" / "w.jsonl"), MemCursors(), MemDb()
    asyncio.run(stamp_cursor(clean_j, clean_c))
    asyncio.run(write_all(clean_j, clean_c, clean_db, [put(i) for i in range(3)]))

    j, cursors, db = str(tmp_path / "w.jsonl"), MemCursors(), MemDb()
    asyncio.run(stamp_cursor(j, cursors))
    asyncio.run(write_all(j, cursors, db, [put(0)]))
    token = FAULT_HOOK.set(crash_at(point))
    try:
        with pytest.raises(Crash):
            asyncio.run(write_all(j, cursors, db, [put(1)]))
    finally:
        FAULT_HOOK.reset(token)
    assert cursors.held["w.jsonl"].ordinal == 1  # the cursor never passed the crashed record

    async def heal_then_write():
        async with JournalWriter(j, cursors, db.apply) as w:
            healed = w.healed
            await w.write(put(2))
        return healed

    assert asyncio.run(heal_then_write()) == 1
    assert db.rows == clean_db.rows
    assert [r["args"] for r in read_journal(j)] == [r["args"] for r in read_journal(clean_j)]
    mine, theirs = cursors.held["w.jsonl"], clean_c.held["w.jsonl"]
    assert (mine.segment, mine.ordinal) == (theirs.segment, theirs.ordinal)
    assert db.log.count("op-1") == (2 if point == "after_apply" else 1)  # a re-apply, absorbed


def test_the_cursor_crosses_a_rotation_into_a_compressed_segment(tmp_path):
    j, cursors, db = str(tmp_path / "w.jsonl"), MemCursors(), MemDb()
    asyncio.run(stamp_cursor(j, cursors))
    asyncio.run(write_all(j, cursors, db, [put(0), put(1)]))
    token = FAULT_HOOK.set(crash_at("after_append"))
    try:
        with pytest.raises(Crash):
            asyncio.run(write_all(j, cursors, db, [put(2)]))
    finally:
        FAULT_HOOK.reset(token)
    assert rotate_journal(j).endswith(".0001.jsonl.xz")  # the cursor's segment closes, compressed
    asyncio.run(write_all(j, cursors, db, [put(3)]))
    assert db.log == ["op-0", "op-1", "op-2", "op-3"]
    end = cursors.held["w.jsonl"]
    assert (end.segment, end.ordinal) == (2, 4) and read_tail(j, end) == []


def test_a_family_rewritten_under_the_cursor_is_refused(tmp_path):
    j, cursors, db = str(tmp_path / "w.jsonl"), MemCursors(), MemDb()
    asyncio.run(stamp_cursor(j, cursors))
    asyncio.run(write_all(j, cursors, db, [put(0), put(1)]))
    p = Path(j)
    p.write_text(p.read_text().replace('"i": 1', '"i": 9'))
    with pytest.raises(CursorMismatch):
        asyncio.run(write_all(j, cursors, db, [put(2)]))


def test_no_cursor_refuses_and_stamp_adopts_the_end(tmp_path):
    j, cursors, db = str(tmp_path / "w.jsonl"), MemCursors(), MemDb()
    Path(j).write_text(json.dumps(put(0), sort_keys=True) + "\n")  # a family the db already holds
    with pytest.raises(NoCursor):
        asyncio.run(write_all(j, cursors, db, [put(1)]))
    assert len(read_journal(j)) == 1 and db.log == []  # refused before anything was journaled
    end = asyncio.run(stamp_cursor(j, cursors))
    assert end.ordinal == 1 and asyncio.run(journal_lag(j, cursors)) == 0
    asyncio.run(write_all(j, cursors, db, [put(1)]))
    assert db.log == ["op-1"]  # the adopted record was not re-applied


def test_an_empty_family_needs_no_adoption(tmp_path):
    j, cursors, db = str(tmp_path / "w.jsonl"), MemCursors(), MemDb()
    asyncio.run(write_all(j, cursors, db, [put(0), put(1)]))  # no cursor, nothing journaled: the origin
    assert db.log == ["op-0", "op-1"] and cursors.held["w.jsonl"].ordinal == 2


def test_an_applied_first_record_is_passed_never_reapplied(tmp_path):
    from cjm_context_graph_primitives.journal import append_write
    j, cursors, db = str(tmp_path / "w.jsonl"), MemCursors(), MemDb()
    asyncio.run(write_all(j, cursors, db, [put(0)]))
    append_write(j, "put", {"i": 1})  # an apply-then-append writer: its db already holds the op
    asyncio.run(write_all(j, cursors, db, [put(2)]))
    assert db.log == ["op-0", "op-2"]  # passed by the heal, never re-applied
    assert cursors.held["w.jsonl"].ordinal == 3 and asyncio.run(journal_lag(j, cursors)) == 0


def test_a_reader_reports_the_lag_and_applies_nothing(tmp_path):
    j, cursors, db = str(tmp_path / "w.jsonl"), MemCursors(), MemDb()
    assert asyncio.run(journal_lag(j, cursors)) is None
    asyncio.run(stamp_cursor(j, cursors))
    token = FAULT_HOOK.set(crash_at("after_append"))
    try:
        with pytest.raises(Crash):
            asyncio.run(write_all(j, cursors, db, [put(0)]))
    finally:
        FAULT_HOOK.reset(token)
    assert asyncio.run(journal_lag(j, cursors)) == 1 and db.log == []


def test_a_nested_writer_joins_the_held_family(tmp_path):
    j, cursors, db = str(tmp_path / "w.jsonl"), MemCursors(), MemDb()
    asyncio.run(stamp_cursor(j, cursors))

    async def nested():
        async with JournalWriter(j, cursors, db.apply) as outer:
            await outer.write(put(0))
            async with JournalWriter(j, cursors, db.apply) as inner:
                await inner.write(put(1))

    asyncio.run(asyncio.wait_for(nested(), 5))
    assert db.log == ["op-0", "op-1"] and cursors.held["w.jsonl"].ordinal == 2


def test_tasks_in_one_loop_apply_in_journal_order(tmp_path):
    j, cursors, db = str(tmp_path / "w.jsonl"), MemCursors(), MemDb()
    asyncio.run(stamp_cursor(j, cursors))

    async def both():
        await asyncio.gather(write_all(j, cursors, db, [put(i, "a") for i in range(15)]),
                             write_all(j, cursors, db, [put(i, "b") for i in range(15)]))

    asyncio.run(both())
    assert db.log == [r["id"] for r in read_journal(j)] and len(db.log) == 30


# -- across processes: a real crash, and concurrent writers ---------------------------------

async def _file_apply(db_file, record):
    await asyncio.sleep(0.001)  # room for the other writer to interleave, if the lock let it
    with open(db_file, "a") as f:
        f.write(record["id"] + "\n")


def child_main(argv):  # A writer process: `n` puts with a prefix, each its own unit of work
    j, cursor_file, db_file, n, prefix = argv
    cursors = FileCursors(cursor_file)

    async def run():
        for i in range(int(n)):
            async with JournalWriter(j, cursors, lambda r: _file_apply(db_file, r)) as w:
                await w.write(put(i, prefix))

    asyncio.run(run())


def _spawn(tmp_path, n, prefix, fault=None):
    env = {k: v for k, v in os.environ.items() if k != JOURNAL_FAULT_ENV}
    if fault:
        env[JOURNAL_FAULT_ENV] = fault
    code = (f"import sys; sys.path.insert(0, {str(Path(__file__).parent)!r}); "
            f"import test_writer; test_writer.child_main(sys.argv[1:])")
    return subprocess.Popen([sys.executable, "-c", code, str(tmp_path / "w.jsonl"),
                             str(tmp_path / "cursors.json"), str(tmp_path / "db.log"), str(n), prefix],
                            env=env)


def _applied(tmp_path):
    p = tmp_path / "db.log"
    return p.read_text().split() if p.exists() else []


def test_a_killed_writer_releases_the_lock_and_the_next_process_heals(tmp_path):
    j, cursors = str(tmp_path / "w.jsonl"), FileCursors(tmp_path / "cursors.json")
    asyncio.run(stamp_cursor(j, cursors))
    child = _spawn(tmp_path, 1, "x", fault="after_append")
    assert child.wait(30) == FAULT_EXIT
    assert [r["id"] for r in read_journal(j)] == ["x-0"] and _applied(tmp_path) == []

    async def heal():
        async with JournalWriter(j, cursors, lambda r: _file_apply(str(tmp_path / "db.log"), r)) as w:
            return w.healed

    assert asyncio.run(asyncio.wait_for(heal(), 5)) == 1  # the dead holder's lock is gone
    assert _applied(tmp_path) == ["x-0"]


def test_an_armed_nth_arrival_crashes_the_nth_write_of_the_process(tmp_path):
    j, cursors = str(tmp_path / "w.jsonl"), FileCursors(tmp_path / "cursors.json")
    asyncio.run(stamp_cursor(j, cursors))
    child = _spawn(tmp_path, 3, "x", fault="after_append@2")
    assert child.wait(30) == FAULT_EXIT
    assert [r["id"] for r in read_journal(j)] == ["x-0", "x-1"] and _applied(tmp_path) == ["x-0"]

    async def heal():
        async with JournalWriter(j, cursors, lambda r: _file_apply(str(tmp_path / "db.log"), r)) as w:
            return w.healed

    assert asyncio.run(asyncio.wait_for(heal(), 5)) == 1  # only the crashed write's record was behind
    assert _applied(tmp_path) == ["x-0", "x-1"]


def test_concurrent_processes_apply_in_journal_order(tmp_path):
    j, cursors = str(tmp_path / "w.jsonl"), FileCursors(tmp_path / "cursors.json")
    asyncio.run(stamp_cursor(j, cursors))
    kids = [_spawn(tmp_path, 25, "a"), _spawn(tmp_path, 25, "b")]
    assert [k.wait(60) for k in kids] == [0, 0]
    ids = [r["id"] for r in read_journal(j)]
    assert len(ids) == 50 and _applied(tmp_path) == ids
    end = asyncio.run(cursors.read("w.jsonl"))
    assert end.ordinal == 50 and read_tail(j, end) == []
