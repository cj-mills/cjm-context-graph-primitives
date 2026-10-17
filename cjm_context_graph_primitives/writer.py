"""THE WRITE ORDER: plan -> append -> apply -> advance, one writer per journal family.

Ruling e4b2dfbb (append-then-apply on every graph), as design detail 65f4a6c9 shapes it. The
journal is the source of truth and a graph db its projection; the db records how far into each
journal family it has applied -- its APPLIED CURSOR, a `JournalPosition` the caller's cursor
store keeps beside the graph, never in it (the ingest_sources pattern). A `JournalWriter` holds
the family's lock for its whole unit of work: on entry it HEALS (applies every record past the
cursor through the same applier replay uses), so the caller PLANS against a db that holds its
whole journal; each `write` then APPENDS the planned op, APPLIES it and ADVANCES the cursor.

A crash anywhere in that sequence leaves the journal ahead of the cursor, never behind it, and
the next writer's heal closes the gap. Until one unit of work spans an apply and its advance
(681604c1 (C)), a crash between the two re-applies that record on heal -- replay idempotence
(deterministic ids) absorbs it. The lock serializes the family's writers, so records apply live
in journal order, exactly as replay applies them (65f4a6c9 (2b)); readers never take it.

Domain-free, like the journal core: the applier (a domain's per-op replay handler) and the
cursor store (a graph store's journal cursors) are the caller's."""

import hashlib
import json
import lzma
import os
from contextvars import ContextVar
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Protocol, Tuple

from cjm_context_graph_primitives.journal import (append_record, APPLIED_FIRST, family_lock,
                                                  indexed_segments, PROVENANCE_TS)

# THE CRASH POINTS (the proof of 65f4a6c9): a test sets FAULT_HOOK (it raises, simulating the
# crash in-process); a proof arms CJM_JOURNAL_FAULT=<point> in a child process, which then dies
# right there with no cleanup -- a real crash, its lock released by the OS. <point>@<n> dies at
# the process's n-th arrival there instead of its first, so a chain of writes in one invocation
# can be crashed at every op boundary (ruling 547f4123 (1)).
JOURNAL_FAULT_ENV = "CJM_JOURNAL_FAULT"
FAULT_EXIT = 86  # The exit code an armed crash point dies with
FAULT_POINTS = ("after_append", "after_apply")
FAULT_HOOK: ContextVar[Optional[Callable[[str], None]]] = ContextVar("journal_fault_hook", default=None)

# The writers this context's enclosing blocks have open, by family (its live tail's real path): a
# nested writer on one of them JOINS it -- the same writer, no second cursor read, no second heal
# (ruling 6683f49e (1)). Copied on every open, never mutated, so a sibling task never sees it.
_OPEN: ContextVar[Dict[str, Any]] = ContextVar("journal_writers_open", default={})

_ARRIVALS: Dict[str, int] = {}  # This process's arrivals per crash point (the armed <point>@<n> counts them)


def fault_point(
    point: str,  # One of FAULT_POINTS (armed as <point> or <point>@<n>)
) -> None:
    """A crash-injection point on the write path: the in-process hook, then the armed env."""
    hook = FAULT_HOOK.get()
    if hook is not None:
        hook(point)
    armed, _, nth = (os.environ.get(JOURNAL_FAULT_ENV) or "").partition("@")
    if armed != point:
        return
    _ARRIVALS[point] = _ARRIVALS.get(point, 0) + 1
    if _ARRIVALS[point] == int(nth or 1):
        os._exit(FAULT_EXIT)


@dataclass(frozen=True)
class JournalPosition:
    """How far into a journal family a db has applied: the segment holding the last applied
    record (its family index), the byte offset just past that record within it, the records
    applied, and that record's digest -- the check that the family under the cursor is still the
    one it applied."""

    segment: int  # Family index of the segment holding the last applied record (0 at the origin)
    offset: int   # Byte offset just past that record within the segment
    ordinal: int  # Records applied (0 = the origin: nothing applied)
    digest: str   # sha256 of the last applied record's line, newline included ("" at the origin)

    def to_dict(self) -> Dict[str, Any]:  # The wire / store form
        return asdict(self)

    @classmethod
    def from_dict(
        cls,
        d: Dict[str, Any],  # A stored position
    ) -> "JournalPosition":  # The position it names
        return cls(segment=int(d["segment"]), offset=int(d["offset"]),
                   ordinal=int(d["ordinal"]), digest=str(d["digest"]))


ORIGIN = JournalPosition(segment=0, offset=0, ordinal=0, digest="")  # Nothing applied: the tail is the whole family


class CursorMismatch(RuntimeError):
    """The family under a cursor is not the one it applied -- rewritten (compaction) or paired
    with another db: refused, never guessed past; a rebuild re-stamps the cursor."""


class NoCursor(RuntimeError):
    """The db holds no applied cursor for a family that holds records: it predates the cursor or
    was built without one. `stamp_cursor` adopts the family's end once the db is known to hold the
    whole family. An EMPTY family needs no adoption: the origin is its cursor (nothing journaled,
    so nothing the db can lack)."""


class CursorStore(Protocol):
    """Where a db keeps its applied cursors: beside the graph, one per journal family."""

    async def read(self, family: str) -> Optional[JournalPosition]: ...
    async def record(self, family: str, position: JournalPosition) -> None: ...


Applier = Callable[[Dict[str, Any]], Awaitable[Any]]  # A domain's per-op replay handler, closed over its store


def family_name(
    path: str,  # The family's live journal path
) -> str:  # Its key in a cursor store
    """A family is keyed by its live tail's FILE NAME, never its path: a db and its journals move
    together (a swap, a restore, another machine), and a path would pin them to one place."""
    return Path(path).name


def _segment_bytes(
    seg: str,       # A segment path, plain or `.xz`
    start: int = 0, # Byte offset to read from
) -> bytes:  # The segment's bytes from `start` (a compressed one decompressed)
    if seg.endswith(".xz"):
        return lzma.decompress(Path(seg).read_bytes())[start:]
    with open(seg, "rb") as f:
        f.seek(start)
        return f.read()


def _line_ending_at(
    seg: str,    # A segment path, plain or `.xz`
    offset: int, # A byte offset just past a record's newline
) -> bytes:  # That record's line, newline included
    """The record whose line ends at `offset`, read backwards from it (a plain segment reads a
    few KB, never the whole tail)."""
    if seg.endswith(".xz"):
        data = lzma.decompress(Path(seg).read_bytes())[:offset]
    else:
        with open(seg, "rb") as f:
            back = 4096
            while True:
                start = max(0, offset - back)
                f.seek(start)
                data = f.read(offset - start)
                if start == 0 or data.rfind(b"\n", 0, len(data) - 1) >= 0:
                    break
                back *= 2
    return data[data.rfind(b"\n", 0, len(data) - 1) + 1:]


def _digest(
    line: bytes,  # A record's line, newline included
) -> str:  # Its sha256, hex
    return hashlib.sha256(line).hexdigest()


def read_tail(
    path: str,                 # The family's live journal path
    position: JournalPosition, # How far a db has applied
) -> List[Tuple[Dict[str, Any], JournalPosition]]:  # Each record past it, with the position just past that record
    """Every record of the family past `position`, in append order -- only the tail is read (the
    cursor's segment from its offset, then each later segment). The position is CHECKED first:
    the record ending at it must carry its digest, else CursorMismatch."""
    segs = indexed_segments(path)
    if position.ordinal:
        held = dict(segs).get(position.segment)
        if held is None or _digest(_line_ending_at(held, position.offset)) != position.digest:
            raise CursorMismatch(
                f"{family_name(path)}: no record ends at segment {position.segment} offset "
                f"{position.offset} with the digest the cursor holds -- the family was rewritten "
                f"under it, or the db belongs to another journal; rebuild to re-stamp it")
    out: List[Tuple[Dict[str, Any], JournalPosition]] = []
    ordinal = position.ordinal
    for index, seg in segs:
        if index < position.segment:
            continue
        at = position.offset if index == position.segment else 0
        for line in _segment_bytes(seg, at).splitlines(keepends=True):
            at += len(line)
            if not line.strip():
                continue
            ordinal += 1
            out.append((json.loads(line), JournalPosition(index, at, ordinal, _digest(line))))
    return out


class JournalWriter:
    """One writer's unit of work on one journal family (65f4a6c9): `async with` takes the
    family's lock and heals the db up to its journal; `write` appends a planned op, applies it
    and advances the cursor; the lock is released on exit. Plan INSIDE the block, so the plan
    reads a db that holds the whole family.

    A writer opened inside an open writer on the same family JOINS it (ruling 6683f49e (1)):
    `async with` yields the OUTER writer -- one lock, one heal, one cursor -- so a core function
    that opens its own writer composes into an app's gesture window as one unit of work. A nested
    writer addressing another db than the one it would join refuses."""

    def __init__(
        self,
        path: str,             # The family's live journal path
        cursors: CursorStore,  # The db's applied cursors
        apply: Applier,        # The domain's per-op applier -- the one replay uses
    ):
        self.path, self.cursors, self.apply = path, cursors, apply
        self.family = family_name(path)
        self.position: Optional[JournalPosition] = None  # The db's applied cursor while the block runs
        self.healed = 0                                   # Records the entry's heal applied
        self._joined: Optional["JournalWriter"] = None    # The outer writer this block joined

    async def __aenter__(self) -> "JournalWriter":
        key = os.path.realpath(self.path)
        outer = _OPEN.get().get(key)
        if outer is not None:
            if outer.cursors != self.cursors:
                raise RuntimeError(
                    f"{self.family}: a nested writer addresses another db than the open writer "
                    f"it would join -- one family, one db per unit of work")
            self._joined = outer
            return outer
        self._lock = family_lock(self.path)
        await self._lock.__aenter__()
        try:
            self.healed = await self._heal()
        except BaseException as e:
            await self._lock.__aexit__(type(e), e, e.__traceback__)
            raise
        self._token = _OPEN.set({**_OPEN.get(), key: self})
        return self

    async def __aexit__(self, *exc) -> None:
        if self._joined is not None:
            self._joined = None  # the outer block owns the lock and the cursor
            return
        _OPEN.reset(self._token)
        await self._lock.__aexit__(*exc)

    async def _heal(self) -> int:  # Records applied past the cursor
        position = await self.cursors.read(self.family)
        if position is None and not read_tail(self.path, ORIGIN):
            position = ORIGIN  # an empty family: nothing journaled, so nothing the db can lack
            await self.cursors.record(self.family, ORIGIN)  # durable: a later append lags it, never orphans it
        if position is None:
            raise NoCursor(
                f"{self.family}: the db holds no applied cursor -- it predates the cursor or was "
                f"built without one; a rebuild stamps it, and stamp_cursor adopts the family's end "
                f"once the db is known to hold the whole family")
        self.position = position
        tail = read_tail(self.path, position)
        healed = 0
        for record, after in tail:
            if not record.get(APPLIED_FIRST):  # an applied-first record's db already holds it: passed
                await self._apply(record)
                healed += 1
            await self._advance(after)
        return healed

    async def _apply(
        self,
        record: Dict[str, Any],  # A journaled record
    ) -> None:
        """Apply one record at its own journaled ts -- the window replay opens for it."""
        token = PROVENANCE_TS.set(record.get("ts"))
        try:
            await self.apply(record)
        finally:
            PROVENANCE_TS.reset(token)

    async def _advance(
        self,
        position: JournalPosition,  # Just past the record now applied
    ) -> None:
        await self.cursors.record(self.family, position)
        self.position = position

    async def write(
        self,
        op: Dict[str, Any],  # A PLANNED op: references resolved to ids, minted ids in it, validated
        dedup: bool = True,  # Skip an already-journaled duplicate (append_op's rule)
    ) -> Optional[Dict[str, Any]]:  # The record as journaled, or None when dedup found it journaled (and so applied)
        """Append, apply, advance. A planned op is total, so its apply cannot refuse; a crash
        after the append leaves a record the next writer's heal applies."""
        if self.position is None:
            raise RuntimeError("JournalWriter.write outside its `async with` block")
        landed = append_record(self.path, op, dedup=dedup)
        if landed is None:
            return None
        fault_point("after_append")
        await self._apply(landed.record)
        fault_point("after_apply")
        await self._advance(JournalPosition(landed.segment, landed.offset,
                                            self.position.ordinal + 1, landed.digest))
        return landed.record

    async def restamp(self) -> JournalPosition:  # The family's end, now the db's cursor
        """Re-stamp the cursor at the family's END after a rewrite under this writer's lock moved
        records the db already holds out of the family (a compaction, ruling 6683f49e (3)): the db
        stood at the end before the rewrite and stands there after it; a record before the cursor
        moved, so the cursor's ordinal -- and, when its own segment was rewritten, its place --
        changes, never what the db has applied."""
        if self.position is None:
            raise RuntimeError("JournalWriter.restamp outside its `async with` block")
        tail = read_tail(self.path, ORIGIN)
        end = tail[-1][1] if tail else ORIGIN
        await self._advance(end)
        return end


async def stamp_cursor(
    path: str,             # The family's live journal path
    cursors: CursorStore,  # The db's applied cursors
) -> JournalPosition:  # The family's end, now the db's cursor
    """Record the family's END as the db's cursor -- for a db that holds the whole family: a
    rebuild at its close (65f4a6c9 (2c)), or the one-time adoption of a db built before the
    cursor existed. Under the family's lock, so no append lands between the read and the stamp."""
    async with family_lock(path):
        tail = read_tail(path, ORIGIN)
        end = tail[-1][1] if tail else ORIGIN
        await cursors.record(family_name(path), end)
    return end


async def journal_lag(
    path: str,             # The family's live journal path
    cursors: CursorStore,  # The db's applied cursors
) -> Optional[int]:  # Records past the db's cursor; None when the db holds no cursor
    """How far a db trails its journal, for a READER to report (65f4a6c9 (2d)): readers never
    apply, so a lag is healed by the family's next writer."""
    position = await cursors.read(family_name(path))
    return None if position is None else len(read_tail(path, position))
