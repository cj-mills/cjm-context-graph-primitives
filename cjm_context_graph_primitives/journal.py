"""The write-journal core: append-only JSONL ops — the durable, replayable source of truth.

The shared PRIMITIVE under every workflow journal (DEC ccbab9f5 point 1): a graph db is a
rebuildable PROJECTION; the non-re-derivable knowledge lives as an append-only log of write
ops. Domain layers own their op vocabularies and REPLAY (each registers its own verbs);
this module owns the discipline — append-on-success with exact-duplicate skip, read in
append order, session stamping for provenance, and the OP CLOCK every write reads its time from.
"""

import asyncio
import fcntl
import functools
import hashlib
import inspect
import json
import lzma
import os
import re
import time
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, Iterator, List, Optional, Tuple

from cjm_context_graph_primitives.timings import record_append, span, timed

# THE OP CLOCK (design 8f6f2343, finding fbce0173; the replay window of 0d50b921 moved down
# here). One op has ONE time: its journaled `ts`. Replay opens this window at each op's ts; a
# live write opens it with ONE clock read (`op_clock`) around its db write and its appends, so
# everything the op stamps — created_at / updated_at on what it adds, asserted_at, its own
# `ts` — is the same value live and on rebuild. Async-scoped (ContextVar); the layer re-exports
# this object, so replay and live share one variable.
PROVENANCE_TS: ContextVar[Optional[float]] = ContextVar("provenance_ts", default=None)

# THE APPLIED-FIRST MARK (design detail 65f4a6c9, eeda5977 leg L2): a record `append_write` lands
# was APPLIED BEFORE its append -- the apply-then-append order the write order retires, verb by
# verb. A writer's heal (writer.JournalWriter) advances past such a record and never re-applies
# it: its db already holds it, through a live path replay's applier may not reproduce. Replay
# ignores the mark (a rebuild applies every record).
APPLIED_FIRST = "applied_first"

# THE FAMILY'S WRITER LOCK (design detail 65f4a6c9 (2b), ruling 6683f49e (3)): the families this
# context's enclosing blocks hold. Every act that mutates a family -- a writer's unit of work, a
# rotation, a compaction -- holds its lock; a nested act on a held family joins the enclosing
# block instead of waiting on itself.
_HELD: ContextVar[frozenset] = ContextVar("journal_families_held", default=frozenset())


def op_now() -> float:  # The open window's ts, else a fresh clock read
    """The op clock's current value: every write-path time reads THIS, never `time.time()`."""
    ts = PROVENANCE_TS.get()
    return ts if ts is not None else time.time()


@contextmanager
def op_clock() -> Iterator[float]:  # The window's ts for the duration of the block
    """Open one op's clock window: reuse an open one (replay, or an enclosing write unit),
    else read the clock ONCE and hold it until the block exits."""
    ts = PROVENANCE_TS.get()
    if ts is not None:
        yield ts
        return
    ts = time.time()
    token = PROVENANCE_TS.set(ts)
    try:
        yield ts
    finally:
        PROVENANCE_TS.reset(token)


def op_clocked(fn):  # The function, run inside one op clock window per call
    """Decorator form of `op_clock` for a write entry point (sync or async): every db write
    it makes and every op it journals carry one time. The grain is one call — a CLI
    invocation, an app's write gesture, a core's commit (amendment efd659a1)."""
    if inspect.iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def run_async(*args, **kwargs):
            with op_clock():
                return await fn(*args, **kwargs)
        return run_async

    @functools.wraps(fn)
    def run(*args, **kwargs):
        with op_clock():
            return fn(*args, **kwargs)
    return run


# THE ATOMIC UNIT (ruling 3de795e4): a human gesture's ops land as ONE unit -- appended together
# in one write, each record tagged with the unit's id and its record count, applied only once the
# whole unit is appended. A unit a crash tore mid-append can stand only at the live tail's END
# (every appender of a write family holds its lock, and the next holder seals a torn end away),
# so readers stop at the COMMITTED end and a torn unit is never applied, live or on rebuild. A
# compaction may later archive a unit's record whole; nothing reads a unit's count but the tail's.
# A single-op unit is an ordinary record (no tags).
UNIT = "unit"            # The unit id each record of a multi-op unit carries
UNIT_SIZE = "unit_size"  # How many records that unit holds


def committed_length(
    path: str,  # The family's live journal path
) -> int:  # Byte length of its committed prefix (the whole file when nothing is torn)
    """How much of the live tail is COMMITTED (ruling 3de795e4): everything but a partial last line
    (an append torn mid-write) and a trailing unit holding fewer records than its size. Read
    backwards from the end, a few KB at a time -- never the whole tail."""
    try:
        size = os.path.getsize(path)
    except FileNotFoundError:
        return 0
    back = 4096
    with open(path, "rb") as f:
        while True:
            start = max(0, size - back)
            f.seek(start)
            data = f.read(size - start)
            if start and b"\n" not in data[:-1]:
                back *= 2  # not one whole line in the window yet
                continue
            begin = data.find(b"\n") + 1 if start else 0  # the window's first line may start before it
            lines: List[Tuple[int, bytes]] = []  # (offset just past the line, line): complete lines only
            at = start + begin
            for line in data[begin:].splitlines(keepends=True):
                at += len(line)
                if line.endswith(b"\n"):
                    lines.append((at, line))
            records = [(end, line) for end, line in lines if line.strip()]
            if not records:
                if not start:
                    return lines[-1][0] if lines else 0
                back *= 2
                continue
            last = json.loads(records[-1][1])
            uid = last.get(UNIT)
            if uid is None:
                return lines[-1][0]
            held = 0
            for _, line in reversed(records):
                if json.loads(line).get(UNIT) != uid:
                    break
                held += 1
            if held >= int(last.get(UNIT_SIZE) or 1):
                return lines[-1][0]
            if held < len(records) or not start:  # the torn unit's first record is in the window
                end, line = records[len(records) - held]
                return end - len(line)
            back *= 2


def seal_tail(
    path: str,  # The family's live journal path -- its lock HELD by the caller
) -> int:  # Bytes cut off the tail (0 when nothing was torn)
    """Cut an uncommitted end off the live tail (ruling 3de795e4): a partial last line and a
    trailing torn unit, which no apply ever followed, so no cursor stands past the cut. Only a
    holder of the family's lock seals -- a writer as it opens, a rotation before it renames -- and
    every appender of a write family holds that lock, so what it finds torn a crash tore."""
    keep = committed_length(path)
    try:
        size = os.path.getsize(path)
    except FileNotFoundError:
        return 0
    if keep >= size:
        return 0
    with open(path, "r+b") as f:
        f.truncate(keep)
        f.flush()
        os.fsync(f.fileno())
    return size - keep


def read_journal(
    path: str,  # Journal file path (JSONL)
) -> List[Dict[str, Any]]:  # The recorded ops, in append order
    """Read every journaled write op across the SEGMENT FAMILY (one JSON object per
    line; missing files = []) — cold segments first, live tail last: append order. The live
    tail is read to its COMMITTED end (`committed_length`): a torn unit is never returned."""
    ops: List[Dict[str, Any]] = []
    tail = str(Path(path))
    with span("read_journal") as measured:  # the instrument (681604c1): a whole-history read
        for seg in journal_segments(path):
            if seg == tail:
                text = Path(seg).read_bytes()[:committed_length(seg)].decode("utf-8")
            else:
                text = read_segment(seg)
            measured["bytes"] = measured.get("bytes", 0) + len(text)
            for line in text.splitlines():
                line = line.strip()
                if line:
                    ops.append(json.loads(line))
        measured["ops"] = len(ops)
    return ops


def append_write(
    path: str,        # Journal file path (JSONL)
    verb: str,        # The write verb ("decide" | "alias" | "assert")
    args: Dict[str, Any],  # The resolved arguments applied (replay re-passes these verbatim)
) -> bool:  # True if appended, False if an identical op was already journaled
    """Append one write op (skipping an exact (verb,args) duplicate) its caller has ALREADY
    APPLIED -- the record carries the applied-first mark, so a writer's heal passes it.

    Args are the RESOLVED inputs to the core verb (e.g. an alias's discovered
    evidence ids), so replay is deterministic and independent of corpus state."""
    return append_op(path, {"verb": verb, "args": args, APPLIED_FIRST: True})


def current_session() -> Optional[str]:  # The active session key, or None
    """The session key stamped on journal appends (provenance, not replay input).

    Read from `CJM_SESSION` — the `cg-write` wrapper exports it from
    `.cjm/current-session`, one start-time timestamp key generated per session
    (DEC 6124d8bf: discipline problems become infrastructure — historical
    per-verb `--session` coverage was 9% for exactly this reason). Stamped
    TOP-LEVEL on the record so dedup (verb+args) and replay stay session-blind."""
    return os.environ.get("CJM_SESSION") or None


@dataclass(frozen=True)
class AppendedRecord:
    """One record `append_record` landed, and where: a writer advances its applied cursor to it
    (design detail 65f4a6c9)."""

    record: Dict[str, Any]  # The op as journaled (ts / session stamped)
    segment: int            # Family index of the segment holding it (the live tail's at append time)
    offset: int             # Byte offset just past its line within that segment
    digest: str             # sha256 of its line, newline included


def stamp_record(
    op: Dict[str, Any],  # The full op record — requires `verb`; envelope fields ride verbatim
) -> Dict[str, Any]:  # The record as it is journaled: `ts` from the op clock, `session` from CJM_SESSION
    """Stamp an op for the journal, each field only when ABSENT -- at its append, or as a unit
    buffers it (ruling 3de795e4), so a buffered op carries the time its plan ran."""
    if not op.get("verb"):
        raise ValueError("append_op: op requires a `verb`")
    record = dict(op)
    record.setdefault("ts", op_now())
    session = current_session()
    if session and "session" not in record:
        record["session"] = session
    return record


def journaled(
    path: str,           # Journal file path (JSONL)
    op: Dict[str, Any],  # An op about to be appended
) -> bool:  # True when the family already holds it (by `id` when present, else exact (verb, args))
    """The append dedup rule: an explicit op `id` is exact-once; an id-less op matches an id-less
    record of the same (verb, args) -- envelope ops never shadow that lane."""
    oid = op.get("id")
    for existing in read_journal(path):
        if oid is not None and existing.get("id") == oid:
            return True
        if (oid is None and "id" not in existing
                and existing.get("verb") == op.get("verb") and existing.get("args") == op.get("args")):
            return True
    return False


@timed("append_op")
def append_record(
    path: str,           # Journal file path (JSONL)
    op: Dict[str, Any],  # The full op record — requires `verb`; envelope fields ride verbatim
    dedup: bool = True,  # Skip an already-journaled duplicate (by `id` when present, else exact (verb, args))
) -> Optional[AppendedRecord]:  # The record and where it landed, or None if skipped as a duplicate
    """Append one op record and say where it landed — the core `append_op` answers True / False for.

    Stamps `ts` from the op clock (an open window's ts, so the op carries the time its db
    write already used) and `session` from CJM_SESSION, only when ABSENT, so a domain
    envelope's own fields (actor / set / anchor / minted ids / explicit ts) ride
    through verbatim. Dedup prefers an explicit op `id` (exact-once semantics for
    envelope ops) and falls back to the exact (verb, args) match. `dedup=False`
    is the bulk path (genesis imports): a full journal rescan per append is
    O(n^2) at import scale — bulk writers dedup upstream or not at all. The landing
    (segment index, byte offset, line digest) is what a writer's applied cursor records; a
    post-append rotation renames the tail to that same index, so it stays true."""
    if dedup and journaled(path, op):
        return None
    record = stamp_record(op)
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(record, sort_keys=True) + "\n").encode("utf-8")
    segment = live_index(path)
    with p.open("ab") as f:
        f.write(line)
        offset = f.tell()
    record_append(path, len(line))  # the instrument's logical bytes (2f5033bf (1))
    maybe_rotate(path)  # post-append: a fat op lands, then closes its segment
    return AppendedRecord(record=record, segment=segment, offset=offset,
                          digest=hashlib.sha256(line).hexdigest())


@timed("append_op")
def append_records(
    path: str,                   # Journal file path (JSONL)
    records: List[Dict[str, Any]],  # Stamped records (`stamp_record`), a unit's tags on each
    tear: Optional[Callable[[], None]] = None,  # The crash proof's seam: called between a torn prefix and the rest
) -> List[AppendedRecord]:  # Each record and where it landed, in order
    """Append an atomic unit's records in ONE write (ruling 3de795e4) and say where each landed. No
    dedup: a unit is planned whole, and a no-change write is caught at its plan (17daa86b). The
    post-append rotation runs once, after the whole unit, so a unit never spans two segments.
    With `tear` (a crash proof only), the write is split mid-way through the second record and
    `tear` runs between -- the torn append a crash can leave."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    lines = [(json.dumps(r, sort_keys=True) + "\n").encode("utf-8") for r in records]
    data = b"".join(lines)
    segment = live_index(path)
    with p.open("ab") as f:
        if tear is not None and len(lines) > 1:
            cut = len(lines[0]) + len(lines[1]) // 2
            f.write(data[:cut])
            f.flush()
            tear()
            f.write(data[cut:])
        else:
            f.write(data)
        end = f.tell()
    record_append(path, len(data))  # the instrument's logical bytes (2f5033bf (1))
    landed: List[AppendedRecord] = []
    offset = end - len(data)
    for record, line in zip(records, lines):
        offset += len(line)
        landed.append(AppendedRecord(record=record, segment=segment, offset=offset,
                                     digest=hashlib.sha256(line).hexdigest()))
    maybe_rotate(path)  # post-append, once: the whole unit lands, then closes its segment
    return landed


def append_op(
    path: str,           # Journal file path (JSONL)
    op: Dict[str, Any],  # The full op record — requires `verb`; envelope fields ride verbatim
    dedup: bool = True,  # Skip an already-journaled duplicate (by `id` when present, else exact (verb, args))
) -> bool:  # True if appended, False if skipped as a duplicate
    """Append one op record — the envelope-agnostic core `append_write` wraps; `append_record`
    carries the contract (stamping, dedup, rotation) and says where the record landed."""
    return append_record(path, op, dedup=dedup) is not None


def _stem_suffix(
    name: str,  # A journal filename (e.g. "context_graph.writes.jsonl")
) -> tuple:  # (stem, suffix) — the trailing ".jsonl" split off (suffix "" when absent)
    """Split a journal filename for segment naming: cold segments interpose the
    rotation index between stem and suffix (`<stem>.NNNN.jsonl`)."""
    return (name[: -len(".jsonl")], ".jsonl") if name.endswith(".jsonl") else (name, "")


def _cold_segments(
    path: str,  # The live journal path — the family's tail
) -> Dict[int, Path]:  # Cold index -> its segment (the plain form wins its index)
    """The family's cold segments by rotation index, plain or compressed: the one glob behind
    `journal_segments`, `indexed_segments`, `live_index` and `rotate_journal`."""
    p = Path(path)
    stem, suffix = _stem_suffix(p.name)
    by_index: Dict[int, Path] = {}
    for pattern in (f"{stem}.[0-9][0-9][0-9][0-9]{suffix}.xz",
                    f"{stem}.[0-9][0-9][0-9][0-9]{suffix}"):
        for q in p.parent.glob(pattern):          # plain globbed second: it wins its index
            if q.is_file():
                by_index[int(q.name[len(stem) + 1:len(stem) + 5])] = q
    return by_index


def indexed_segments(
    path: str,  # The live journal path — the family's tail
) -> List[Tuple[int, str]]:  # (family index, segment path): cold segments in rotation order, then the live tail
    """The SEGMENT FAMILY with each segment's index: a cold segment by its NNNN, the live tail at
    `live_index` (one past the highest cold index). A position in the family -- (index, byte
    offset) -- is stable across rotation, because the tail closes into exactly its index with its
    bytes unchanged (design detail 65f4a6c9 (2c))."""
    cold = _cold_segments(path)
    p = Path(path)
    tail = [(max(cold, default=0) + 1, str(p))] if p.exists() else []
    return [(k, str(cold[k])) for k in sorted(cold)] + tail


def live_index(
    path: str,  # The live journal path — the family's tail
) -> int:  # The family index the live tail holds: the index its rotation will mint
    """The live tail's place in its family: one past the highest cold index, plain or compressed."""
    return max(_cold_segments(path), default=0) + 1


def journal_segments(
    path: str,  # The live journal path (JSONL) — the family's tail
) -> List[str]:  # Cold segments in rotation order, then the live tail (existing files only)
    """The journal's SEGMENT FAMILY: rotated cold segments + the live tail.

    Rotation (DEC bb1b9995) partitions one logical journal into immutable cold
    segments named `<stem>.NNNN.jsonl` beside the live file — a FILE-level
    repartition only: reading the family in order is byte-identical to the
    unrotated journal, so replay semantics never change and genesis stays
    one-time in segment 0001. Durability motive: GitHub hard-blocks blobs at
    100MB; rotation keeps every journal file small forever. A cold segment may be
    stored COMPRESSED as `<stem>.NNNN.jsonl.xz` (ruling f1173d0b) — one path per
    index either way; when both forms exist (a swap that crashed before its unlink)
    the PLAIN one is listed, the original, until `compress_cold_segments` finishes it.
    Read a listed segment with `read_segment`, never `Path.read_text`."""
    return [seg for _, seg in indexed_segments(path)]


def read_segment(
    seg: str,  # A segment path — plain `.jsonl`, or a compressed cold `.jsonl.xz`
) -> str:  # The segment's text (a compressed one decompressed: byte-identical to its plain original)
    """Read one journal segment's text, plain or xz-compressed.

    A cold segment may be stored xz-compressed (ruling f1173d0b) — an encoding of the same
    bytes, never a change to its records — so every reader of a listed segment goes through
    here."""
    data = Path(seg).read_bytes()
    if seg.endswith(".xz"):
        data = lzma.decompress(data)
    return data.decode("utf-8")


def write_segment(
    seg: str,   # The segment path; a `.xz` path is written compressed
    text: str,  # The segment's full text
) -> None:
    """Write one segment atomically, in its own encoding (plain or xz-compressed).

    tmp + fsync + os.replace — the path a maintenance rewrite (compaction, a7617bd4) takes,
    so a compressed segment stays compressed and a plain one plain."""
    data = text.encode("utf-8")
    if seg.endswith(".xz"):
        data = lzma.compress(data, preset=9)
    tmp = seg + ".tmp"
    with open(tmp, "wb") as out:
        out.write(data)
        out.flush()
        os.fsync(out.fileno())
    os.replace(tmp, seg)


@timed("rotate_journal")
def rotate_journal(
    path: str,             # The live journal path — its content becomes the next cold segment
    compress: bool = True, # Store the closed segment xz-compressed (ruling f1173d0b)
    wait: bool = True,     # Wait for the family's lock; False: skip when another holder has it
) -> Optional[str]:  # The cold segment path minted, or None (missing/empty tail, or the lock busy unwaited)
    """Close the live tail into the next immutable cold segment (explicit rotation).

    A rename, then (by default) the segment is stored compressed by `compress_segment`:
    its bytes are re-ENCODED, never rewritten — decompressing reproduces them exactly,
    so the family read — and therefore replay — is untouched. `append_op` auto-rotates
    past the size budget via `maybe_rotate`; this explicit verb exists for migration
    splits and manual closes. Cold segments are part of the journal — the never-delete
    rule covers every segment, not just the live tail (the plain copy is removed only
    once its compressed form is proven).

    Under the family's lock (ruling 6683f49e (3)): a rotation renames the tail an append's landing
    names, so it never interleaves with a writer -- inside a writer's or a compaction's block it
    joins the hold, elsewhere it takes the lock; with `wait=False` a busy lock skips the rotation."""
    with family_lock_sync(path, wait=wait) as held:
        if held is None:
            return None
        seal_tail(path)  # a torn unit never closes into a cold segment (ruling 3de795e4)
        p = Path(path)
        if not p.exists() or p.stat().st_size == 0:
            return None
        stem, suffix = _stem_suffix(p.name)
        cold = p.with_name(f"{stem}.{live_index(path):04d}{suffix}")
        p.rename(cold)
        return compress_segment(str(cold)) if compress else str(cold)


@timed("compress_segment")
def compress_segment(
    seg: str,  # A PLAIN cold segment (`<stem>.NNNN.jsonl`) — never the live tail
) -> str:  # The compressed segment's path (`<seg>.xz`)
    """Store one cold segment xz-compressed, proven lossless before the plain copy goes.

    Ruling f1173d0b: write `<seg>.xz` beside it, PROVE it (decompressed == the original
    bytes), and only then remove the plain copy. Lossless by construction — the family
    reads byte-identical, so replay never changes; a crash before the unlink leaves both
    forms and `journal_segments` lists the plain one until a re-run finishes the swap.
    Preset 9, measured at the ruling: ~58x on the dev source journal, ~7x on the writes
    journals, ~0.05 s to decompress a 32 MB segment."""
    src = Path(seg)
    if not re.search(r"\.[0-9]{4}(\.jsonl)?$", src.name):
        raise ValueError(f"compress_segment: {seg} is not a cold segment (<stem>.NNNN.jsonl) "
                         f"— the live tail is never compressed")
    data = src.read_bytes()
    dst = seg + ".xz"
    write_segment(dst, data.decode("utf-8"))
    if lzma.decompress(Path(dst).read_bytes()) != data:
        Path(dst).unlink()
        raise ValueError(f"compress_segment: {dst} does not decompress to {seg} — "
                         f"the plain segment is kept")
    src.unlink()
    return dst


def compress_cold_segments(
    path: str,  # The live journal path (the family's tail)
) -> List[Dict[str, Any]]:  # One row per segment compressed: {segment, bytes_before, bytes_after}
    """Compress every PLAIN cold segment of a family (the live tail is never touched).

    The one-time conversion of the segments rotated before compression, and the recovery
    for a rotation that crashed mid-swap. Idempotent: a compressed family yields []."""
    tail = str(Path(path))
    rows: List[Dict[str, Any]] = []
    for seg in journal_segments(path):
        if seg == tail or seg.endswith(".xz"):
            continue
        before = os.path.getsize(seg)
        dst = compress_segment(seg)
        rows.append({"segment": dst, "bytes_before": before, "bytes_after": os.path.getsize(dst)})
    return rows


def maybe_rotate(
    path: str,                          # The live journal path
    max_bytes: int = 32 * 1024 * 1024,  # Tail budget — far under GitHub's 100MB blob block
) -> Optional[str]:  # The cold segment minted, or None (tail under budget)
    """Rotate the live tail once it reaches the budget (the POST-append check).

    Checked after a successful append, never before — a fat op always lands and
    merely closes its segment out oversized (graceful degradation: the durability
    net only breaks at a single op crossing the blob limit, ~75+ audio hours in
    one source at measured spine-extension rates). Never waits for the family's lock: an
    appender inside the write order holds it already (the rotation joins), and one outside it
    skips a rotation another holder blocks -- the next append's check closes the segment."""
    p = Path(path)
    if p.exists() and p.stat().st_size >= max_bytes:
        return rotate_journal(path, wait=False)
    return None


def lock_path(
    path: str,  # The family's live journal path
) -> str:  # The lock file beside it (never a segment: the family's globs cannot match it)
    return f"{path}.lock"


def _open_lock(
    path: str,  # The family's live journal path
) -> int:  # An fd on the family's lock file, this holder's own open of it
    """Open the lock file a family's holders flock -- each holder its own open, so the flock
    excludes every other holder, this process's included."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return os.open(lock_path(path), os.O_RDWR | os.O_CREAT, 0o644)


@asynccontextmanager
async def family_lock(
    path: str,  # The family's live journal path
) -> AsyncIterator[bool]:  # True when this block took the lock, False when an enclosing block holds it
    """The family's exclusive writer lock: an flock on its own open of the lock file, so it
    excludes every other holder -- another process, another thread, another task of this loop --
    and dies with its process. Waited for without blocking the loop (a non-blocking try, then a
    short sleep), so a task holding it here can run to its release. Reentrant within one context:
    a nested writer joins the enclosing unit of work."""
    key = os.path.realpath(path)
    if key in _HELD.get():
        yield False
        return
    fd = _open_lock(path)
    try:
        delay = 0.002
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 0.05)
        token = _HELD.set(_HELD.get() | {key})
        try:
            yield True
        finally:
            _HELD.reset(token)
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


@contextmanager
def family_lock_sync(
    path: str,          # The family's live journal path
    wait: bool = True,  # Block until the lock is free; False: give up at once when another holder has it
) -> Iterator[Optional[bool]]:  # True: this block took the lock; False: an enclosing block holds it; None: busy, not waited for
    """`family_lock` for a synchronous family act -- a rotation (ruling 6683f49e (3)). Joins an
    enclosing block's hold; else blocks on the flock, or with `wait=False` yields None at once
    when another holder has it: the post-append budget check (`maybe_rotate`) never blocks, so
    an appender outside the write order cannot wait on a writer of its own thread."""
    key = os.path.realpath(path)
    if key in _HELD.get():
        yield False
        return
    fd = _open_lock(path)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | (0 if wait else fcntl.LOCK_NB))
        except BlockingIOError:
            yield None
            return
        token = _HELD.set(_HELD.get() | {key})
        try:
            yield True
        finally:
            _HELD.reset(token)
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
