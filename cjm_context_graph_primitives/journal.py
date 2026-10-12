"""The write-journal core: append-only JSONL ops — the durable, replayable source of truth.

The shared PRIMITIVE under every workflow journal (DEC ccbab9f5 point 1): a graph db is a
rebuildable PROJECTION; the non-re-derivable knowledge lives as an append-only log of write
ops. Domain layers own their op vocabularies and REPLAY (each registers its own verbs);
this module owns the discipline — append-on-success with exact-duplicate skip, read in
append order, session stamping for provenance, and the OP CLOCK every write reads its time from.
"""

import functools
import inspect
import json
import lzma
import os
import re
import time
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

# THE OP CLOCK (design 8f6f2343, finding fbce0173; the replay window of 0d50b921 moved down
# here). One op has ONE time: its journaled `ts`. Replay opens this window at each op's ts; a
# live write opens it with ONE clock read (`op_clock`) around its db write and its appends, so
# everything the op stamps — created_at / updated_at on what it adds, asserted_at, its own
# `ts` — is the same value live and on rebuild. Async-scoped (ContextVar); the layer re-exports
# this object, so replay and live share one variable.
PROVENANCE_TS: ContextVar[Optional[float]] = ContextVar("provenance_ts", default=None)


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


def read_journal(
    path: str,  # Journal file path (JSONL)
) -> List[Dict[str, Any]]:  # The recorded ops, in append order
    """Read every journaled write op across the SEGMENT FAMILY (one JSON object per
    line; missing files = []) — cold segments first, live tail last: append order."""
    ops: List[Dict[str, Any]] = []
    for seg in journal_segments(path):
        for line in read_segment(seg).splitlines():
            line = line.strip()
            if line:
                ops.append(json.loads(line))
    return ops


def append_write(
    path: str,        # Journal file path (JSONL)
    verb: str,        # The write verb ("decide" | "alias" | "assert")
    args: Dict[str, Any],  # The resolved arguments applied (replay re-passes these verbatim)
) -> bool:  # True if appended, False if an identical op was already journaled
    """Append one write op (skipping an exact (verb,args) duplicate).

    Args are the RESOLVED inputs to the core verb (e.g. an alias's discovered
    evidence ids), so replay is deterministic and independent of corpus state."""
    return append_op(path, {"verb": verb, "args": args})


def current_session() -> Optional[str]:  # The active session key, or None
    """The session key stamped on journal appends (provenance, not replay input).

    Read from `CJM_SESSION` — the `cg-write` wrapper exports it from
    `.cjm/current-session`, one start-time timestamp key generated per session
    (DEC 6124d8bf: discipline problems become infrastructure — historical
    per-verb `--session` coverage was 9% for exactly this reason). Stamped
    TOP-LEVEL on the record so dedup (verb+args) and replay stay session-blind."""
    return os.environ.get("CJM_SESSION") or None


def append_op(
    path: str,           # Journal file path (JSONL)
    op: Dict[str, Any],  # The full op record — requires `verb`; envelope fields ride verbatim
    dedup: bool = True,  # Skip an already-journaled duplicate (by `id` when present, else exact (verb, args))
) -> bool:  # True if appended, False if skipped as a duplicate
    """Append one op record — the envelope-agnostic core `append_write` wraps.

    Stamps `ts` from the op clock (an open window's ts, so the op carries the time its db
    write already used) and `session` from CJM_SESSION, only when ABSENT, so a domain
    envelope's own fields (actor / set / anchor / minted ids / explicit ts) ride
    through verbatim. Dedup prefers an explicit op `id` (exact-once semantics for
    envelope ops) and falls back to the exact (verb, args) match. `dedup=False`
    is the bulk path (genesis imports): a full journal rescan per append is
    O(n^2) at import scale — bulk writers dedup upstream or not at all."""
    if not op.get("verb"):
        raise ValueError("append_op: op requires a `verb`")
    if dedup:
        oid = op.get("id")
        for existing in read_journal(path):
            if oid is not None and existing.get("id") == oid:
                return False
            if (oid is None and "id" not in existing
                    and existing.get("verb") == op["verb"] and existing.get("args") == op.get("args")):
                return False  # the exact-match lane is id-less records only — envelope ops never shadow it
    record = dict(op)
    record.setdefault("ts", op_now())
    session = current_session()
    if session and "session" not in record:
        record["session"] = session
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a") as f:
        f.write(json.dumps(record, sort_keys=True) + "\n")
    maybe_rotate(path)  # post-append: a fat op lands, then closes its segment
    return True


def _stem_suffix(
    name: str,  # A journal filename (e.g. "context_graph.writes.jsonl")
) -> tuple:  # (stem, suffix) — the trailing ".jsonl" split off (suffix "" when absent)
    """Split a journal filename for segment naming: cold segments interpose the
    rotation index between stem and suffix (`<stem>.NNNN.jsonl`)."""
    return (name[: -len(".jsonl")], ".jsonl") if name.endswith(".jsonl") else (name, "")


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
    p = Path(path)
    stem, suffix = _stem_suffix(p.name)
    by_index: Dict[str, Path] = {}
    for pattern in (f"{stem}.[0-9][0-9][0-9][0-9]{suffix}.xz",
                    f"{stem}.[0-9][0-9][0-9][0-9]{suffix}"):
        for q in p.parent.glob(pattern):          # plain globbed second: it wins its index
            if q.is_file():
                by_index[q.name[len(stem) + 1:len(stem) + 5]] = q
    cold = [by_index[k] for k in sorted(by_index)]
    return [str(q) for q in cold] + ([str(p)] if p.exists() else [])


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


def rotate_journal(
    path: str,             # The live journal path — its content becomes the next cold segment
    compress: bool = True, # Store the closed segment xz-compressed (ruling f1173d0b)
) -> Optional[str]:  # The cold segment path minted, or None (missing/empty tail)
    """Close the live tail into the next immutable cold segment (explicit rotation).

    A rename, then (by default) the segment is stored compressed by `compress_segment`:
    its bytes are re-ENCODED, never rewritten — decompressing reproduces them exactly,
    so the family read — and therefore replay — is untouched. `append_op` auto-rotates
    past the size budget via `maybe_rotate`; this explicit verb exists for migration
    splits and manual closes. Cold segments are part of the journal — the never-delete
    rule covers every segment, not just the live tail (the plain copy is removed only
    once its compressed form is proven)."""
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return None
    stem, suffix = _stem_suffix(p.name)
    taken = (int(q.name[len(stem) + 1:len(stem) + 5])
             for pattern in (f"{stem}.[0-9][0-9][0-9][0-9]{suffix}",
                             f"{stem}.[0-9][0-9][0-9][0-9]{suffix}.xz")
             for q in p.parent.glob(pattern))
    cold = p.with_name(f"{stem}.{max(taken, default=0) + 1:04d}{suffix}")
    p.rename(cold)
    return compress_segment(str(cold)) if compress else str(cold)


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
    one source at measured spine-extension rates)."""
    p = Path(path)
    if p.exists() and p.stat().st_size >= max_bytes:
        return rotate_journal(path)
    return None
