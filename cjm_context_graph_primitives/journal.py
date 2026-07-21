"""The write-journal core: append-only JSONL ops — the durable, replayable source of truth.

The shared PRIMITIVE under every workflow journal (DEC ccbab9f5 point 1): a graph db is a
rebuildable PROJECTION; the non-re-derivable knowledge lives as an append-only log of write
ops. Domain layers own their op vocabularies and REPLAY (each registers its own verbs);
this module owns the discipline — append-on-success with exact-duplicate skip, read in
append order, session stamping for provenance.
"""

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


def read_journal(
    path: str,  # Journal file path (JSONL)
) -> List[Dict[str, Any]]:  # The recorded ops, in append order
    """Read every journaled write op across the SEGMENT FAMILY (one JSON object per
    line; missing files = []) — cold segments first, live tail last: append order."""
    ops: List[Dict[str, Any]] = []
    for seg in journal_segments(path):
        for line in Path(seg).read_text().splitlines():
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

    Stamps `ts` (and `session` from CJM_SESSION) only when ABSENT, so a domain
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
    record.setdefault("ts", time.time())
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
    100MB; rotation keeps every journal file small forever."""
    p = Path(path)
    stem, suffix = _stem_suffix(p.name)
    cold = sorted(q for q in p.parent.glob(f"{stem}.[0-9][0-9][0-9][0-9]{suffix}") if q.is_file())
    return [str(q) for q in cold] + ([str(p)] if p.exists() else [])


def rotate_journal(
    path: str,  # The live journal path — its content becomes the next cold segment
) -> Optional[str]:  # The cold segment path minted, or None (missing/empty tail)
    """Close the live tail into the next immutable cold segment (explicit rotation).

    Rename-only: no bytes are rewritten, so the family concatenation — and
    therefore replay — is untouched. `append_op` auto-rotates past the size
    budget via `maybe_rotate`; this explicit verb exists for migration splits
    and manual closes. Cold segments are part of the journal — the never-delete
    rule covers every segment, not just the live tail."""
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return None
    stem, suffix = _stem_suffix(p.name)
    taken = (int(q.name[len(stem) + 1:len(stem) + 5])
             for q in p.parent.glob(f"{stem}.[0-9][0-9][0-9][0-9]{suffix}"))
    cold = p.with_name(f"{stem}.{max(taken, default=0) + 1:04d}{suffix}")
    p.rename(cold)
    return str(cold)


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
