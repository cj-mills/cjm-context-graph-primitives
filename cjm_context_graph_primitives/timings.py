"""THE INSTRUMENT's collector: phase spans and the storage door's accumulator for ONE run.

Design detail 681604c1 (work item 85b0d368): a run is one CLI invocation, one rebuild phase or
one app gesture; everything measured inside it folds into ONE event for the timings ledger
(`<db stem>.timings.jsonl` beside the db). The ledger is telemetry, never truth -- the reads
ledger's class (45df767d): appended O(1) with dedup off, prunable, never replayed. Recording is
opt-in: with no run armed every call here costs one ContextVar read, and nothing here may break
the work it measures.

THE GRAIN IS THE PHASE: spans aggregate by their nested path (`verb/apply_live/read_journal`)
into calls and inclusive seconds, so a replay's 30k journal appends cost one row, not 30k. A
span's block may add counters (rows, bytes, ops) to its record. The storage door records each
call by method -- calls, seconds, rows in / out, wire bytes, the worker's own seconds -- and a
call with no ids, endpoints or filter as WHOLE scope, under the phase it ran in. A series keeps
a cost by position bin (a replay's per-verb cost against journal position: healthy is flat).
The caller attaches the growth covariates, so a cost's shape is a regression read off the
ledger (ruling d7742c47 (b)).
"""

import functools
import inspect
import json
import os
import sys
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple

# The run being measured (None = nothing armed) and the current span path. The path is per
# async task (a ContextVar), so concurrent tasks nest their own spans; the run's tables are one
# shared object, so every task's measurements land in the same event.
CURRENT_RUN: ContextVar[Optional["Run"]] = ContextVar("cjm_timings_run", default=None)
_PATH: ContextVar[Tuple[str, ...]] = ContextVar("cjm_timings_path", default=())
IO_PHASE_COUNTERS = ("write_bytes", "cancelled_write_bytes", "wchar")  # The disk bytes a top-level phase carries

LEDGER_SUFFIX = ".timings.jsonl"  # The ledger beside a db: <db stem>.timings.jsonl


def timings_path(
    db_path: str,  # The graph db the runs measure
) -> str:  # Its timings ledger path
    """The timings ledger beside a db (`dev-graph.db` -> `dev-graph.timings.jsonl`)."""
    p = Path(db_path)
    return str(p.with_name(p.stem + LEDGER_SUFFIX))


def process_started() -> Optional[float]:  # Unix seconds this process started, or None
    """When this process started, to ~10 ms: its age (uptime less its start tick, both from
    /proc) taken from now -- `btime` is whole seconds, too coarse for a launch phase. None off
    Linux or when /proc is unreadable."""
    try:
        ticks = int(Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()[19])
        uptime = float(Path("/proc/uptime").read_text().split()[0])
        return time.time() - (uptime - ticks / os.sysconf("SC_CLK_TCK"))
    except Exception:
        return None


def read_io() -> Optional[Dict[str, int]]:  # This process's I/O counters, or None when unreadable
    """This process's I/O counters from /proc/self/io (design detail 2f5033bf (1)): read_bytes /
    write_bytes are what reached the block layer, rchar / wchar what the process asked to read and
    write, syscr / syscw its read and write calls. A reaped child's counters accrue to its parent,
    so a CLI run's delta covers its graph worker once the worker is reaped. None off Linux."""
    try:
        with open("/proc/self/io") as f:
            return {k.strip(): int(v) for k, v in (line.split(":", 1) for line in f if ":" in line)}
    except Exception:
        return None


def io_delta(
    before: Optional[Dict[str, int]],  # `read_io` at the start
    after: Optional[Dict[str, int]],   # `read_io` at the end
) -> Optional[Dict[str, int]]:  # after less before, counter by counter; None when either is missing
    """The I/O a stretch of work did."""
    if not before or not after:
        return None
    return {k: v - before.get(k, 0) for k, v in after.items()}


def _env_float(name: str) -> Optional[float]:  # A float env var, or None
    try:
        return float(os.environ[name])
    except (KeyError, ValueError):
        return None


def _add(table: Dict[str, Dict[str, float]], key: str, seconds: float, counters: Dict[str, Any]) -> None:
    """Fold one measurement into a table row: calls + 1, seconds and numeric counters summed."""
    row = table.setdefault(key, {"calls": 0, "s": 0.0})
    row["calls"] += 1
    row["s"] += seconds
    for k, v in counters.items():
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            row[k] = row.get(k, 0) + v
        elif v is True:
            row[k] = row.get(k, 0) + 1


class Run:
    """One measured run: its identity, the phase table, the storage table, the series."""

    def __init__(
        self,
        kind: str,                    # "cli" | "rebuild" | "gesture" | ...
        name: str,                    # The verb / phase / gesture
        run_id: Optional[str] = None,  # Joins a run's events across processes (CJM_RUN_ID)
        **attrs,                      # Identifying context (lane, db, mode, ...)
    ):
        self.kind = kind
        self.name = name
        self.run_id = run_id or os.environ.get("CJM_RUN_ID") or str(uuid.uuid4())
        self.attrs: Dict[str, Any] = dict(attrs)
        tier = os.environ.get("CJM_BENCH_TIER")
        if tier:  # a scale-harness run (design detail 3bd8673d): its tier, kept apart from live runs
            self.attrs.update({"tier": int(tier), "lane": os.environ.get("CJM_BENCH_LANE")})
            cache = os.environ.get("CJM_BENCH_CACHE")
            if cache:  # the harness's cache condition (ruling b7b3976f (B)): cold, then warm
                self.attrs["cache"] = cache
            load = os.environ.get("CJM_BENCH_LOAD")
            if load:  # a concurrent replay run (design detail 2f5033bf (2)): its load, client count, client
                self.attrs.update({"load": load, "clients": int(os.environ.get("CJM_BENCH_CLIENTS") or 0),
                                   "client": int(os.environ.get("CJM_BENCH_CLIENT") or 0)})
        self.started = time.time()
        self.t0 = time.perf_counter()
        self.phases: Dict[str, Dict[str, float]] = {}
        self.storage: Dict[str, Dict[str, float]] = {}
        self.whole: Dict[str, Dict[str, float]] = {}
        self.series: Dict[str, Dict[str, Dict[str, float]]] = {}
        self.covariates: Optional[Dict[str, Any]] = None  # The growth covariates, once measured
        self.plan_scans: Dict[str, Dict[str, float]] = {}  # Storage calls whose plan reads a table whole
        self.plans: Dict[str, Any] = {}  # The storage door's plan per query shape (asked once per run)
        self.appended: Dict[str, Dict[str, int]] = {}  # Journal appends per file: ops, bytes (the logical size)
        self.replayed: Dict[str, Dict[str, int]] = {}  # Journal ops replayed per file: ops, bytes (a replay's logical size)
        self.io0 = read_io()  # The process's I/O counters at the run's start (design detail 2f5033bf (1))
        self._token = None

    def add(self, path: str, seconds: float, **counters) -> None:
        """Record one already-measured phase (e.g. the launch, measured before the run began)."""
        _add(self.phases, path, seconds, counters)

    def event(
        self,
        outcome: Any = None,                         # Exit code / "ok" / an error class name
        covariates: Optional[Dict[str, Any]] = None,  # The growth covariates the caller measured
    ) -> Dict[str, Any]:  # The ledger record (`verb` = the run's name, as the reads ledger)
        """The run folded into one ledger event (seconds rounded to microseconds); the
        covariates default to the ones the run holds."""
        covariates = covariates or self.covariates
        def rounded(table):
            return {k: {f: (round(v, 6) if isinstance(v, float) else v) for f, v in row.items()}
                    for k, row in sorted(table.items())}
        ev: Dict[str, Any] = {
            "verb": self.name, "kind": self.kind, "run_id": self.run_id, "ts": self.started,
            "wall_s": round(time.perf_counter() - self.t0, 6), "outcome": outcome,
            "pid": os.getpid(), "phases": rounded(self.phases), "storage": rounded(self.storage)}
        if self.whole:
            ev["whole"] = rounded(self.whole)
        if self.plan_scans:
            ev["plan_scans"] = rounded(self.plan_scans)
        if self.series:
            ev["series"] = {name: rounded(bins) for name, bins in sorted(self.series.items())}
        io = io_delta(self.io0, read_io())
        if io:
            ev["io"] = io  # the process's I/O over the run (a gesture's is process-wide: an upper bound)
        if self.appended:
            ev["appended"] = {k: dict(v) for k, v in sorted(self.appended.items())}
        if self.replayed:
            ev["replayed"] = {k: dict(v) for k, v in sorted(self.replayed.items())}
        if covariates:
            ev["covariates"] = covariates
        if self.attrs:
            ev["attrs"] = self.attrs
        actor = os.environ.get("CJM_ACTOR")
        if actor:
            ev["actor"] = actor
        return ev


def begin_run(
    kind: str,                     # "cli" | "rebuild" | "gesture" | ...
    name: str,                     # The verb / phase / gesture
    run_id: Optional[str] = None,  # Default: CJM_RUN_ID, else a fresh id
    **attrs,                       # Identifying context
) -> Run:  # The armed run (end it with `end_run`)
    """Arm a run for the current context; every span and storage call below lands in it.

    When the launching wrapper stamped CJM_T0 (its clock just before `conda run`), the gap to
    this process's start is recorded as the `launch` phase and the process's own start to now
    as `import` -- the fixed cost an invocation pays before its verb runs."""
    run = Run(kind, name, run_id=run_id, **attrs)
    started = process_started()
    if started is not None:
        t0 = _env_float("CJM_T0")
        if t0 is not None and started >= t0:
            run.add("launch", started - t0)
        run.add("import", max(0.0, run.started - started))
    run._token = CURRENT_RUN.set(run)
    return run


def end_run(run: Run) -> None:
    """Disarm a run begun with `begin_run` (its tables stay readable for `event`)."""
    if run._token is not None:
        CURRENT_RUN.reset(run._token)
        run._token = None


@contextmanager
def span(
    name: str,  # The phase's name; nested spans join their parents' names with "/"
) -> Iterator[Dict[str, Any]]:  # A dict the block may add counters to (rows, bytes, ops ...)
    """Measure one phase of the armed run (a no-op yielding a scratch dict when none is). A
    top-level phase also carries its disk bytes -- write_bytes, cancelled_write_bytes and wchar
    over the phase (finding 7345f40d: a run's bytes attributed to the phase that wrote them; a
    reaped worker's land in the phase that reaped it)."""
    run = CURRENT_RUN.get()
    counters: Dict[str, Any] = {}
    if run is None:
        yield counters
        return
    path = _PATH.get() + (name,)
    token = _PATH.set(path)
    t = time.perf_counter()
    io0 = read_io() if len(path) == 1 else None
    try:
        yield counters
    finally:
        _PATH.reset(token)
        d = io_delta(io0, read_io())
        if d:
            counters.update({k: d[k] for k in IO_PHASE_COUNTERS if k in d})
        _add(run.phases, "/".join(path), time.perf_counter() - t, counters)


def timed(name: str):
    """Decorator form of `span` for a function (sync or async): each call is one phase."""
    def wrap(fn):
        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def run_async(*args, **kwargs):
                if CURRENT_RUN.get() is None:
                    return await fn(*args, **kwargs)
                with span(name):
                    return await fn(*args, **kwargs)
            return run_async

        @functools.wraps(fn)
        def run_sync(*args, **kwargs):
            if CURRENT_RUN.get() is None:
                return fn(*args, **kwargs)
            with span(name):
                return fn(*args, **kwargs)
        return run_sync
    return wrap


def current_phase() -> str:  # The current span path ("" at the run's top level)
    """The span path a measurement would land under now."""
    return "/".join(_PATH.get())


def record_storage(
    method: str,              # The storage method called
    seconds: float,           # Host-observed seconds for the call (submit to result)
    whole: bool = False,      # The call named no ids, endpoints or filter: it read a whole kind / graph
    scans: Optional[list] = None,  # What its plan reads whole ("nodes" | "edges" | "nodes:label")
    **counters,               # rows_in / rows_out / bytes_in / bytes_out / worker_s / post_s / decode_s
) -> None:
    """Fold one storage call into the armed run (a no-op when none is): by method, and -- when
    the call was whole-scope -- by `<phase> :: <method>`, the guard's evidence of where it ran;
    a call whose PLAN reads a table whole, by `<phase> :: <method> :: <what it scans>` (the plan
    guard, design detail 3bd8673d)."""
    run = CURRENT_RUN.get()
    if run is None:
        return
    _add(run.storage, method, seconds, counters)
    if whole:
        _add(run.whole, f"{current_phase() or '-'} :: {method}", seconds, {})
    if scans:
        _add(run.plan_scans, f"{current_phase() or '-'} :: {method} :: {'+'.join(scans)}",
             seconds, {})


def record_series(
    name: str,        # The series (e.g. "replay:assert")
    position: int,    # The measured item's position (e.g. its journal offset)
    seconds: float,   # Its cost
    bin_size: int = 1000,  # Positions per bin
) -> None:
    """Fold one positioned cost into its bin (the armed run's series; a no-op when none is):
    a cost that rises with position is history-shaped, the shape d7742c47 (b) reads."""
    run = CURRENT_RUN.get()
    if run is None:
        return
    bins = run.series.setdefault(name, {})
    _add(bins, str(position // bin_size * bin_size), seconds, {})


def record_append(
    path: str,    # The journal appended to (its live path)
    nbytes: int,  # Bytes appended: the op's line
) -> None:
    """Fold one journal append into the armed run (a no-op when none is): ops and bytes per
    journal file -- a write's logical size, the denominator of its write amplification (design
    detail 2f5033bf (1))."""
    run = CURRENT_RUN.get()
    if run is None:
        return
    row = run.appended.setdefault(Path(path).name, {"ops": 0, "bytes": 0})
    row["ops"] += 1
    row["bytes"] += nbytes


def record_replay(
    path: str,           # The journal replayed (its live path)
    op: Dict[str, Any],  # One op the replay applied
) -> None:
    """Fold one replayed op into the armed run (a no-op when none is): ops and bytes per journal
    file, each op at its journaled line's size -- a replay's logical size, the denominator of its
    write amplification (ruling 9af0e42e (3): the tail at most 32x over the journal bytes it
    replays)."""
    run = CURRENT_RUN.get()
    if run is None:
        return
    row = run.replayed.setdefault(Path(path).name, {"ops": 0, "bytes": 0})
    row["ops"] += 1
    row["bytes"] += len((json.dumps(op, sort_keys=True) + "\n").encode("utf-8"))


def append_event(
    path: str,              # The timings ledger (JSONL)
    event: Dict[str, Any],  # One run's event
) -> bool:  # True when appended
    """Append one timing event: O(1), dedup off (every run is one event), fail-open -- the
    instrument must never break the work it measured. The one append every recorder shares
    (the CLI's invocations, the harness, an app's gestures)."""
    from cjm_context_graph_primitives.journal import append_op  # the journal imports this module
    try:
        return append_op(path, event, dedup=False)
    except Exception as e:
        print(f"warning: timings-ledger append failed (the run itself completed): {e}",
              file=sys.stderr)
        return False
