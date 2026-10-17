# cjm-context-graph-primitives

<!-- generated from the context graph by `cjm-context-graph readme` — do not edit by hand; edit the graph (the urge to hand-edit = move it on-graph) -->

The dep-light data-primitives library for the cjm context-graph ecosystem — the shared data nouns that workflow cores, graph-storage adapters, provenance bundles, and the graph-aware composition layer all speak: structured resource locators with canonical URI rendering, content-hash-primary SourceRef provenance references, atomic typed content slices, GraphNode/GraphEdge/GraphContext containers, and the structured typed query expression executed by graph-storage adapters. Data nouns vs storage verbs: everything that consumes or produces graph data depends on this library; only persistence depends on a graph-storage adapter. Capabilities never construct SourceRef — the composition layer does.

## Modules

- **`cjm_context_graph_primitives.__init__`**
- **`cjm_context_graph_primitives.graph`** — The graph data nouns - GraphNode / GraphEdge / GraphContext. Moved here from cjm-graph-plugin-system per the data-nouns-vs-storage-verbs split (pass-2 Thread 2): every consumer of graph DATA (workflow cores, bundles, the CR-18 graph-aware layer, the storage adapter itself) depends on this library; only persistence depends on the storage adapter. GraphContext satisfies the substrate's FileBackedDTO protocol (to_temp_file) for zero-copy worker transfer. All three nouns are wire-registered (stage 4): graph-storage adapter methods return them typed across the worker boundary.
- **`cjm_context_graph_primitives.journal`** — The write-journal core: append-only JSONL ops — the durable, replayable source of truth.
- **`cjm_context_graph_primitives.locators`** — Structured resource locators - the typed sum type addressing WHERE referenced content lives (CR-19). A locator renders a canonical URI string for the things strings are good at (grep, logs, cache keys, display) while keeping typed field access primary; unknown kinds round-trip losslessly for forward compatibility.
- **`cjm_context_graph_primitives.provenance`** — SourceRef - the cross-cutting provenance reference (CR-19). Identity = content_hash (PRIMARY); location = locator; region = optional atomic typed slice. verify() is hash-based regardless of whether the locator still resolves - the structural fix for dangling row-id provenance (cache-hit rows; ledgers E13/D3).
- **`cjm_context_graph_primitives.query`** — The structured typed query expression (pass-2 Thread 5) - DATA nouns describing graph reads. Execution lives in graph-storage adapters (stage 4 translates expressions per-backend); this library only defines, validates, and (de)serializes them. Typed expressions are the primary, portable surface - no storage-schema leak (the raw-SQL nodes/edges + json_extract coupling of ledger C2/C3), and scale-shaped (server-side filter/page/count answering D13). RawQuery is the explicitly-marked, backend-coupled escape hatch: recurring raw patterns EXPOSE missing typed-expression capabilities.
- **`cjm_context_graph_primitives.slices`** — Atomic typed content slices - WHAT REGION of the located resource a reference consumes. The slice KIND selects the content facet on multi-facet nodes (TimeSlice -> audio, CharSlice -> text), which dissolves the chunk-local-vs-source-coordinate ambiguity without a frame field (pass-2 Thread 2).
- **`cjm_context_graph_primitives.timings`** — THE INSTRUMENT's collector: phase spans and the storage door's accumulator for ONE run.
- **`cjm_context_graph_primitives.writer`** — THE WRITE ORDER: plan -> append -> apply -> advance, one writer per journal family.

## API

### `cjm_context_graph_primitives.graph`

- `GraphContext` _class_ — Container for graph read results (a subgraph).
- `GraphEdge` _class_ — A relationship between two nodes. Composition is ALWAYS edges — grouping,
- `GraphNode` _class_ — An entity in a context graph.

### `cjm_context_graph_primitives.journal`

- `AppendedRecord` _class_ — One record `append_record` landed, and where: a writer advances its applied cursor to it
- `append_op` _function_ — Append one op record — the envelope-agnostic core `append_write` wraps; `append_record`
- `append_record` _function_ — Append one op record and say where it landed — the core `append_op` answers True / False for.
- `append_records` _function_ — Append an atomic unit's records in ONE write (ruling 3de795e4) and say where each landed. No
- `append_write` _function_ — Append one write op (skipping an exact (verb,args) duplicate) its caller has ALREADY
- `committed_length` _function_ — How much of the live tail is COMMITTED (ruling 3de795e4): everything but a partial last line
- `compress_cold_segments` _function_ — Compress every PLAIN cold segment of a family (the live tail is never touched).
- `compress_segment` _function_ — Store one cold segment xz-compressed, proven lossless before the plain copy goes.
- `current_session` _function_ — The session key stamped on journal appends (provenance, not replay input).
- `family_lock` _function_ — The family's exclusive writer lock: an flock on its own open of the lock file, so it
- `family_lock_sync` _function_ — `family_lock` for a synchronous family act -- a rotation (ruling 6683f49e (3)). Joins an
- `indexed_segments` _function_ — The SEGMENT FAMILY with each segment's index: a cold segment by its NNNN, the live tail at
- `journal_segments` _function_ — The journal's SEGMENT FAMILY: rotated cold segments + the live tail.
- `journaled` _function_ — The append dedup rule: an explicit op `id` is exact-once; an id-less op matches an id-less
- `live_index` _function_ — The live tail's place in its family: one past the highest cold index, plain or compressed.
- `lock_path` _function_
- `maybe_rotate` _function_ — Rotate the live tail once it reaches the budget (the POST-append check).
- `op_clock` _function_ — Open one op's clock window: reuse an open one (replay, or an enclosing write unit),
- `op_clocked` _function_ — Decorator form of `op_clock` for a write entry point (sync or async): every db write
- `op_now` _function_ — The op clock's current value: every write-path time reads THIS, never `time.time()`.
- `read_journal` _function_ — Read every journaled write op across the SEGMENT FAMILY (one JSON object per
- `read_segment` _function_ — Read one journal segment's text, plain or xz-compressed.
- `rotate_journal` _function_ — Close the live tail into the next immutable cold segment (explicit rotation).
- `seal_tail` _function_ — Cut an uncommitted end off the live tail (ruling 3de795e4): a partial last line and a
- `stamp_record` _function_ — Stamp an op for the journal, each field only when ABSENT -- at its append, or as a unit
- `write_segment` _function_ — Write one segment atomically, in its own encoding (plain or xz-compressed).

### `cjm_context_graph_primitives.locators`

- `FileRef` _class_ — Locator for a filesystem artifact (e.g. a consumed run manifest).
- `GraphNodeRef` _class_ — Locator for a node in a context graph.
- `UnknownLocator` _class_ — Lossless carrier for a locator kind this library version does not know.
- `locator_from_dict` _function_ — Reconstruct a locator from its wire dict.

### `cjm_context_graph_primitives.provenance`

- `SourceRef` _class_ — A provenance reference to (a region of) a resource.

### `cjm_context_graph_primitives.query`

- `EdgeQuery` _class_ — Typed edge read: filter / order / page / project / count.
- `EdgeQueryResult` _class_ — Typed result of an `EdgeQuery` — exactly one field is populated,
- `NodeQuery` _class_ — Typed node read: filter / traverse / order / page / project / count.
- `NodeQueryResult` _class_ — Typed result of a `NodeQuery` — exactly one field is populated,
- `OrderBy` _class_ — Result ordering by one property (server-side; C2/C3 ORDER BY index).
- `PropertyPredicate` _class_ — One property comparison; a query's `where` list combines predicates with AND.
- `RawQuery` _class_ — The explicitly-marked, backend-coupled escape hatch.
- `RawQueryResult` _class_ — Typed result of a `RawQuery` — tabular, backend-shaped (the columns are
- `RelationPredicate` _class_ — Match nodes that have an edge of `relation_type` (one-hop, typed traversal —
- `SourcePredicate` _class_ — Match nodes whose `sources` contain a reference matching by content hash
- `query_from_dict` _function_ — Reconstruct a query expression from its tagged wire dict.
- `result_from_dict` _function_ — Reconstruct a query result from its tagged wire dict.

### `cjm_context_graph_primitives.slices`

- `CharSlice` _class_ — Character-range slice into a text facet.
- `FrameSlice` _class_ — Frame-range slice into a video facet.
- `FullContent` _class_ — Whole-facet reference (no range) — selects a content facet without slicing.
- `LineSlice` _class_ — Line-range slice into code or structured text.
- `PageSlice` _class_ — Page slice into a paginated document facet (PDF, EPUB).
- `TimeSlice` _class_ — Temporal slice into an audio/video facet, in seconds.
- `UnknownSlice` _class_ — Lossless carrier for a slice kind this library version does not know.
- `parse_slice` _function_ — Parse a canonical slice string into a typed slice.
- `slice_from_dict` _function_ — Reconstruct a typed slice from its wire dict.

### `cjm_context_graph_primitives.timings`

- `Run` _class_ — One measured run: its identity, the phase table, the storage table, the series.
- `append_event` _function_ — Append one timing event: O(1), dedup off (every run is one event), fail-open -- the
- `begin_run` _function_ — Arm a run for the current context; every span and storage call below lands in it.
- `current_phase` _function_ — The span path a measurement would land under now.
- `end_run` _function_ — Disarm a run begun with `begin_run` (its tables stay readable for `event`).
- `io_delta` _function_ — The I/O a stretch of work did.
- `process_started` _function_ — When this process started, to ~10 ms: its age (uptime less its start tick, both from
- `read_io` _function_ — This process's I/O counters from /proc/self/io (design detail 2f5033bf (1)): read_bytes /
- `record_append` _function_ — Fold one journal append into the armed run (a no-op when none is): ops and bytes per
- `record_replay` _function_ — Fold one replayed op into the armed run (a no-op when none is): ops and bytes per journal
- `record_series` _function_ — Fold one positioned cost into its bin (the armed run's series; a no-op when none is):
- `record_storage` _function_ — Fold one storage call into the armed run (a no-op when none is): by method, and -- when
- `span` _function_ — Measure one phase of the armed run (a no-op yielding a scratch dict when none is). A
- `timed` _function_ — Decorator form of `span` for a function (sync or async): each call is one phase.
- `timings_path` _function_ — The timings ledger beside a db (`dev-graph.db` -> `dev-graph.timings.jsonl`).

### `cjm_context_graph_primitives.writer`

- `CursorMismatch` _class_ — The family under a cursor is not the one it applied -- rewritten (compaction) or paired
- `CursorStore` _class_ — Where a db keeps its applied cursors: beside the graph, one per journal family.
- `JournalPosition` _class_ — How far into a journal family a db has applied: the segment holding the last applied
- `JournalWriter` _class_ — One writer's unit of work on one journal family (65f4a6c9): `async with` takes the
- `NoCursor` _class_ — The db holds no applied cursor for a family that holds records: it predates the cursor or
- `family_name` _function_ — A family is keyed by its live tail's FILE NAME, never its path: a db and its journals move
- `fault_armed` _function_ — Whether a proof may crash at `point` -- so the write path takes a fault's shape (a torn
- `fault_point` _function_ — A crash-injection point on the write path: the in-process hook, then the armed env.
- `journal_lag` _function_ — How far a db trails its journal, for a READER to report (65f4a6c9 (2d)): readers never
- `read_tail` _function_ — Every record of the family past `position`, in append order -- only the tail is read (the
- `stamp_cursor` _function_ — Record the family's END as the db's cursor -- for a db that holds the whole family: a

## Dependencies

**Depends on:** `cjm-substrate`
**Used by:** `cjm-capability-graph-sqlite`, `cjm-context-graph-layer`, `cjm-context-graph-projection`, `cjm-dev-graph-schema`, `cjm-graph-storage-adapter-interface`, `cjm-graph-workbench-qt`, `cjm-markdown-decompose-core`, `cjm-notebook-decompose-core`, `cjm-python-decompose-core`, `cjm-session-scratchpad-qt`, `cjm-substrate-qt-kit`, `cjm-transcript-correction-core`, `cjm-transcript-correction-qt`, `cjm-transcript-decomp-core`, `cjm-transcript-graph-schema`, `cjm-transcription-core`, `cjm-workflow-hub-qt`
