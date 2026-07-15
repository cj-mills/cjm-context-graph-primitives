# cjm-context-graph-primitives

<!-- generated from the context graph by `cjm-context-graph readme` — do not edit by hand; edit the graph (the urge to hand-edit = move it on-graph) -->

The dep-light data-primitives library for the cjm context-graph ecosystem — the shared data nouns that workflow cores, graph-storage adapters, provenance bundles, and the graph-aware composition layer all speak: structured resource locators with canonical URI rendering, content-hash-primary SourceRef provenance references, atomic typed content slices, GraphNode/GraphEdge/GraphContext containers, and the structured typed query expression executed by graph-storage adapters. Data nouns vs storage verbs: everything that consumes or produces graph data depends on this library; only persistence depends on a graph-storage adapter. Capabilities never construct SourceRef — the composition layer does.

## Modules

- **`cjm_context_graph_primitives.graph`** — The graph data nouns - GraphNode / GraphEdge / GraphContext. Moved here from cjm-graph-plugin-system per the data-nouns-vs-storage-verbs split (pass-2 Thread 2): every consumer of graph DATA (workflow cores, bundles, the CR-18 graph-aware layer, the storage adapter itself) depends on this library; only persistence depends on the storage adapter. GraphContext satisfies the substrate's FileBackedDTO protocol (to_temp_file) for zero-copy worker transfer. All three nouns are wire-registered (stage 4): graph-storage adapter methods return them typed across the worker boundary.
- **`cjm_context_graph_primitives.journal`** — The write-journal core: append-only JSONL ops — the durable, replayable source of truth.
- **`cjm_context_graph_primitives.locators`** — Structured resource locators - the typed sum type addressing WHERE referenced content lives (CR-19). A locator renders a canonical URI string for the things strings are good at (grep, logs, cache keys, display) while keeping typed field access primary; unknown kinds round-trip losslessly for forward compatibility.
- **`cjm_context_graph_primitives.provenance`** — SourceRef - the cross-cutting provenance reference (CR-19). Identity = content_hash (PRIMARY); location = locator; region = optional atomic typed slice. verify() is hash-based regardless of whether the locator still resolves - the structural fix for dangling row-id provenance (cache-hit rows; ledgers E13/D3).
- **`cjm_context_graph_primitives.query`** — The structured typed query expression (pass-2 Thread 5) - DATA nouns describing graph reads. Execution lives in graph-storage adapters (stage 4 translates expressions per-backend); this library only defines, validates, and (de)serializes them. Typed expressions are the primary, portable surface - no storage-schema leak (the raw-SQL nodes/edges + json_extract coupling of ledger C2/C3), and scale-shaped (server-side filter/page/count answering D13). RawQuery is the explicitly-marked, backend-coupled escape hatch: recurring raw patterns EXPOSE missing typed-expression capabilities.
- **`cjm_context_graph_primitives.slices`** — Atomic typed content slices - WHAT REGION of the located resource a reference consumes. The slice KIND selects the content facet on multi-facet nodes (TimeSlice -> audio, CharSlice -> text), which dissolves the chunk-local-vs-source-coordinate ambiguity without a frame field (pass-2 Thread 2).

## API

### `cjm_context_graph_primitives.graph`

- `GraphContext` _class_ — Container for graph read results (a subgraph).
- `GraphEdge` _class_ — A relationship between two nodes. Composition is ALWAYS edges — grouping,
- `GraphNode` _class_ — An entity in a context graph.

### `cjm_context_graph_primitives.journal`

- `append_op` _function_ — Append one op record — the envelope-agnostic core `append_write` wraps.
- `append_write` _function_ — Append one write op (skipping an exact (verb,args) duplicate).
- `current_session` _function_ — The session key stamped on journal appends (provenance, not replay input).
- `read_journal` _function_ — Read every journaled write op (one JSON object per line; missing file = []).

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

## Dependencies

**Depends on:** `cjm-substrate`
**Used by:** `cjm-capability-graph-sqlite`, `cjm-context-graph-layer`, `cjm-context-graph-projection`, `cjm-dev-graph-schema`, `cjm-markdown-decompose-core`, `cjm-notebook-decompose-core`, `cjm-python-decompose-core`, `cjm-transcript-correction-core`, `cjm-transcript-correction-tui`, `cjm-transcript-decomp-core`, `cjm-transcript-graph-schema`
