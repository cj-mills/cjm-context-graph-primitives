"""Tests for cjm_context_graph_primitives.graph — the graph data nouns.

Projected from the graph notebook's test cell at the c25780e8 flip."""
import json
import os

from cjm_context_graph_primitives.graph import GraphContext, GraphEdge, GraphNode
from cjm_context_graph_primitives.locators import FileRef, GraphNodeRef
from cjm_context_graph_primitives.provenance import SourceRef
from cjm_context_graph_primitives.slices import CharSlice, TimeSlice
from cjm_substrate.core.wire import WIRE_KIND_KEY, wire_decode, wire_encode

REF = SourceRef(locator=FileRef(path="/runs/run_x.json"),
                content_hash="sha256:ab", slice=CharSlice(0, 10))


def _ctx():
    n1 = GraphNode(id="n1", label="Segment", properties={"index": 0, "text": "hello"},
                   sources=[REF], created_at=1.0)
    n2 = GraphNode(id="n2", label="Segment", properties={"index": 1})
    e = GraphEdge(id="e1", source_id="n1", target_id="n2", relation_type="NEXT")
    return GraphContext(nodes=[n1, n2], edges=[e], metadata={"query": "test"})


def test_wire_roundtrip_preserves_nested_sourcerefs():
    ctx = _ctx()
    back = GraphContext.from_dict(ctx.to_dict())
    assert back.nodes[0].sources[0] == REF
    assert back.edges[0].relation_type == "NEXT"
    assert back.metadata == {"query": "test"}


def test_filebacked_dto_roundtrip():
    ctx = _ctx()
    p = ctx.to_temp_file()
    try:
        again = GraphContext.from_file(p)
        assert again.nodes[0].sources[0] == REF and len(again.nodes) == 2
    finally:
        os.unlink(p)


def test_multi_facet_node_slice_kind_selects_facet():
    seg = GraphNode(id="s1", label="Segment", sources=[
        SourceRef(locator=GraphNodeRef(node_id="audio-seg-3"),
                  content_hash="sha256:aa", slice=TimeSlice(0.0, 4.2)),
        SourceRef(locator=GraphNodeRef(node_id="transcript-7"),
                  content_hash="sha256:bb", slice=CharSlice(120, 180)),
    ])
    assert GraphNode.from_dict(seg.to_dict()).sources == seg.sources


def test_wire_envelope_roundtrip_typed_across_worker_boundary():
    ctx = _ctx()
    for obj, kind in ((ctx.nodes[0], "graph.node"), (ctx.edges[0], "graph.edge"),
                      (ctx, "graph.context")):
        env = wire_encode(obj)
        assert env[WIRE_KIND_KEY] == kind
        env = json.loads(json.dumps(env))  # survives JSON transport
        assert wire_decode(env) == obj, kind
