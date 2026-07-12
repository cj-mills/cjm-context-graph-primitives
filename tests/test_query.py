"""Tests for cjm_context_graph_primitives.query — typed query expressions + results.

Projected from the query notebook's two test cells at the c25780e8 flip: the
evidenced core read patterns (documentation-as-test) + result-DTO round-trips."""
import json

import pytest

from cjm_context_graph_primitives.graph import GraphEdge, GraphNode
from cjm_context_graph_primitives.query import (EdgeQuery, EdgeQueryResult, NodeQuery,
                                                NodeQueryResult, OrderBy,
                                                PropertyPredicate, RawQuery,
                                                RawQueryResult, RelationPredicate,
                                                SourcePredicate, query_from_dict,
                                                result_from_dict)
from cjm_substrate.core.errors import CapabilityInputError
from cjm_substrate.core.wire import WIRE_KIND_KEY, wire_decode, wire_encode


def test_evidenced_core_read_patterns_roundtrip():
    # 1. C2/C3 spine read: ordered projection
    spine = NodeQuery(
        label="Segment",
        related=RelationPredicate(relation_type="PART_OF", direction="out", node_id="doc-1"),
        order_by=OrderBy(prop="index"),
        project=["index", "text", "start_time", "end_time", "sources"])
    assert query_from_dict(spine.to_dict()) == spine

    # 2. Prune-candidate read (eq half of the OR case)
    empties = NodeQuery(
        label="Segment",
        where=[PropertyPredicate(prop="text", op="eq", value="")],
        related=RelationPredicate(relation_type="PART_OF", node_id="doc-1"),
        count=True)
    assert query_from_dict(empties.to_dict()) == empties

    # 3. Cross-transcript correction-cache read (far-end node_source)
    by_hash = NodeQuery(
        label="Correction",
        related=RelationPredicate(relation_type="CORRECTS", direction="out",
                                  node_source=SourcePredicate(content_hash="sha256:ab")))
    assert query_from_dict(by_hash.to_dict()) == by_hash

    # 4. C17 batch far end
    batch_corrections = NodeQuery(
        label="Correction",
        related=RelationPredicate(relation_type="CORRECTS", node_ids=["s1", "s2", "s3"]))
    assert query_from_dict(batch_corrections.to_dict()) == batch_corrections

    # 5. D13 verify aggregate: server-side NEXT count scoped by source_related
    next_count = EdgeQuery(
        relation_type="NEXT",
        source_related=RelationPredicate(relation_type="PART_OF", node_id="doc-1"),
        count=True)
    assert query_from_dict(next_count.to_dict()) == next_count

    # 6. Superseded-set read
    superseded = EdgeQuery(relation_type="SUPERSEDES", target_ids=["c1", "c2"],
                           project=["target_id"])
    assert query_from_dict(superseded.to_dict()) == superseded

    # 7. Dotted property path
    doc_corrections = NodeQuery(
        label="Correction",
        where=[PropertyPredicate(prop="payload.document_id", op="eq", value="doc-1")])
    assert query_from_dict(doc_corrections.to_dict()) == doc_corrections

    # batch-by-id (C17)
    batch = NodeQuery(ids=["n1", "n2", "n3"])
    assert query_from_dict(batch.to_dict()) == batch

    # raw escape is marked: backend required
    raw = RawQuery(text="SELECT COUNT(*) FROM nodes", backend="sqlite")
    assert query_from_dict(raw.to_dict()) == raw
    with pytest.raises(CapabilityInputError):
        RawQuery(text="SELECT 1", backend="")


def test_validation_raises_loudly():
    bads = (lambda: PropertyPredicate("text", "like", "%x%"),
            lambda: PropertyPredicate("text", "is_null", value=1),
            lambda: RelationPredicate("NEXT", direction="up"),
            lambda: RelationPredicate("NEXT", node_id="a", node_ids=["b"]),
            lambda: EdgeQuery(source_id="a", source_ids=["b"]),
            lambda: EdgeQuery(target_id="a", target_ids=["b"]),
            lambda: SourcePredicate())
    for bad in bads:
        with pytest.raises(CapabilityInputError):
            bad()

    with pytest.raises(CapabilityInputError):
        query_from_dict({"type": "graph_query"})


def test_result_dto_roundtrips():
    node = GraphNode(id="n1", label="Segment", properties={"index": 0, "text": "hi"})
    edge = GraphEdge(id="e1", source_id="n1", target_id="n2", relation_type="NEXT")

    nodes_res = NodeQueryResult(nodes=[node])
    rows_res = NodeQueryResult(rows=[{"id": "n1", "index": 0, "text": "hi"}])
    count_res = NodeQueryResult(count=3579)
    edge_res = EdgeQueryResult(edges=[edge])
    edge_rows = EdgeQueryResult(rows=[{"id": "e1", "source_id": "n1", "target_id": "n2",
                                       "decision": "corrected"}])
    raw_res = RawQueryResult(columns=["c"], rows=[[1]], row_count=1, backend="sqlite")

    # tagged-dict round-trip via the registry
    for r in (nodes_res, rows_res, count_res, edge_res, edge_rows, raw_res):
        assert result_from_dict(r.to_dict()) == r

    # wire-envelope round-trip (typed across the worker boundary, surviving JSON)
    for r, kind in ((nodes_res, "graph.node_query_result"),
                    (edge_res, "graph.edge_query_result"),
                    (raw_res, "graph.raw_query_result")):
        env = wire_encode(r)
        assert env[WIRE_KIND_KEY] == kind
        assert wire_decode(json.loads(json.dumps(env))) == r, kind

    # nested nodes reconstruct typed through the envelope
    decoded = wire_decode(json.loads(json.dumps(wire_encode(nodes_res))))
    assert isinstance(decoded.nodes[0], GraphNode)
    assert decoded.nodes[0].properties["index"] == 0

    with pytest.raises(CapabilityInputError):
        result_from_dict({"type": "mystery_result"})
