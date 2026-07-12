"""Tests for cjm_context_graph_primitives.locators — typed resource locators (CR-19).

Projected from the locators notebook's test cells at the c25780e8 flip."""
import pytest

from cjm_context_graph_primitives.locators import (FileRef, GraphNodeRef, UnknownLocator,
                                                   locator_from_dict)
from cjm_substrate.core.errors import CapabilityInputError


def test_graph_node_ref():
    ref = GraphNodeRef(node_id="abc-123")
    assert ref.to_uri() == "graph-node:abc-123"
    assert str(ref) == "graph-node:abc-123"
    cross = GraphNodeRef(node_id="abc-123", graph_id="g9")
    assert cross.to_uri() == "graph-node:g9/abc-123"
    assert ref.to_dict() == {"kind": "graph-node", "node_id": "abc-123", "graph_id": None}
    # frozen value object: equality + hashability
    assert ref == GraphNodeRef(node_id="abc-123")
    assert hash(ref) == hash(GraphNodeRef(node_id="abc-123"))
    assert ref != cross


def test_file_ref():
    f = FileRef(path="/runs/run_x.json")
    assert f.to_uri() == "file:/runs/run_x.json"
    assert f.to_dict() == {"kind": "file", "path": "/runs/run_x.json"}
    spaced = FileRef(path="/media/Show 62 - Supernova in the East I.mp3")
    assert spaced.to_uri() == "file:/media/Show 62 - Supernova in the East I.mp3"
    assert hash(f) == hash(FileRef(path="/runs/run_x.json"))


def test_locator_from_dict_roundtrips_and_strictness():
    for loc in (GraphNodeRef(node_id="n1"), GraphNodeRef(node_id="n1", graph_id="g1"),
                FileRef(path="/a/b.json")):
        assert locator_from_dict(loc.to_dict()) == loc

    # unknown-kind round-trip is byte-identical on re-serialization
    future = {"kind": "actor", "actor_id": "agent:claude", "extra": {"v": 2}}
    u = locator_from_dict(future)
    assert isinstance(u, UnknownLocator) and u.to_dict() == future
    assert u.to_uri() == 'actor:{"actor_id":"agent:claude","extra":{"v":2}}'

    # strict on known kinds: malformed payload raises
    with pytest.raises(CapabilityInputError):
        locator_from_dict({"kind": "file", "path": "/x", "surprise": 1})
    with pytest.raises(CapabilityInputError):
        locator_from_dict({"node_id": "n1"})
