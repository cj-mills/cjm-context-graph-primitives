"""Tests for cjm_context_graph_primitives.provenance — SourceRef (CR-19).

Projected from the provenance notebook's test cell at the c25780e8 flip."""
import pytest

from cjm_context_graph_primitives.locators import FileRef, GraphNodeRef, UnknownLocator
from cjm_context_graph_primitives.provenance import SourceRef
from cjm_context_graph_primitives.slices import CharSlice, TimeSlice
from cjm_substrate.core.errors import CapabilityInputError


def test_identity_vs_location_split():
    text = "It's one small step for man,"
    ref = SourceRef(locator=FileRef(path="/runs/run_x.json"),
                    content_hash=SourceRef.compute_hash(text.encode()),
                    slice=CharSlice(25, 53))
    # verify() works regardless of locator resolution
    assert ref.verify(text.encode())
    assert not ref.verify(b"tampered")
    # canonical render carries locator, slice, and hash
    assert ref.to_uri().startswith("file:/runs/run_x.json#char:25-53@sha256:")
    # wire round-trip
    assert SourceRef.from_dict(ref.to_dict()) == ref


def test_whole_resource_and_unknown_locator():
    whole = SourceRef(locator=GraphNodeRef(node_id="n1"), content_hash="sha256:00")
    assert whole.to_dict()["slice"] is None
    assert SourceRef.from_dict(whole.to_dict()) == whole
    assert whole.to_uri() == "graph-node:n1@sha256:00"

    # unknown locator kind inside a SourceRef round-trips losslessly
    fut = {"locator": {"kind": "actor", "actor_id": "agent:claude"},
           "content_hash": "sha256:ab", "slice": None}
    r2 = SourceRef.from_dict(fut)
    assert isinstance(r2.locator, UnknownLocator) and r2.to_dict() == fut


def test_kind_selects_facet_and_strictness():
    audio_ref = SourceRef(locator=GraphNodeRef(node_id="seg-9"),
                          content_hash="sha256:aa", slice=TimeSlice(6.6, 9.8))
    text_ref = SourceRef(locator=GraphNodeRef(node_id="seg-9"),
                         content_hash="sha256:bb", slice=CharSlice(0, 28))
    assert audio_ref != text_ref and audio_ref.locator == text_ref.locator

    with pytest.raises(CapabilityInputError):
        SourceRef.from_dict({"content_hash": "sha256:00"})
