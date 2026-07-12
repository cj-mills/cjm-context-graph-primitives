"""Tests for cjm_context_graph_primitives.slices — atomic typed content slices.

Projected from the slices notebook's test cell at the c25780e8 flip."""
import pytest

from cjm_context_graph_primitives.slices import (CharSlice, FrameSlice, FullContent,
                                                 LineSlice, PageSlice, TimeSlice,
                                                 UnknownSlice, parse_slice, slice_from_dict)
from cjm_substrate.core.errors import CapabilityInputError


def test_roundtrips_every_known_kind():
    known = [CharSlice(0, 500), TimeSlice(6.6, 9.8), FrameSlice(0, 120),
             LineSlice(10, 25), PageSlice(3), PageSlice(3, bbox="10,20,300,400"),
             FullContent("audio")]
    for sl in known:
        assert parse_slice(sl.to_slice_string()) == sl, sl
        assert slice_from_dict(sl.to_dict()) == sl, sl

    assert str(TimeSlice(6.6, 9.8)) == "time:6.6-9.8"
    assert str(FullContent("audio")) == "full:audio"


def test_unknown_kind_wire_roundtrip():
    fut = {"kind": "bbox3d", "x": 1, "y": 2}
    u = slice_from_dict(fut)
    assert isinstance(u, UnknownSlice) and u.to_dict() == fut


def test_validation_and_legacy_break():
    for bad in (lambda: CharSlice(5, 2), lambda: TimeSlice(-1.0, 2.0),
                lambda: PageSlice(0)):
        with pytest.raises(CapabilityInputError):
            bad()

    # legacy forms are a clean break
    with pytest.raises(CapabilityInputError):
        parse_slice("timestamp:10.5-30.0")
