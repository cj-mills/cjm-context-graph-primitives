"""Structured resource locators - the typed sum type addressing WHERE referenced content lives (CR-19). A locator renders a canonical URI string for the things strings are good at (grep, logs, cache keys, display) while keeping typed field access primary; unknown kinds round-trip losslessly for forward compatibility.

Design notes:

**Build-now kinds** (the two the workflow cores evidence demands):
`GraphNodeRef` (intra/cross-graph node references — retires the
`external:<path>` hack) and `FileRef` (filesystem artifacts, e.g. a consumed
run manifest).

**Design admits, NOT built** (add a kind when an adopter exists —
generalize-seam-one-level): `CapabilityRowRef(capability_id, table, row_id)` —
a row in a capability-owned data store (the old `SourceRef.plugin_name` /
`table_name`/`row_id` triple, structured; dropped from build-now because
content-addressing makes dangling rows survivable and nothing constructs row
refs today) · `UrlRef(url)` — web resources · `ArtifactRef(hash)` —
content-addressed artifacts · `ActorRef(id)` / `EventRef(id)` —
asserted/derived provenance roots (also the CR-14 graph-tier logging anchor).

**Forward-compatibility law:** a consumer must round-trip locator kinds it
cannot interpret (`UnknownLocator`); `SourceRef.content_hash` still verifies
content behind an un-understood locator. Known kinds are STRICT — schema
evolution of a known kind is governed by this library own versioning (this
lib is the schema authority every consumer pins)."""

import json
from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, Optional, Union

from cjm_substrate.core.errors import CapabilityInputError


@dataclass(frozen=True)
class GraphNodeRef:
    """Locator for a node in a context graph.

    `graph_id=None` means the current graph (intra-graph reference). A non-None
    `graph_id` addresses a node in another graph — cross-graph references become
    real with provenance bundles (CR-20); the field exists now so the wire shape
    does not change when they do.
    """
    node_id: str                    # Target node id (UUID)
    graph_id: Optional[str] = None  # Owning graph id; None = the current graph

    KIND: ClassVar[str] = "graph-node"  # Locator kind discriminator

    def to_uri(self) -> str:  # Canonical URI, e.g. "graph-node:<node_id>" or "graph-node:<graph_id>/<node_id>"
        """Render the canonical URI form."""
        if self.graph_id:
            return f"{self.KIND}:{self.graph_id}/{self.node_id}"
        return f"{self.KIND}:{self.node_id}"

    def __str__(self) -> str:  # Same as `to_uri`
        return self.to_uri()

    def to_dict(self) -> Dict[str, Any]:  # Wire dict with "kind" discriminator
        """Serialize to the wire dict form."""
        return {"kind": self.KIND, "node_id": self.node_id, "graph_id": self.graph_id}


@dataclass(frozen=True)
class FileRef:
    """Locator for a filesystem artifact (e.g. a consumed run manifest).

    The path is stored raw and rendered raw — the URI form is a display/grep
    canonical string, not an RFC 3986 URI (no percent-encoding; corpus paths
    contain spaces).
    """
    path: str  # Absolute filesystem path

    KIND: ClassVar[str] = "file"  # Locator kind discriminator

    def to_uri(self) -> str:  # Canonical URI, e.g. "file:/abs/path"
        """Render the canonical URI form."""
        return f"{self.KIND}:{self.path}"

    def __str__(self) -> str:  # Same as `to_uri`
        return self.to_uri()

    def to_dict(self) -> Dict[str, Any]:  # Wire dict with "kind" discriminator
        """Serialize to the wire dict form."""
        return {"kind": self.KIND, "path": self.path}


@dataclass(frozen=True)
class UnknownLocator:
    """Lossless carrier for a locator kind this library version does not know.

    Forward-compatibility law (CR-19): consumers must round-trip locator kinds
    they cannot interpret — a shared bundle from a newer ecosystem version (or a
    future source type) keeps its references intact, and `SourceRef.content_hash`
    still verifies the content behind an un-understood locator. `data` preserves
    the original payload verbatim (minus the "kind" discriminator).

    Not hashable in practice (carries a dict); known-kind locators are the
    value-object path.
    """
    kind: str                                            # The unrecognized kind discriminator
    data: Dict[str, Any] = field(default_factory=dict)   # Original payload, verbatim (minus "kind")

    def to_uri(self) -> str:  # Best-effort canonical URI: "<kind>:<canonical-json>"
        """Render a deterministic best-effort URI form."""
        canonical = json.dumps(self.data, sort_keys=True, separators=(",", ":"))
        return f"{self.kind}:{canonical}"

    def __str__(self) -> str:  # Same as `to_uri`
        return self.to_uri()

    def to_dict(self) -> Dict[str, Any]:  # The original wire dict, reconstructed verbatim
        """Serialize back to the original wire dict form."""
        return {"kind": self.kind, **self.data}


ResourceLocator = Union[GraphNodeRef, FileRef, UnknownLocator]  # The locator sum type

LOCATOR_KINDS: Dict[str, type] = {  # Known-kind registry for wire-dict dispatch
    GraphNodeRef.KIND: GraphNodeRef,
    FileRef.KIND: FileRef,
}


def locator_from_dict(
    d: Dict[str, Any]  # Wire dict with a "kind" discriminator
) -> ResourceLocator:  # Typed locator; unknown kinds round-trip as UnknownLocator
    """Reconstruct a locator from its wire dict.

    Unknown kinds are preserved losslessly as `UnknownLocator`. Known kinds are
    strict: a payload mismatch (extra/missing fields) raises, because additive
    evolution of a known kind must land in this library, not be silently dropped.
    """
    kind = d.get("kind")
    if not kind:
        raise CapabilityInputError(
            f"Locator dict missing 'kind' discriminator: {d!r}",
            fields_invalid=["kind"],
        )
    cls = LOCATOR_KINDS.get(kind)
    payload = {k: v for k, v in d.items() if k != "kind"}
    if cls is None:
        return UnknownLocator(kind=kind, data=payload)
    try:
        return cls(**payload)
    except TypeError as e:
        raise CapabilityInputError(
            f"Malformed '{kind}' locator payload {payload!r}: {e}",
            fields_invalid=list(payload.keys()),
        ) from e
