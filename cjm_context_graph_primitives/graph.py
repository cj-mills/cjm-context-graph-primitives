"""The graph data nouns - GraphNode / GraphEdge / GraphContext. Moved here from cjm-graph-plugin-system per the data-nouns-vs-storage-verbs split (pass-2 Thread 2): every consumer of graph DATA (workflow cores, bundles, the CR-18 graph-aware layer, the storage adapter itself) depends on this library; only persistence depends on the storage adapter. GraphContext satisfies the substrate's FileBackedDTO protocol (to_temp_file) for zero-copy worker transfer. All three nouns are wire-registered (stage 4): graph-storage adapter methods return them typed across the worker boundary."""

import json
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from cjm_context_graph_primitives.provenance import SourceRef
from cjm_substrate.core.wire import wire_type


@wire_type("graph.node")
@dataclass
class GraphNode:
    """An entity in a context graph.

    `sources` carries the node's provenance references (multiple refs per node:
    e.g. a fine Segment carries an audio ref and a text ref — the slice kind
    selects the facet).
    """
    id: str            # UUID
    label: str         # e.g. "Source", "Segment", "Correction"
    properties: Dict[str, Any] = field(default_factory=dict)  # Arbitrary domain payload
    sources: List[SourceRef] = field(default_factory=list)    # Provenance references
    created_at: Optional[float] = None  # Unix timestamp when created
    updated_at: Optional[float] = None  # Unix timestamp when last updated

    def to_dict(self) -> Dict[str, Any]:  # Wire dict with nested source dicts
        """Serialize to the wire dict form."""
        return {
            "id": self.id,
            "label": self.label,
            "properties": self.properties,
            "sources": [s.to_dict() for s in self.sources],
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any]  # Wire dict (nested source dicts or SourceRef instances)
    ) -> "GraphNode":  # Reconstructed node
        """Reconstruct from the wire dict form (single authority — storage adapters
        and `GraphContext` both route through here rather than re-implementing)."""
        sources = []
        for s in data.get("sources", []):
            sources.append(SourceRef.from_dict(s) if isinstance(s, dict) else s)
        return cls(
            id=data["id"],
            label=data["label"],
            properties=data.get("properties", {}),
            sources=sources,
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )


@wire_type("graph.edge")
@dataclass
class GraphEdge:
    """A relationship between two nodes. Composition is ALWAYS edges — grouping,
    chains, supersession, and provenance topology all live here, never in
    multi-range slices or per-ref chain fields."""
    id: str             # UUID
    source_id: str      # Origin node UUID
    target_id: str      # Destination node UUID
    relation_type: str  # e.g. "NEXT", "PART_OF", "CORRECTS", "DERIVED_FROM"
    properties: Dict[str, Any] = field(default_factory=dict)  # Arbitrary metadata
    created_at: Optional[float] = None  # Unix timestamp when created
    updated_at: Optional[float] = None  # Unix timestamp when last updated

    def to_dict(self) -> Dict[str, Any]:  # Wire dict
        """Serialize to the wire dict form."""
        return {
            "id": self.id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "relation_type": self.relation_type,
            "properties": self.properties,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any]  # Wire dict
    ) -> "GraphEdge":  # Reconstructed edge
        """Reconstruct from the wire dict form."""
        return cls(
            id=data["id"],
            source_id=data["source_id"],
            target_id=data["target_id"],
            relation_type=data["relation_type"],
            properties=data.get("properties", {}),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )


@wire_type("graph.context")
@dataclass
class GraphContext:
    """Container for graph read results (a subgraph).

    Satisfies the substrate's `FileBackedDTO` protocol via `to_temp_file` for
    zero-copy transfer across the worker boundary.
    """
    nodes: List[GraphNode]  # Nodes in the subgraph
    edges: List[GraphEdge]  # Edges in the subgraph
    metadata: Dict[str, Any] = field(default_factory=dict)  # Query metadata, stats, etc.

    def to_dict(self) -> Dict[str, Any]:  # Wire dict
        """Serialize to the wire dict form."""
        return {
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "metadata": self.metadata,
        }

    def to_temp_file(self) -> str:  # Absolute path to a temporary JSON file
        """Save to a temp file for zero-copy transfer (FileBackedDTO)."""
        tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        json.dump(self.to_dict(), tmp)
        tmp.close()
        return str(Path(tmp.name).absolute())

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any]  # Wire dict with nodes, edges, metadata
    ) -> "GraphContext":  # Reconstructed context
        """Reconstruct from the wire dict form."""
        return cls(
            nodes=[GraphNode.from_dict(n) for n in data.get("nodes", [])],
            edges=[GraphEdge.from_dict(e) for e in data.get("edges", [])],
            metadata=data.get("metadata", {}),
        )

    @classmethod
    def from_file(
        cls,
        filepath: str  # Path to a JSON file produced by `to_temp_file`
    ) -> "GraphContext":  # Reconstructed context
        """Load from a JSON file."""
        with open(filepath, "r") as f:
            return cls.from_dict(json.load(f))
