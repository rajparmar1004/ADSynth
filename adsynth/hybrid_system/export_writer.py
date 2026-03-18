"""

Implements the newline-delimited JSON export format described in paper Appendix A.4.

Node record format:
  {"type": "node", "id": "...", "labels": [...], "properties": {...}}

Relationship record format:
  {"type": "relationship", "start": "...", "end": "...", "relType": "...", "properties": {...}}

Output goes to <run_dir>/graph.jsonl (one record per line).
The writer also validates nodes/edges against the schema registry before writing.
"""

import json
import os
from typing import Any, Dict, List, Optional

from adsynth.hybrid_system.schema_registry import (
    NodeLabel, RelType, SCHEMA_VERSION,
    validate_node, is_allowed_edge,
)


# In-memory graph store (mirrors adsynth DATABASE.py pattern)

# List of node dicts:  {id, labels, properties}
HYBRID_NODES: List[Dict[str, Any]] = []

# List of edge dicts:  {start, end, relType, properties}
HYBRID_EDGES: List[Dict[str, Any]] = []

# Quick-lookup: node id -> index in HYBRID_NODES
_NODE_INDEX: Dict[str, int] = {}


# Graph mutation helpers

def add_node(label: NodeLabel,
             node_id: str,
             properties: Dict[str, Any],
             extra_labels: Optional[List[str]] = None,
             validate: bool = True) -> int:
    """
    Add a node to HYBRID_NODES.

    Parameters
    ----------
    label        : primary NodeLabel for this node
    node_id      : stable unique identifier (e.g. UUID or SID-derived string)
    properties   : node property dict (must include global + label-required fields)
    extra_labels : additional labels to attach (e.g. ["Base"])
    validate     : if True, run schema validation and raise on errors

    Returns
    -------
    Index of the node in HYBRID_NODES
    """
    if node_id in _NODE_INDEX:
        return _NODE_INDEX[node_id]   # idempotent — don't double-add

    if validate:
        errors = validate_node(label, properties)
        if errors:
            raise ValueError(
                f"Node '{node_id}' ({label.value}) failed schema validation:\n  "
                + "\n  ".join(errors)
            )

    labels = [label.value]
    if extra_labels:
        labels.extend(extra_labels)

    node = {
        "id": node_id,
        "labels": labels,
        "properties": dict(properties),
    }
    idx = len(HYBRID_NODES)
    HYBRID_NODES.append(node)
    _NODE_INDEX[node_id] = idx
    return idx


def add_edge(rel_type: RelType,
             start_id: str,
             end_id: str,
             properties: Optional[Dict[str, Any]] = None,
             validate: bool = True) -> None:
    """
    Add a directed relationship to HYBRID_EDGES.

    Parameters
    ----------
    rel_type   : RelType enum value
    start_id   : source node id (must already exist in HYBRID_NODES)
    end_id     : target node id (must already exist in HYBRID_NODES)
    properties : optional edge properties
    validate   : if True, check endpoint constraints via schema registry
    """
    if start_id not in _NODE_INDEX:
        raise KeyError(f"Source node '{start_id}' not in graph")
    if end_id not in _NODE_INDEX:
        raise KeyError(f"Target node '{end_id}' not in graph")

    if validate:
        src_node = HYBRID_NODES[_NODE_INDEX[start_id]]
        dst_node = HYBRID_NODES[_NODE_INDEX[end_id]]

        # Use the first label on each node for endpoint validation
        src_label = NodeLabel(src_node["labels"][0])
        dst_label = NodeLabel(dst_node["labels"][0])

        if not is_allowed_edge(rel_type, src_label, dst_label):
            raise ValueError(
                f"Schema violation: ({src_label.value}) -[{rel_type.value}]-> ({dst_label.value}) "
                f"is not a valid endpoint combination"
            )

    edge = {
        "start": start_id,
        "end": end_id,
        "relType": rel_type.value,
        "properties": dict(properties or {}),
    }
    HYBRID_EDGES.append(edge)


def get_node_by_id(node_id: str) -> Optional[Dict[str, Any]]:
    """Return the node dict for a given id, or None if not found."""
    if node_id in _NODE_INDEX:
        return HYBRID_NODES[_NODE_INDEX[node_id]]
    return None


def reset_graph() -> None:
    """Clear all nodes and edges (useful for tests / multiple runs)."""
    HYBRID_NODES.clear()
    HYBRID_EDGES.clear()
    _NODE_INDEX.clear()


# JSON-lines export

def _node_record(node: Dict[str, Any]) -> Dict[str, Any]:
    """Convert internal node dict to paper Appendix A.4 format."""
    return {
        "type": "node",
        "id": node["id"],
        "labels": node["labels"],
        "properties": node["properties"],
    }


def _edge_record(edge: Dict[str, Any]) -> Dict[str, Any]:
    """Convert internal edge dict to paper Appendix A.4 format."""
    return {
        "type": "relationship",
        "start": edge["start"],
        "end": edge["end"],
        "relType": edge["relType"],
        "properties": edge["properties"],
    }


def write_graph_jsonl(output_dir: str,
                      filename: str = "graph.jsonl") -> str:
    """
    Write all nodes then all edges as newline-delimited JSON to
    <output_dir>/<filename>.

    Returns the full path to the written file.
    """
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    with open(output_path, "w", encoding="utf-8") as fh:
        for node in HYBRID_NODES:
            fh.write(json.dumps(_node_record(node), separators=(",", ":")) + "\n")
        for edge in HYBRID_EDGES:
            fh.write(json.dumps(_edge_record(edge), separators=(",", ":")) + "\n")

    return output_path


def write_graph_stats(output_dir: str,
                      filename: str = "graph_stats.json") -> str:
    """
    Write a brief statistics file alongside the graph.
    Useful for quick sanity-checking the Week 1 deliverable.
    """
    # Count nodes by label
    label_counts: Dict[str, int] = {}
    for node in HYBRID_NODES:
        primary = node["labels"][0] if node["labels"] else "Unknown"
        label_counts[primary] = label_counts.get(primary, 0) + 1

    # Count edges by relType
    rel_counts: Dict[str, int] = {}
    for edge in HYBRID_EDGES:
        rt = edge["relType"]
        rel_counts[rt] = rel_counts.get(rt, 0) + 1

    stats = {
        "schemaVersion": SCHEMA_VERSION,
        "total_nodes": len(HYBRID_NODES),
        "total_edges": len(HYBRID_EDGES),
        "nodes_by_label": label_counts,
        "edges_by_relType": rel_counts,
    }

    os.makedirs(output_dir, exist_ok=True)
    stats_path = os.path.join(output_dir, filename)
    with open(stats_path, "w", encoding="utf-8") as fh:
        json.dump(stats, fh, indent=2)

    return stats_path


# Quick self-test

if __name__ == "__main__":
    import uuid
    from adsynth.hybrid_system.schema_registry import Plane

    print("export_writer self-test...")
    reset_graph()

    run_id = "test-run-001"

    # Add an ADDomain node
    domain_id = str(uuid.uuid4())
    add_node(NodeLabel.ADDomain, domain_id, {
        "id": domain_id,
        "name": "corp.local",
        "plane": Plane.AD.value,
        "runId": run_id,
        "tenantId": None,
        "domainId": domain_id,
        "sid": "S-1-5-21-123456789-1234567890-123456789",
    })

    # Add a Tenant node
    tenant_id = str(uuid.uuid4())
    add_node(NodeLabel.Tenant, tenant_id, {
        "id": tenant_id,
        "name": "corp.onmicrosoft.com",
        "plane": Plane.Entra.value,
        "runId": run_id,
        "tenantId": tenant_id,
        "domainId": None,
        "tenantGuid": tenant_id,
    })

    # Add SYNC_LINK edge
    add_edge(RelType.SYNC_LINK, domain_id, tenant_id)

    # Attempt a bad edge (should fail)
    try:
        add_edge(RelType.SYNC_LINK, tenant_id, domain_id)  # reversed - invalid
        print("  [FAIL] Should have raised ValueError")
    except ValueError as e:
        print(f"  [PASS] Correctly rejected reversed SYNC_LINK: {e}")

    # Write output
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        path = write_graph_jsonl(tmpdir)
        stats_path = write_graph_stats(tmpdir)
        with open(path) as f:
            lines = f.readlines()
        print(f"\n  Wrote {len(lines)} lines to {path}")
        for line in lines:
            print(f"    {line.rstrip()}")
        with open(stats_path) as f:
            print(f"\n  Stats:\n{f.read()}")

    print("\nexport_writer self-test complete.")
