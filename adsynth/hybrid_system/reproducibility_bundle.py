"""
Hybrid Identity Graph — Reproducibility Bundle Writer
=====================================================
Writes the reproducibility bundle described in the paper (§6 + CLI spec).

Bundle contents (written to <run_dir>/):
  graph.jsonl         — nodes + edges (via export_writer)
  graph_stats.json    — summary statistics
  config.json         — the configuration Θ used for this run
  seed.json           — the seed vector s used for this run
  manifest.json       — run metadata (schemaVersion, timestamp, hashes)

The bundle is designed so that any run can be fully reproduced given only
config.json + seed.json, plus the same codebase version.
"""

import hashlib
import json
import os
import time
from typing import Any, Dict, Optional

from adsynth.hybrid_system.schema_registry import SCHEMA_VERSION
from adsynth.hybrid_system.export_writer import (
    HYBRID_NODES,
    HYBRID_EDGES,
    write_graph_jsonl,
    write_graph_stats,
)


# Helpers

def _sha256_file(path: str) -> str:
    """Return the SHA-256 hex digest of a file's contents."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_json(path: str, data: Any) -> None:
    """Write a Python object as pretty-printed JSON."""
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)


# Bundle writer

def write_reproducibility_bundle(
    run_id: str,
    run_dir: str,
    config: Dict[str, Any],
    seed: Dict[str, Any],
) -> Dict[str, str]:
    """
    Write the complete reproducibility bundle for a run.

    Parameters
    ----------
    run_id   : Unique identifier for this run (e.g. "run-2026-03-01-001")
    run_dir  : Directory to write the bundle into (created if absent)
    config   : The configuration Θ dict used to generate the graph
    seed     : The seed vector s (dict with at minimum {"globalSeed": int})

    Returns
    -------
    Dict mapping bundle file names to their full paths.
    """
    os.makedirs(run_dir, exist_ok=True)

    paths: Dict[str, str] = {}

    # 1. Write graph (nodes + edges)
    graph_path = write_graph_jsonl(run_dir, "graph.jsonl")
    paths["graph.jsonl"] = graph_path

    # 2. Write graph stats
    stats_path = write_graph_stats(run_dir, "graph_stats.json")
    paths["graph_stats.json"] = stats_path

    # 3. Write config Θ
    config_path = os.path.join(run_dir, "config.json")
    _write_json(config_path, config)
    paths["config.json"] = config_path

    # 4. Write seed vector s
    seed_path = os.path.join(run_dir, "seed.json")
    _write_json(seed_path, seed)
    paths["seed.json"] = seed_path

    # 5. Compute file hashes and write manifest
    file_hashes = {
        name: _sha256_file(path)
        for name, path in paths.items()
    }

    manifest = {
        "runId": run_id,
        "schemaVersion": SCHEMA_VERSION,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "totalNodes": len(HYBRID_NODES),
        "totalEdges": len(HYBRID_EDGES),
        "files": file_hashes,
    }
    manifest_path = os.path.join(run_dir, "manifest.json")
    _write_json(manifest_path, manifest)
    paths["manifest.json"] = manifest_path

    return paths


def print_bundle_summary(paths: Dict[str, str], run_id: str) -> None:
    """Print a human-readable summary of the written bundle."""
    print(f"\n{'='*60}")
    print(f"Reproducibility Bundle Written  (runId: {run_id})")
    print(f"{'='*60}")
    for name, path in paths.items():
        size = os.path.getsize(path)
        print(f"  {name:<22} {size:>8} bytes  →  {path}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    import uuid
    import tempfile
    from adsynth.hybrid_system.export_writer import reset_graph, add_node, add_edge
    from adsynth.hybrid_system.schema_registry import NodeLabel, RelType, Plane
    from adsynth.hybrid_system.hybrid_config import DEFAULT_HYBRID_CONFIG

    reset_graph()
    run_id = f"test-{uuid.uuid4().hex[:8]}"

    # Minimal graph for test
    d_id = str(uuid.uuid4())
    t_id = str(uuid.uuid4())
    add_node(NodeLabel.ADDomain, d_id, {
        "id": d_id, "name": "test.local", "plane": Plane.AD.value,
        "runId": run_id, "tenantId": None, "domainId": d_id,
        "sid": "S-1-5-21-111-222-333",
    })
    add_node(NodeLabel.Tenant, t_id, {
        "id": t_id, "name": "test.onmicrosoft.com", "plane": Plane.Entra.value,
        "runId": run_id, "tenantId": t_id, "domainId": None,
        "tenantGuid": t_id,
    })
    add_edge(RelType.SYNC_LINK, d_id, t_id)

    seed = {"globalSeed": 42, "domainSeed": 100, "nhi_seed": 200}

    with tempfile.TemporaryDirectory() as tmpdir:
        run_dir = os.path.join(tmpdir, run_id)
        paths = write_reproducibility_bundle(run_id, run_dir, DEFAULT_HYBRID_CONFIG, seed)
        print_bundle_summary(paths, run_id)

        # Verify manifest exists and is parseable
        with open(paths["manifest.json"]) as f:
            manifest = json.load(f)
        print(f"Manifest: {json.dumps(manifest, indent=2)}")

    print("reproducibility_bundle self-test: PASS")
