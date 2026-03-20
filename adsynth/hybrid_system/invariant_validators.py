"""
adsynth/hybrid_system/invariant_validators.py
=============================================
Semantic invariant validators — updated to read from the original
DATABASE.py data store (NODES, EDGES) instead of the parallel
export_writer.py (HYBRID_NODES, HYBRID_EDGES).

Logic is identical to the original invariant_validators.py.
Only the data source references changed.

Invariants:
  I1 — Per-link SyncIdentity: every SYNC_LINK has exactly one SyncIdentity
       with SERVICES_LINK, SYNCS_TO, and RUNS_ON(EntraConnect)
  I2 — PTA: every PTA-mode tenant has a PTAAgent server + HAS_PTA_AGENT edge
  I3 — ADFS: every ADFS-mode link has IS_FEDERATED_WITH + ADFS server
  I4 — PHS: SyncIdentity with syncMode PHS/Mixed exists + SYNCED_TO edges present
"""

from typing import Any, Dict, List, Optional

from adsynth.DATABASE import (
    NODES, EDGES,
    SYNC_LINKS, SYNC_IDENTITY_NODES,
    TENANT_HYBRID_MODE,
    PTA_AGENT_NODES, ADFS_SERVER_NODES,
)


# ============================================================
# Internal helpers — read from NODES / EDGES
# ============================================================

def _nodes_by_label(label: str) -> List[Dict[str, Any]]:
    return [n for n in NODES if n["labels"] and n["labels"][-1] == label]


def _edges_by_type(rel_type: str) -> List[Dict[str, Any]]:
    return [e for e in EDGES if e.get("label") == rel_type]


def _edge_exists(rel_type: str, start_id: str, end_id: str) -> bool:
    for e in EDGES:
        if (e.get("label") == rel_type
                and e.get("start", {}).get("id") == start_id
                and e.get("end", {}).get("id") == end_id):
            return True
    return False


def _get_node_by_objectid(objectid: str) -> Optional[Dict[str, Any]]:
    for n in NODES:
        if n["properties"].get("objectid") == objectid:
            return n
    return None


def _get_node_by_index(idx: int) -> Optional[Dict[str, Any]]:
    if 0 <= idx < len(NODES):
        return NODES[idx]
    return None


def _neo4j_id_of(node: Dict[str, Any]) -> str:
    """Return the Neo4j internal id string used in edge start/end."""
    return node.get("id", "")


# ============================================================
# Invariant I1 — Per-link SyncIdentity
# ============================================================

def check_sync_identity_invariant() -> List[str]:
    """
    For every SYNC_LINK (domain_name, tenant_id) in SYNC_LINKS:
      a) Exactly one SyncIdentity with linkKey = "domain_id->tenant_id" exists
      b) SERVICES_LINK(sync, domain) exists
      c) SYNCS_TO(sync, tenant) exists
      d) RUNS_ON(sync, host) where host has serverRole=EntraConnect
    """
    violations = []

    if not SYNC_LINKS:
        return []

    # Build set of EntraConnect server neo4j ids
    ec_neo4j_ids = set()
    for n in NODES:
        if (n["labels"] and n["labels"][-1] == "ConnectorHost"
                and n["properties"].get("serverRole") == "EntraConnect"):
            ec_neo4j_ids.add(_neo4j_id_of(n))

    for domain_name, tenant_id in SYNC_LINKS:
        # Get node indices from tracking dict
        sync_idx = SYNC_IDENTITY_NODES.get((domain_name, tenant_id))

        # (a) SyncIdentity must exist
        if sync_idx is None:
            violations.append(
                f"SYNC_LINK({domain_name}, {tenant_id}): "
                f"no SyncIdentity node in SYNC_IDENTITY_NODES"
            )
            continue

        sync_node = _get_node_by_index(sync_idx)
        if sync_node is None:
            violations.append(
                f"SYNC_LINK({domain_name}, {tenant_id}): "
                f"SyncIdentity index {sync_idx} not found in NODES"
            )
            continue

        sync_neo4j_id = _neo4j_id_of(sync_node)

        # Find domain node neo4j id
        domain_node = None
        for n in NODES:
            if (n["labels"] and n["labels"][-1] == "Domain"
                    and n["properties"].get("name", "").upper() == domain_name.upper()):
                domain_node = n
                break

        # Find tenant node neo4j id
        tenant_node = _get_node_by_objectid(tenant_id)

        # (b) SERVICES_LINK(sync -> domain)
        if domain_node:
            if not _edge_exists("SERVICES_LINK", sync_neo4j_id,
                                _neo4j_id_of(domain_node)):
                violations.append(
                    f"SyncIdentity for ({domain_name},{tenant_id}) "
                    f"missing SERVICES_LINK -> domain"
                )
        else:
            violations.append(
                f"SyncIdentity for ({domain_name},{tenant_id}): "
                f"domain node '{domain_name}' not found in NODES"
            )

        # (c) SYNCS_TO(sync -> tenant)
        if tenant_node:
            if not _edge_exists("SYNCS_TO", sync_neo4j_id,
                                _neo4j_id_of(tenant_node)):
                violations.append(
                    f"SyncIdentity for ({domain_name},{tenant_id}) "
                    f"missing SYNCS_TO -> tenant"
                )
        else:
            violations.append(
                f"SyncIdentity for ({domain_name},{tenant_id}): "
                f"tenant node '{tenant_id}' not found in NODES"
            )

        # (d) RUNS_ON(sync -> EntraConnect host)
        runs_on_targets = {
            e.get("end", {}).get("id")
            for e in EDGES
            if e.get("label") == "RUNS_ON"
            and e.get("start", {}).get("id") == sync_neo4j_id
        }
        valid_hosts = runs_on_targets & ec_neo4j_ids
        if not valid_hosts:
            violations.append(
                f"SyncIdentity for ({domain_name},{tenant_id}) "
                f"not RUNS_ON any EntraConnect server"
            )

    return violations


# ============================================================
# Invariant I2 — PTA mode
# ============================================================

def check_pta_invariant() -> List[str]:
    """
    For every tenant with TENANT_HYBRID_MODE in (PTA, Mixed):
      a) At least one PTAAgentHost node exists
      b) HAS_PTA_AGENT(tenant -> pta_host) edge exists
    """
    violations = []

    pta_tenants = [
        t_id for t_id, mode in TENANT_HYBRID_MODE.items()
        if mode in ("PTA", "Mixed")
    ]

    for tenant_id in pta_tenants:
        pta_indices = PTA_AGENT_NODES.get(tenant_id, [])

        if not pta_indices:
            violations.append(
                f"PTA tenant '{tenant_id}': "
                f"no PTAAgentHost nodes in PTA_AGENT_NODES"
            )
            continue

        # Check HAS_PTA_AGENT edge exists
        tenant_node = _get_node_by_objectid(tenant_id)
        if not tenant_node:
            violations.append(
                f"PTA tenant '{tenant_id}': tenant node not found in NODES"
            )
            continue

        tenant_neo4j_id = _neo4j_id_of(tenant_node)
        pta_neo4j_ids = {
            _neo4j_id_of(NODES[i]) for i in pta_indices
            if 0 <= i < len(NODES)
        }

        has_edge = any(
            e.get("label") == "HAS_PTA_AGENT"
            and e.get("start", {}).get("id") == tenant_neo4j_id
            and e.get("end", {}).get("id") in pta_neo4j_ids
            for e in EDGES
        )

        if not has_edge:
            violations.append(
                f"PTA tenant '{tenant_id}': "
                f"HAS_PTA_AGENT edge to PTAAgentHost missing"
            )

    return violations


# ============================================================
# Invariant I3 — ADFS mode
# ============================================================

def check_adfs_invariant() -> List[str]:
    """
    For every link with TENANT_HYBRID_MODE in (ADFS, Mixed):
      a) IS_FEDERATED_WITH(domain -> tenant) edge exists
      b) At least one ADFSServer node exists
    """
    violations = []

    adfs_tenants = [
        t_id for t_id, mode in TENANT_HYBRID_MODE.items()
        if mode in ("ADFS", "Mixed")
    ]

    adfs_server_nodes = _nodes_by_label("ADFSServer")

    for tenant_id in adfs_tenants:
        if not ADFS_SERVER_NODES.get(tenant_id):
            violations.append(
                f"ADFS tenant '{tenant_id}': "
                f"no ADFSServer nodes in ADFS_SERVER_NODES"
            )

        if not adfs_server_nodes:
            violations.append(
                "ADFS mode: no ADFSServer nodes found in NODES"
            )

        # IS_FEDERATED_WITH edge
        fed_edges = _edges_by_type("IS_FEDERATED_WITH")
        tenant_fed = [
            e for e in fed_edges
            if e.get("end", {}).get("id") == _neo4j_id_of(
                _get_node_by_objectid(tenant_id) or {}
            )
        ]
        if not tenant_fed:
            violations.append(
                f"ADFS tenant '{tenant_id}': "
                f"IS_FEDERATED_WITH edge missing"
            )

    return violations


# ============================================================
# Invariant I4 — PHS mode
# ============================================================

def check_phs_invariant() -> List[str]:
    """
    For every link with TENANT_HYBRID_MODE in (PHS, Mixed):
      a) SyncIdentity with syncMode PHS or Mixed exists for that link
      b) At least one SYNCED_TO edge exists globally
    """
    violations = []

    phs_links = [
        (d, t) for (d, t) in SYNC_LINKS
        if TENANT_HYBRID_MODE.get(t) in ("PHS", "Mixed")
    ]

    if not phs_links:
        return []

    for domain_name, tenant_id in phs_links:
        sync_idx = SYNC_IDENTITY_NODES.get((domain_name, tenant_id))
        if sync_idx is None:
            violations.append(
                f"PHS link ({domain_name},{tenant_id}): "
                f"SyncIdentity not found"
            )
            continue

        sync_node = _get_node_by_index(sync_idx)
        if not sync_node:
            continue

        mode = sync_node["properties"].get("syncMode", "")
        if mode not in ("PHS", "Mixed"):
            violations.append(
                f"PHS link ({domain_name},{tenant_id}): "
                f"SyncIdentity has syncMode='{mode}', expected PHS or Mixed"
            )

    # At least one SYNCED_TO edge must exist if users were generated
    user_nodes = _nodes_by_label("User")
    synced_to = _edges_by_type("SYNCED_TO")
    if user_nodes and not synced_to:
        violations.append(
            "PHS mode active but no SYNCED_TO edges found — "
            "check user sync generation"
        )

    return violations


# ============================================================
# Full validation — run all invariants
# ============================================================

def validate_graph_invariants() -> Dict[str, List[str]]:
    """
    Run all invariant checks on current NODES / EDGES.
    Returns {invariant_name: [violation_strings]}.
    Empty list means the invariant holds.
    """
    results = {}

    results["I1_sync_identity"] = check_sync_identity_invariant()
    results["I2_pta_mode"] = check_pta_invariant()
    results["I3_adfs_mode"] = check_adfs_invariant()
    results["I4_phs_mode"] = check_phs_invariant()

    return results


def print_validation_report(results: Optional[Dict[str, List[str]]] = None) -> bool:
    """
    Print a human-readable validation report.
    Returns True if all invariants pass.
    """
    if results is None:
        results = validate_graph_invariants()

    all_pass = all(len(v) == 0 for v in results.values())

    print("\n" + "=" * 60)
    print("Semantic Invariant Validation Report")
    print("=" * 60)

    if not results:
        print("  (no invariants applicable)")
        return True

    for name, violations in results.items():
        status = "PASS" if not violations else "FAIL"
        print(f"  [{status}] {name}")
        for v in violations:
            print(f"         ⚠  {v}")

    print("-" * 60)
    print(f"  Result: {'ALL INVARIANTS PASS' if all_pass else 'VIOLATIONS FOUND'}")
    print("=" * 60 + "\n")

    return all_pass
