"""
Hybrid Identity Graph — Semantic Invariant Validators
======================================================
Implements the semantic correctness checks from paper Appendix A.3.

These validators operate on the in-memory graph (HYBRID_NODES / HYBRID_EDGES)
after generation is complete, or can be called incrementally.

Invariants checked:
  1. Per-link SyncIdentity invariant  (§4.2.3 + Appendix A.3)
     For every SYNC_LINK(d, t) edge, exactly one SyncIdentity s_dt must exist:
       - SERVICES_LINK(s_dt, d)
       - SYNCS_TO(s_dt, t)
       - RUNS_ON(s_dt, host)  where host.serverRole == "EntraConnect"

  2. PHS mode invariant
     If PHS is enabled for (d, t), per-link SyncIdentity with syncMode ∈ {PHS, Mixed} exists,
     and at least one SYNCED_TO edge exists.

  3. PTA mode invariant
     If PTA is enabled for tenant t, at least one Server with serverRole=="PTAAgent" exists
     AND HAS_PTA_AGENT(t, ptaServer) exists.

  4. AD FS mode invariant
     If ADFS is enabled for (d, t), IS_FEDERATED_WITH(d, t) exists AND a Server with
     serverRole=="ADFS" exists.
"""

from typing import Any, Dict, List, Optional, Set, Tuple

from adsynth.hybrid_system.schema_registry import RelType, NodeLabel, VALID_SYNC_MODES
from adsynth.hybrid_system.export_writer import HYBRID_NODES, HYBRID_EDGES


# Internal helpers

def _nodes_by_label(label: NodeLabel) -> List[Dict[str, Any]]:
    """Return all nodes whose first label matches the given NodeLabel."""
    return [n for n in HYBRID_NODES if n["labels"] and n["labels"][0] == label.value]


def _edges_by_type(rel_type: RelType) -> List[Dict[str, Any]]:
    """Return all edges of a given RelType."""
    return [e for e in HYBRID_EDGES if e["relType"] == rel_type.value]


def _edge_exists(rel_type: RelType, start_id: str, end_id: str) -> bool:
    """Check if a specific directed edge exists."""
    for e in HYBRID_EDGES:
        if e["relType"] == rel_type.value and e["start"] == start_id and e["end"] == end_id:
            return True
    return False


def _get_node_by_id(node_id: str) -> Optional[Dict[str, Any]]:
    for n in HYBRID_NODES:
        if n["id"] == node_id:
            return n
    return None


def _nodes_with_server_role(role: str) -> List[Dict[str, Any]]:
    """Return Server nodes with a specific serverRole property value."""
    return [
        n for n in HYBRID_NODES
        if n["labels"] and n["labels"][0] == NodeLabel.Server.value
        and n["properties"].get("serverRole") == role
    ]


# Invariant 1: Per-link SyncIdentity

def check_sync_identity_invariant() -> List[str]:
    """
    For every SYNC_LINK(d, t) edge, verify:
      a) Exactly one SyncIdentity exists with linkKey = "d.id->t.id"
      b) SERVICES_LINK(sync_id, d.id) exists
      c) SYNCS_TO(sync_id, t.id) exists
      d) RUNS_ON(sync_id, h.id) for some Server h with serverRole=="EntraConnect"

    Returns list of violation strings (empty = all good).
    """
    violations = []
    entra_connect_servers: Set[str] = {
        n["id"] for n in _nodes_with_server_role("EntraConnect")
    }

    sync_links = _edges_by_type(RelType.SYNC_LINK)
    if not sync_links:
        return []  # No sync links → invariant is vacuously satisfied

    # Index SyncIdentity nodes by linkKey for efficient lookup
    sync_id_by_link_key: Dict[str, List[Dict[str, Any]]] = {}
    for node in _nodes_by_label(NodeLabel.SyncIdentity):
        lk = node["properties"].get("linkKey", "")
        sync_id_by_link_key.setdefault(lk, []).append(node)

    for sl in sync_links:
        d_id = sl["start"]
        t_id = sl["end"]
        expected_link_key = f"{d_id}->{t_id}"

        matches = sync_id_by_link_key.get(expected_link_key, [])

        # (a) Exactly one SyncIdentity per SYNC_LINK
        if len(matches) == 0:
            violations.append(
                f"SYNC_LINK({d_id}, {t_id}): no SyncIdentity with linkKey='{expected_link_key}'"
            )
            continue
        if len(matches) > 1:
            violations.append(
                f"SYNC_LINK({d_id}, {t_id}): {len(matches)} SyncIdentity nodes found "
                f"(expected exactly 1) with linkKey='{expected_link_key}'"
            )

        s = matches[0]
        s_id = s["id"]

        # (b) SERVICES_LINK(s, d)
        if not _edge_exists(RelType.SERVICES_LINK, s_id, d_id):
            violations.append(
                f"SyncIdentity '{s_id}' missing SERVICES_LINK -> domain '{d_id}'"
            )

        # (c) SYNCS_TO(s, t)
        if not _edge_exists(RelType.SYNCS_TO, s_id, t_id):
            violations.append(
                f"SyncIdentity '{s_id}' missing SYNCS_TO -> tenant '{t_id}'"
            )

        # (d) RUNS_ON(s, host) where host.serverRole == "EntraConnect"
        runs_on_targets = {
            e["end"] for e in HYBRID_EDGES
            if e["relType"] == RelType.RUNS_ON.value and e["start"] == s_id
        }
        valid_hosts = runs_on_targets & entra_connect_servers
        if not valid_hosts:
            violations.append(
                f"SyncIdentity '{s_id}' is not RUNS_ON any EntraConnect server "
                f"(available EntraConnect servers: {entra_connect_servers})"
            )

    return violations


# Invariant 2: PHS mode

def check_phs_invariant(domain_id: str, tenant_id: str) -> List[str]:
    """
    If PHS is enabled for (domain_id, tenant_id):
      a) A SyncIdentity with syncMode ∈ {PHS, Mixed} and correct linkKey must exist.
      b) At least one SYNCED_TO(user_ad, user_entra) edge must exist.

    Returns list of violation strings.
    """
    violations = []
    link_key = f"{domain_id}->{tenant_id}"

    # Find the SyncIdentity for this link
    sync_identity = None
    for node in _nodes_by_label(NodeLabel.SyncIdentity):
        if node["properties"].get("linkKey") == link_key:
            sync_identity = node
            break

    if sync_identity is None:
        return [f"PHS check: no SyncIdentity found for linkKey='{link_key}'"]

    sync_mode = sync_identity["properties"].get("syncMode", "")
    if sync_mode not in ("PHS", "Mixed"):
        violations.append(
            f"PHS mode required for ({domain_id},{tenant_id}) but SyncIdentity "
            f"'{sync_identity['id']}' has syncMode='{sync_mode}'"
        )

    # At least one SYNCED_TO edge must exist globally
    # (only enforced once User nodes exist — skipped in topology-only runs)
    synced_to_edges = _edges_by_type(RelType.SYNCED_TO)
    user_nodes = [n for n in HYBRID_NODES if n["labels"] and n["labels"][0] == NodeLabel.User.value]
    if user_nodes and not synced_to_edges:
        violations.append(
            f"PHS mode enabled for ({domain_id},{tenant_id}) but no SYNCED_TO edges exist"
        )

    return violations


# Invariant 3: PTA mode

def check_pta_invariant(tenant_id: str) -> List[str]:
    """
    If PTA is enabled for tenant_id:
      a) At least one Server with serverRole=="PTAAgent" must exist.
      b) HAS_PTA_AGENT(tenant_id, pta_server) must exist.

    Returns list of violation strings.
    """
    violations = []

    pta_servers = _nodes_with_server_role("PTAAgent")
    if not pta_servers:
        violations.append(
            f"PTA mode requires at least one Server with serverRole='PTAAgent', none found"
        )
        return violations

    pta_server_ids = {n["id"] for n in pta_servers}
    pta_agent_edges = [
        e for e in _edges_by_type(RelType.HAS_PTA_AGENT)
        if e["start"] == tenant_id and e["end"] in pta_server_ids
    ]
    if not pta_agent_edges:
        violations.append(
            f"Tenant '{tenant_id}' has no HAS_PTA_AGENT edge to any PTAAgent server "
            f"(PTAAgent servers: {pta_server_ids})"
        )

    return violations


# Invariant 4: AD FS mode

def check_adfs_invariant(domain_id: str, tenant_id: str) -> List[str]:
    """
    If AD FS is enabled for (domain_id, tenant_id):
      a) IS_FEDERATED_WITH(domain_id, tenant_id) must exist.
      b) At least one Server with serverRole=="ADFS" must exist.

    Returns list of violation strings.
    """
    violations = []

    if not _edge_exists(RelType.IS_FEDERATED_WITH, domain_id, tenant_id):
        violations.append(
            f"ADFS mode: IS_FEDERATED_WITH({domain_id}, {tenant_id}) edge is missing"
        )

    adfs_servers = _nodes_with_server_role("ADFS")
    if not adfs_servers:
        violations.append(
            "ADFS mode requires at least one Server with serverRole='ADFS', none found"
        )

    return violations


# Full graph validation — run all invariants

def validate_graph_invariants() -> Dict[str, List[str]]:
    """
    Run all semantic invariant checks on the current HYBRID_NODES / HYBRID_EDGES.

    Returns a dict mapping invariant name -> list of violation strings.
    An empty list means the invariant holds.

    Note: PHS/PTA/ADFS invariants are only triggered if the corresponding
    sync mode is actually configured.  This function inspects SyncIdentity nodes
    to determine which invariants apply.
    """
    results: Dict[str, List[str]] = {}

    # Invariant 1: always run
    results["sync_identity"] = check_sync_identity_invariant()

    # Per sync-link, check mode-specific invariants
    sync_links = _edges_by_type(RelType.SYNC_LINK)
    for sl in sync_links:
        d_id, t_id = sl["start"], sl["end"]
        link_key = f"{d_id}->{t_id}"

        # Find the SyncIdentity for this link
        sync_identity = None
        for node in _nodes_by_label(NodeLabel.SyncIdentity):
            if node["properties"].get("linkKey") == link_key:
                sync_identity = node
                break

        if sync_identity is None:
            continue  # Already flagged by invariant 1

        sync_mode = sync_identity["properties"].get("syncMode", "")

        if sync_mode in ("PHS", "Mixed"):
            key = f"phs_{d_id}_{t_id}"
            results[key] = check_phs_invariant(d_id, t_id)

        if sync_mode in ("PTA", "Mixed"):
            key = f"pta_{t_id}"
            results[key] = check_pta_invariant(t_id)

        if sync_mode in ("ADFS", "Mixed"):
            key = f"adfs_{d_id}_{t_id}"
            results[key] = check_adfs_invariant(d_id, t_id)

    return results


def print_validation_report(results: Optional[Dict[str, List[str]]] = None) -> bool:
    """
    Print a human-readable validation report.
    Returns True if all invariants pass (no violations).
    """
    if results is None:
        results = validate_graph_invariants()

    all_pass = all(len(v) == 0 for v in results.values())

    print("\n" + "=" * 60)
    print("Semantic Invariant Validation Report")
    print("=" * 60)

    if not results:
        print("  (no invariants applicable — graph may be empty)")
        return True

    for invariant_name, violations in results.items():
        status = "PASS" if not violations else "FAIL"
        print(f"  [{status}] {invariant_name}")
        for v in violations:
            print(f"         ⚠  {v}")

    print("-" * 60)
    print(f"  Result: {'ALL INVARIANTS PASS' if all_pass else 'VIOLATIONS FOUND'}")
    print("=" * 60 + "\n")

    return all_pass


if __name__ == "__main__":
    # Quick test with an empty graph
    from adsynth.hybrid_system.export_writer import reset_graph
    reset_graph()
    ok = print_validation_report()
    assert ok, "Expected empty graph to pass all invariants"
    print("invariant_validators self-test: PASS")