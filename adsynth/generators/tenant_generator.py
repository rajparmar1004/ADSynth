"""
generators/tenant_generator.py — Tenant node generation
========================================================
Implements CreateTenants() from Algorithm 1.

Per paper §6.2:
  - Instantiate Tenant nodes (T)
  - Number of tenants = nDomains × nTenantsPerDomain  (by default 1:1,
    but multi-tenant configs push this higher)
  - Each tenant is typed as parent or subsidiary for realism

Each tenant gets:
  - A deterministic id  (det_uuid based on run_id + tenant index)
  - A name like contoso.onmicrosoft.com, contoso2.onmicrosoft.com …
  - A random tenantGuid
  - An orgType:  parent (first tenant) or subsidiary (rest)
  - A posture sampled from {good, average, poor}  (§5.6 hygiene realism)
  - plane = Entra
"""

import random
from typing import List, Dict, Any

from adsynth.hybrid_system.schema_registry import NodeLabel, Plane
from adsynth.hybrid_system.export_writer import add_node
from adsynth.generators.common import det_uuid, make_tenant_guid, weighted_choice



# Tenant node creation


def create_tenants(
    config: Dict[str, Any],
    seed: Dict[str, Any],
    run_id: str,
) -> List[Dict[str, Any]]:
    """
    Generate Tenant nodes and add them to the graph.

    Total tenants = nDomains × nTenantsPerDomain.
    The first tenant is always the "parent"; the rest are "subsidiaries".

    Returns a list of tenant dicts:
      {id, name, tenantGuid, orgType, posture, plane}
    """
    rng = random.Random(seed["tenantSeed"])

    domain_cfg = config["Domain"]
    tenant_cfg = config["Tenant"]

    n_domains          = domain_cfg["nDomains"]
    n_tenants_per_dom  = tenant_cfg["nTenantsPerDomain"]
    prefix             = tenant_cfg["tenantNamePrefix"]   # e.g. "contoso"

    # Total distinct tenants in the enterprise
    # Paper default scenario: T=3 (parent + 2 subsidiaries)
    n_tenants = max(1, n_domains * n_tenants_per_dom)

    tenants: List[Dict[str, Any]] = []

    for i in range(n_tenants):
        # Name: contoso.onmicrosoft.com, contoso2.onmicrosoft.com …
        tenant_name = (
            f"{prefix}.onmicrosoft.com" if i == 0
            else f"{prefix}{i+1}.onmicrosoft.com"
        )
        tenant_id   = det_uuid("tenant", run_id, tenant_name)
        tenant_guid = make_tenant_guid(rng)

        # First tenant is parent; rest are subsidiaries
        org_type = "parent" if i == 0 else "subsidiary"

        # Security posture — influences misconfig rates later
        posture = weighted_choice(rng, {"good": 40, "average": 40, "poor": 20})

        props = {
            "id":         tenant_id,
            "name":       tenant_name,
            "plane":      Plane.Entra.value,
            "runId":      run_id,
            "tenantId":   tenant_id,
            "domainId":   None,
            "tenantGuid": tenant_guid,
            # optional realism fields
            "orgType":    org_type,
            "posture":    posture,
            "displayName": f"{prefix.capitalize()} {'Corp' if i == 0 else f'Subsidiary {i}'}",
        }

        add_node(NodeLabel.Tenant, tenant_id, props)

        tenants.append({
            "id":         tenant_id,
            "name":       tenant_name,
            "tenantGuid": tenant_guid,
            "orgType":    org_type,
            "posture":    posture,
            "plane":      Plane.Entra.value,
        })

    return tenants



# Self-test


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

    from adsynth.hybrid_system.export_writer import reset_graph, HYBRID_NODES
    from adsynth.hybrid_system.hybrid_config import DEFAULT_HYBRID_CONFIG
    import copy

    reset_graph()
    cfg = copy.deepcopy(DEFAULT_HYBRID_CONFIG)
    cfg["Domain"]["nDomains"] = 2
    cfg["Tenant"]["nTenantsPerDomain"] = 2   # → 4 tenants total
    seed = {"tenantSeed": 99}

    tenants = create_tenants(cfg, seed, "test-run")
    print(f"Tenants created: {len(tenants)}")
    for t in tenants:
        print(f"  [{t['orgType']:10}] {t['name']}  posture={t['posture']}  id={t['id'][:8]}…")
    print(f"\nGraph: {len(HYBRID_NODES)} nodes")
    print("tenant_generator self-test: PASS")
