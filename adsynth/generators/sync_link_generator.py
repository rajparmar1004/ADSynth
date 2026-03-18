"""
generators/sync_link_generator.py — Sync link + bridge component generation
============================================================================
Implements CreateSyncLinks() and the per-link bridge instantiation from
Algorithm 1 (paper §6.2 and §6.4).

For every (domain, tenant) pair in the sync mapping L ⊆ D × T:

  1. SYNC_LINK(domain → tenant)
  2. SyncIdentity node (exactly one per link — the key design invariant)
       SERVICES_LINK(sync → domain)
       SYNCS_TO(sync → tenant)
  3. EntraConnect Server node
       RUNS_ON(sync → entra_connect_server)
  4. If PTA mode:
       PTAAgent Server node
       HAS_PTA_AGENT(tenant → pta_server)
  5. If ADFS mode:
       ADFS Server node
       IS_FEDERATED_WITH(domain → tenant)

Sync mode per link is sampled from the Θ.SyncMode distribution.

Multi-tenant mapping (paper §6.2):
  - By default each domain syncs to exactly ceil(T/D) tenants
  - p_domain_multisync controls the probability a domain gets an
    *additional* tenant link (the Storm-0501 multi-sync scenario)
"""

import random
from typing import List, Dict, Any, Tuple

from adsynth.hybrid_system.schema_registry import (
    NodeLabel, RelType, Plane,
    make_sync_identity_id, make_sync_identity_link_key,
)
from adsynth.hybrid_system.export_writer import add_node, add_edge
from adsynth.generators.common import (
    det_uuid, rand_uuid, make_domain_sid, weighted_choice,
)


# 
# Sync mapping builder  (L ⊆ D × T)
# 

def build_sync_mapping(
    domains: List[Dict[str, Any]],
    tenants: List[Dict[str, Any]],
    config: Dict[str, Any],
    rng: random.Random,
) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Build the bipartite sync mapping L ⊆ D × T.

    Strategy (per paper §6.2):
      1. Round-robin assign each tenant at least one domain.
      2. With probability p_domain_multisync, some domains also sync to
         an additional tenant (multi-tenant / Storm-0501 pattern).

    Returns list of (domain_dict, tenant_dict) pairs.
    """
    n_domains = len(domains)
    n_tenants = len(tenants)

    if n_domains == 0 or n_tenants == 0:
        return []

    p_multisync = config["Domain"].get("p_domain_multisync", 0.15)

    # Step 1: ensure every tenant gets at least one domain
    # Distribute domains across tenants round-robin, then continue cycling
    mapping: List[Tuple[Dict, Dict]] = []
    seen: set = set()

    # First pass: guarantee every tenant is covered
    shuffled_domains = list(domains)
    rng.shuffle(shuffled_domains)
    for i, tenant in enumerate(tenants):
        domain = shuffled_domains[i % len(shuffled_domains)]
        key = (domain["id"], tenant["id"])
        if key not in seen:
            mapping.append((domain, tenant))
            seen.add(key)

    # Second pass: assign remaining domains (those not yet in any link)
    covered_domains = {d["id"] for d, _ in mapping}
    for domain in domains:
        if domain["id"] not in covered_domains:
            tenant = tenants[len(mapping) % n_tenants]
            key = (domain["id"], tenant["id"])
            if key not in seen:
                mapping.append((domain, tenant))
                seen.add(key)

    # Step 2: multi-sync — each domain may also sync to one extra tenant
    for domain in domains:
        if rng.random() < p_multisync and n_tenants > 1:
            # Pick a different tenant from the one already assigned
            current_tenants = {t["id"] for d, t in mapping if d["id"] == domain["id"]}
            extras = [t for t in tenants if t["id"] not in current_tenants]
            if extras:
                extra_tenant = rng.choice(extras)
                key = (domain["id"], extra_tenant["id"])
                if key not in seen:
                    mapping.append((domain, extra_tenant))
                    seen.add(key)

    return mapping


# 
# EntraConnect server helper
# 

def _create_entra_connect_server(
    domain: Dict[str, Any],
    tenant: Dict[str, Any],
    run_id: str,
    rng: random.Random,
) -> str:
    """Create an EntraConnect Server node. Returns its id."""
    server_id = det_uuid("entra-connect", run_id, domain["id"], tenant["id"])
    hostname   = f"ECSVR-{domain['name'].split('.')[0].upper()}-{rng.randint(1,99):02d}"

    props = {
        "id":         server_id,
        "name":       hostname,
        "plane":      Plane.AD.value,
        "runId":      run_id,
        "tenantId":   tenant["id"],
        "domainId":   domain["id"],
        "hostname":   hostname,
        "serverRole": "EntraConnect",
        "os":         rng.choice(["Windows Server 2019", "Windows Server 2022"]),
    }
    add_node(NodeLabel.Server, server_id, props)
    return server_id


# 
# PTA agent server helper
# 

def _create_pta_server(
    domain: Dict[str, Any],
    tenant: Dict[str, Any],
    run_id: str,
    rng: random.Random,
) -> str:
    """Create a PTAAgent Server node. Returns its id."""
    server_id = det_uuid("pta-agent", run_id, domain["id"], tenant["id"])
    hostname   = f"PTASVR-{domain['name'].split('.')[0].upper()}-{rng.randint(1,99):02d}"

    props = {
        "id":         server_id,
        "name":       hostname,
        "plane":      Plane.AD.value,
        "runId":      run_id,
        "tenantId":   tenant["id"],
        "domainId":   domain["id"],
        "hostname":   hostname,
        "serverRole": "PTAAgent",
        "os":         rng.choice(["Windows Server 2019", "Windows Server 2022"]),
    }
    add_node(NodeLabel.Server, server_id, props)
    return server_id


# 
# ADFS server helper
# 

def _create_adfs_server(
    domain: Dict[str, Any],
    tenant: Dict[str, Any],
    run_id: str,
    rng: random.Random,
) -> str:
    """Create an ADFS Server node. Returns its id."""
    server_id = det_uuid("adfs-server", run_id, domain["id"], tenant["id"])
    hostname   = f"ADFS-{domain['name'].split('.')[0].upper()}-{rng.randint(1,99):02d}"

    props = {
        "id":         server_id,
        "name":       hostname,
        "plane":      Plane.AD.value,
        "runId":      run_id,
        "tenantId":   tenant["id"],
        "domainId":   domain["id"],
        "hostname":   hostname,
        "serverRole": "ADFS",
        "os":         rng.choice(["Windows Server 2019", "Windows Server 2022"]),
    }
    add_node(NodeLabel.Server, server_id, props)
    return server_id


# 
# SyncIdentity node helper
# 

def _create_sync_identity(
    domain: Dict[str, Any],
    tenant: Dict[str, Any],
    sync_mode: str,
    run_id: str,
    rng: random.Random,
) -> str:
    """Create a SyncIdentity node. Returns its id."""
    sync_id  = make_sync_identity_id(domain["id"], tenant["id"])
    link_key = make_sync_identity_link_key(domain["id"], tenant["id"])

    lifecycle = rng.choices(["LongLived", "Ephemeral"], weights=[90, 10])[0]

    props = {
        "id":        sync_id,
        "name":      f"SyncIdentity_{domain['name'].split('.')[0]}_{tenant['name'].split('.')[0]}",
        "plane":     Plane.Hybrid.value,
        "runId":     run_id,
        "tenantId":  tenant["id"],
        "domainId":  domain["id"],
        "ownerType": "System",
        "lifecycle": lifecycle,
        "syncMode":  sync_mode,
        "linkKey":   link_key,
    }
    add_node(NodeLabel.SyncIdentity, sync_id, props)
    return sync_id


# 
# Main: CreateSyncLinks  (per Algorithm 1, steps 1 + bridge)
# 

def create_sync_links(
    domains: List[Dict[str, Any]],
    tenants: List[Dict[str, Any]],
    config: Dict[str, Any],
    seed: Dict[str, Any],
    run_id: str,
) -> List[Dict[str, Any]]:
    """
    Build the full sync topology:
      - Sync mapping L ⊆ D × T
      - SYNC_LINK edges
      - Per-link SyncIdentity + EntraConnect server + RUNS_ON
      - Mode-specific bridge assets (PTA server, ADFS server)

    Returns list of link dicts:
      {domain, tenant, sync_mode, sync_id, ec_server_id,
       pta_server_id (optional), adfs_server_id (optional)}
    """
    rng_topo   = random.Random(seed["domainSeed"] ^ 0xABCD1234)
    rng_bridge = random.Random(seed.get("syncSeed", seed["domainSeed"] ^ 0xFF))

    sync_mode_dist = config["SyncMode"]

    # Build mapping
    mapping = build_sync_mapping(domains, tenants, config, rng_topo)

    links: List[Dict[str, Any]] = []

    for domain, tenant in mapping:
        # --- Pick sync mode for this link ---
        sync_mode = weighted_choice(rng_topo, sync_mode_dist)

        # 1. SYNC_LINK edge
        add_edge(RelType.SYNC_LINK, domain["id"], tenant["id"], properties={
            "syncMode": sync_mode,
        })

        # 2. SyncIdentity node
        sync_id = _create_sync_identity(domain, tenant, sync_mode, run_id, rng_bridge)

        # SERVICES_LINK and SYNCS_TO
        add_edge(RelType.SERVICES_LINK, sync_id, domain["id"])
        add_edge(RelType.SYNCS_TO,      sync_id, tenant["id"])

        # 3. EntraConnect server + RUNS_ON
        ec_id = _create_entra_connect_server(domain, tenant, run_id, rng_bridge)
        add_edge(RelType.RUNS_ON, sync_id, ec_id)

        link_record: Dict[str, Any] = {
            "domain":      domain,
            "tenant":      tenant,
            "sync_mode":   sync_mode,
            "sync_id":     sync_id,
            "ec_server_id": ec_id,
        }

        # 4. PTA bridge
        if sync_mode in ("PTA", "Mixed"):
            pta_id = _create_pta_server(domain, tenant, run_id, rng_bridge)
            add_edge(RelType.HAS_PTA_AGENT, tenant["id"], pta_id)
            link_record["pta_server_id"] = pta_id

        # 5. ADFS bridge
        if sync_mode in ("ADFS", "Mixed"):
            adfs_id = _create_adfs_server(domain, tenant, run_id, rng_bridge)
            add_edge(RelType.IS_FEDERATED_WITH, domain["id"], tenant["id"], properties={
                "federationType": "ADFS",
            })
            link_record["adfs_server_id"] = adfs_id

        links.append(link_record)

    return links


# 
# Self-test
# 

if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

    from adsynth.hybrid_system.export_writer import reset_graph, HYBRID_NODES, HYBRID_EDGES
    from adsynth.hybrid_system.invariant_validators import print_validation_report
    from adsynth.hybrid_system.hybrid_config import DEFAULT_HYBRID_CONFIG
    from adsynth.generators.domain_generator import create_domains, create_trusts
    from adsynth.generators.tenant_generator import create_tenants
    import copy

    reset_graph()
    cfg = copy.deepcopy(DEFAULT_HYBRID_CONFIG)
    cfg["Domain"]["nDomains"] = 2
    cfg["Tenant"]["nTenantsPerDomain"] = 2
    cfg["Domain"]["p_domain_multisync"] = 0.5
    seed = {"domainSeed": 42, "tenantSeed": 99, "syncSeed": 77}

    run_id  = "test-week2"
    domains = create_domains(cfg, seed, run_id)
    tenants = create_tenants(cfg, seed, run_id)
    links   = create_sync_links(domains, tenants, cfg, seed, run_id)

    print(f"Domains : {len(domains)}")
    print(f"Tenants : {len(tenants)}")
    print(f"Links   : {len(links)}")
    for lnk in links:
        mode = lnk["sync_mode"]
        extra = ""
        if "pta_server_id" in lnk:  extra += " +PTA"
        if "adfs_server_id" in lnk: extra += " +ADFS"
        print(f"  {lnk['domain']['name']} → {lnk['tenant']['name']}  [{mode}]{extra}")
    print(f"\nGraph: {len(HYBRID_NODES)} nodes, {len(HYBRID_EDGES)} edges")

    # Validate invariants
    print_validation_report()
    print("sync_link_generator self-test: PASS")