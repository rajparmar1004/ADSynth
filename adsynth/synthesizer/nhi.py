"""
adsynth/synthesizer/nhi.py
===========================
Non-Human Identity generation.
Ported from adsynth/generators/nhi_generator.py but writes
through the original DATABASE.py node_operation / edge_operation.

Paper §5 — N_generic(t) formula:
  N_generic(t) = clamp(floor(0.14 · U_t), 6, 2500)
  Split: cloud 55% / on-prem 45%
  Cloud:   70% ServicePrincipal, 30% ManagedIdentity
  On-prem: 100% AutomationAccount

Paper §5.5 — Cross-tenant shared services pool:
  N_cross = floor(0.03 · sum_t(N_generic(t)))

Appendix B.4 — Hygiene priors:
  ownerType:  Team 60-75%, System 15-25%, Unknown 5-15%
  lifecycle:  LongLived ~75%, Ephemeral ~25%

Appendix B.5 — Privilege bands (heavy-tailed):
  Tier-0: 0.7-1.5% of generic automation per tenant
  Tier-1: 4-8%
  Remainder: standard/scoped

All SyncIdentity nodes are Tier-0 by construction (set in hybrid_seam.py).
"""

import math
import random
import uuid
from typing import Any, Dict, List

from adsynth.DATABASE import (
    NODES, NODE_GROUPS,
    DATABASE_ID, RUN_ID,
    NHI_NODE_INDICES,
    TENANT_METADATA,
    node_operation, edge_operation, get_node_index,
    ridcount,
)


# ============================================================
# N_generic formula (paper §5.5)
# ============================================================

def n_generic(u_t: int) -> int:
    """N_generic(t) = clamp(floor(0.14 * U_t), 6, 2500)"""
    return max(6, min(2500, math.floor(0.14 * u_t)))


# ============================================================
# Hygiene sampling helpers (Appendix B.4 + B.6)
# ============================================================

def _sample_owner_type(rng: random.Random, posture: str) -> str:
    if posture == "good":
        dist = {"Team": 75, "System": 20, "Unknown": 5}
    elif posture == "average":
        dist = {"Team": 65, "System": 22, "Unknown": 13}
    else:  # poor
        dist = {"Team": 55, "System": 18, "Unknown": 27}
    keys = list(dist.keys())
    weights = [dist[k] for k in keys]
    return rng.choices(keys, weights=weights, k=1)[0]


def _sample_lifecycle(rng: random.Random) -> str:
    return rng.choices(["LongLived", "Ephemeral"], weights=[75, 25], k=1)[0]


def _sample_rotation_cadence(rng: random.Random, posture: str) -> int:
    """Appendix B.6 rotation tail."""
    long_tail_prob = {"poor": 20, "average": 12, "good": 6}.get(posture, 12)
    if rng.randint(1, 100) <= long_tail_prob:
        return rng.randint(366, 730)
    return rng.randint(30, 365)


def _sample_privilege_tier(rng: random.Random, posture: str) -> str:
    """Appendix B.5 heavy-tailed privilege bands."""
    roll = rng.random() * 100
    t0_ceil = 1.5 if posture != "poor" else 2.5
    t1_ceil = 8.0 if posture != "poor" else 10.0
    if roll < t0_ceil:
        return "tier0"
    if roll < t1_ceil:
        return "tier1"
    return "standard"


# ============================================================
# ServicePrincipal (cloud, plane=Entra)
# ============================================================

def _create_service_principal(
    index: int,
    tenant_id: str,
    tenant_name: str,
    rng: random.Random,
    is_cross_tenant: bool = False,
) -> int:
    """Create one ServicePrincipal node. Returns node index."""
    posture = TENANT_METADATA.get(tenant_id, {}).get("posture", "average")

    sp_objectid = str(uuid.uuid4()).upper()
    short_name = tenant_name.split(".")[0]
    display_name = f"SP_{short_name}_{index}"

    keys = [
        "labels", "name", "objectid", "plane", "runId",
        "tenantId", "domainId",
        # NHI required
        "ownerType", "lifecycle",
        # SP required
        "appId",
        # Hygiene
        "credentialType", "rotationCadenceDays",
        # Privilege
        "privilegeTier", "highvalue",
        # Cross-tenant flag
        "isCrossTenant",
    ]
    values = [
        "AZServicePrincipal", display_name, sp_objectid, "Entra", RUN_ID,
        tenant_id, None,
        _sample_owner_type(rng, posture),
        _sample_lifecycle(rng),
        str(uuid.uuid4()).upper(),
        rng.choices(["secret", "cert", "managed"], weights=[50, 35, 15], k=1)[0],
        _sample_rotation_cadence(rng, posture),
        _sample_privilege_tier(rng, posture),
        False,  # highvalue set below
        is_cross_tenant,
    ]

    idx = node_operation("AZServicePrincipal", keys, values, sp_objectid)

    # Set highvalue based on privilege tier
    priv = NODES[idx]["properties"]["privilegeTier"]
    NODES[idx]["properties"]["highvalue"] = (priv == "tier0")

    NHI_NODE_INDICES.append(idx)

    # Contain in tenant
    tenant_idx = get_node_index(tenant_id, "objectid")
    if tenant_idx != -1:
        edge_operation(tenant_idx, idx, "AZContains", ["isacl"], [False])

    return idx


# ============================================================
# ManagedIdentity (cloud, plane=Entra)
# ============================================================

def _create_managed_identity(
    index: int,
    tenant_id: str,
    tenant_name: str,
    rng: random.Random,
) -> int:
    """Create one ManagedIdentity node. Returns node index."""
    posture = TENANT_METADATA.get(tenant_id, {}).get("posture", "average")

    mi_objectid = str(uuid.uuid4()).upper()
    short_name = tenant_name.split(".")[0]
    display_name = f"MI_{short_name}_{index}"

    mi_type = rng.choices(
        ["SystemAssigned", "UserAssigned"], weights=[70, 30], k=1
    )[0]

    keys = [
        "labels", "name", "objectid", "plane", "runId",
        "tenantId", "domainId",
        "ownerType", "lifecycle",
        "miType",
        "rotationCadenceDays", "privilegeTier", "highvalue",
    ]
    values = [
        "ManagedIdentity", display_name, mi_objectid, "Entra", RUN_ID,
        tenant_id, None,
        _sample_owner_type(rng, posture),
        _sample_lifecycle(rng),
        mi_type,
        _sample_rotation_cadence(rng, posture),
        _sample_privilege_tier(rng, posture),
        False,
    ]

    idx = node_operation("ManagedIdentity", keys, values, mi_objectid)
    NODES[idx]["properties"]["highvalue"] = (
        NODES[idx]["properties"]["privilegeTier"] == "tier0"
    )
    NHI_NODE_INDICES.append(idx)

    tenant_idx = get_node_index(tenant_id, "objectid")
    if tenant_idx != -1:
        edge_operation(tenant_idx, idx, "AZContains", ["isacl"], [False])

    return idx


# ============================================================
# AutomationAccount (on-prem, plane=AD)
# ============================================================

def _create_automation_account(
    index: int,
    domain_name: str,
    domain_sid: str,
    rng: random.Random,
) -> int:
    """Create one AutomationAccount node. Returns node index."""
    rid = ridcount[0]
    ridcount[0] += 1
    aa_sid = f"{domain_sid}-{rid}"

    short_name = domain_name.split(".")[0]
    display_name = f"AA_{short_name}_{index}@{domain_name}"

    kind = rng.choices(
        ["service", "scheduled-task", "deployment", "script"],
        weights=[40, 30, 20, 10], k=1
    )[0]

    keys = [
        "labels", "name", "objectid", "plane", "runId",
        "tenantId", "domainId",
        "domain",
        "ownerType", "lifecycle",
        "automationKind",
        "rotationCadenceDays", "privilegeTier", "highvalue",
    ]
    values = [
        "AutomationAccount", display_name, aa_sid, "AD", RUN_ID,
        None, domain_name,
        domain_name,
        _sample_owner_type(rng, "average"),
        _sample_lifecycle(rng),
        kind,
        _sample_rotation_cadence(rng, "average"),
        _sample_privilege_tier(rng, "average"),
        False,
    ]

    idx = node_operation("AutomationAccount", keys, values, aa_sid)
    NODES[idx]["properties"]["highvalue"] = (
        NODES[idx]["properties"]["privilegeTier"] == "tier0"
    )
    NHI_NODE_INDICES.append(idx)

    # Place in domain
    domain_idx = get_node_index(domain_name + "_Domain", "name")
    if domain_idx != -1:
        edge_operation(domain_idx, idx, "Contains", ["isacl"], [False])

    return idx


# ============================================================
# Cross-tenant shared services pool (paper §5.5)
# ============================================================

def _create_cross_tenant_pool(
    total_generic: int,
    tenants: List[Dict[str, Any]],
    rng: random.Random,
) -> List[int]:
    """
    N_cross = floor(0.03 * total_generic)
    ServicePrincipals marked isCrossTenant=True, assigned to parent tenant.
    """
    n_cross = math.floor(0.03 * total_generic)
    if not tenants or n_cross == 0:
        return []

    parent = next(
        (t for t in tenants
         if TENANT_METADATA.get(t["id"], {}).get("orgType") == "parent"),
        tenants[0]
    )

    ids = []
    for i in range(n_cross):
        idx = _create_service_principal(
            index=10000 + i,
            tenant_id=parent["id"],
            tenant_name=parent["name"],
            rng=rng,
            is_cross_tenant=True,
        )
        ids.append(idx)

    return ids


# ============================================================
# Main entry point: create_non_humans
# ============================================================

def create_non_humans(
    domains: List[Dict[str, Any]],
    tenants: List[Dict[str, Any]],
    users_per_tenant: Dict[str, int],
    config: Dict[str, Any],
    seed: int,
) -> Dict[str, Any]:
    """
    Generate all NonHumanIdentity nodes using paper priors.

    Parameters
    ----------
    domains           : list of {name, sid, id} dicts
    tenants           : list of {id, name} dicts
    users_per_tenant  : {tenant_id: U_t} from user generation
    config            : full parameters dict
    seed              : integer seed

    Returns
    -------
    {
      "sp_by_tenant":  {tenant_id: [node_indices]},
      "mi_by_tenant":  {tenant_id: [node_indices]},
      "aa_by_domain":  {domain_name: [node_indices]},
      "cross_pool":    [node_indices],
      "total_generic": int,
    }
    """
    rng = random.Random(seed ^ 0xFF00AA)

    sp_by_tenant: Dict[str, List[int]] = {}
    mi_by_tenant: Dict[str, List[int]] = {}
    aa_by_domain: Dict[str, List[int]] = {}
    total_generic = 0

    for tenant in tenants:
        t_id = tenant["id"]
        u_t = users_per_tenant.get(t_id, config.get("AZUser", {}).get("nUsers", 50))
        n_gen = n_generic(u_t)
        total_generic += n_gen

        # Cloud: 55% of n_gen
        n_cloud = round(n_gen * 0.55)
        n_sp = round(n_cloud * 0.70)
        n_mi = n_cloud - n_sp

        sp_ids = []
        for i in range(n_sp):
            sp_ids.append(
                _create_service_principal(i, t_id, tenant["name"], rng)
            )
        sp_by_tenant[t_id] = sp_ids

        mi_ids = []
        for i in range(n_mi):
            mi_ids.append(
                _create_managed_identity(i, t_id, tenant["name"], rng)
            )
        mi_by_tenant[t_id] = mi_ids

    # On-prem: 45% distributed across domains
    n_onprem_total = round(total_generic * 0.45)
    n_per_domain = max(1, n_onprem_total // max(1, len(domains)))

    for domain in domains:
        aa_ids = []
        for i in range(n_per_domain):
            aa_ids.append(
                _create_automation_account(
                    i, domain["name"], domain["sid"], rng
                )
            )
        aa_by_domain[domain["name"]] = aa_ids

    # Cross-tenant pool
    cross_pool = _create_cross_tenant_pool(total_generic, tenants, rng)

    return {
        "sp_by_tenant":  sp_by_tenant,
        "mi_by_tenant":  mi_by_tenant,
        "aa_by_domain":  aa_by_domain,
        "cross_pool":    cross_pool,
        "total_generic": total_generic,
    }
