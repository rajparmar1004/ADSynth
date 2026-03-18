"""
generators/nhi_generator.py — Non-Human Identity generation
============================================================
Implements CreateNonHumans() from Algorithm 1.

Grounded entirely in paper §5 and Appendix B.  ADSynth has no equivalent.

Paper §5.5 — N_generic(t) formula:
  N_generic(t) = clamp(floor(0.14 · U_t), 6, 2500)
  Split: cloud 55% / on-prem 45%
  Cloud:   70% ServicePrincipal, 30% ManagedIdentity
  On-prem: 100% AutomationAccount

Paper §5.5 — Cross-tenant shared services pool:
  N_cross = floor(0.03 · sum_t(N_generic(t)))
  These are ServicePrincipals that span multiple tenants.

Appendix B.4 — Hygiene priors:
  ownerType:  Team 60–75%, System 15–25%, Unknown 5–15%
  lifecycle:  LongLived ~75%, Ephemeral ~25%

Appendix B.5 — Privilege bands (heavy-tailed):
  Tier-0 candidates: 0.7–1.5% of generic automation per tenant
  Tier-1:            4–8%   of generic automation per tenant
  Remainder:         scoped / standard

Appendix B.6 — Rotation tail:
  rotationCadenceDays > 365: 5–15% overall, higher under poor posture

All SyncIdentity nodes (created in Week 2) are Tier-0 by construction
per Appendix B.5 hard rule.
"""

import math
import random
from typing import Any, Dict, List

from adsynth.hybrid_system.schema_registry import (
    NodeLabel, RelType, Plane,
    VALID_OWNER_TYPES, VALID_LIFECYCLES,
)
from adsynth.hybrid_system.export_writer import add_node
from adsynth.generators.common import det_uuid, rand_uuid, weighted_choice


# 
# N_generic formula  (paper §5.5)
# 

def n_generic(u_t: int) -> int:
    """N_generic(t) = clamp(floor(0.14 · U_t), 6, 2500)"""
    return max(6, min(2500, math.floor(0.14 * u_t)))


# 
# Hygiene helpers  (Appendix B.6)
# 

def _sample_owner_type(rng: random.Random, posture: str) -> str:
    """
    ownerType distribution adjusted by tenant posture.
    Poor posture → more Unknown (governance gap realism).
    """
    if posture == "good":
        dist = {"Team": 75, "System": 20, "Unknown": 5}
    elif posture == "average":
        dist = {"Team": 65, "System": 22, "Unknown": 13}
    else:  # poor
        dist = {"Team": 55, "System": 18, "Unknown": 27}
    return weighted_choice(rng, dist)


def _sample_lifecycle(rng: random.Random) -> str:
    return weighted_choice(rng, {"LongLived": 75, "Ephemeral": 25})


def _sample_rotation_cadence(rng: random.Random, posture: str) -> int:
    """
    Appendix B.6: rotation tail 5–15% overall, higher under poor posture.
    Returns rotationCadenceDays.
    """
    if posture == "poor":
        long_tail_prob = 20
    elif posture == "average":
        long_tail_prob = 12
    else:
        long_tail_prob = 6

    if rng.randint(1, 100) <= long_tail_prob:
        return rng.randint(366, 730)  # long-tail: 1–2 years
    return rng.randint(30, 365)


def _sample_privilege_tier(rng: random.Random, posture: str) -> str:
    """
    Appendix B.5 heavy-tailed privilege bands.
    Returns "tier0", "tier1", or "standard".
    """
    roll = rng.random() * 100
    t0_ceiling = 1.5 if posture != "poor" else 2.5
    t1_ceiling = 8.0 if posture != "poor" else 10.0
    if roll < t0_ceiling:
        return "tier0"
    if roll < t1_ceiling:
        return "tier1"
    return "standard"


# 
# ServicePrincipal  (cloud, plane=Entra)
# 

def _create_service_principal(
    index: int,
    tenant: Dict[str, Any],
    run_id: str,
    rng: random.Random,
    is_cross_tenant: bool = False,
) -> str:
    posture  = tenant.get("posture", "average")
    owner    = _sample_owner_type(rng, posture)
    lc       = _sample_lifecycle(rng)
    rotation = _sample_rotation_cadence(rng, posture)
    priv     = _sample_privilege_tier(rng, posture)
    app_id   = rand_uuid(rng)
    cred_type = weighted_choice(rng, {"secret": 50, "cert": 35, "managed": 15})

    nid = det_uuid("sp", run_id, tenant["id"], str(index))
    add_node(NodeLabel.ServicePrincipal, nid, {
        "id":                  nid,
        "name":                f"SP_{tenant['name'].split('.')[0]}_{index}",
        "plane":               Plane.Entra.value,
        "runId":               run_id,
        "tenantId":            tenant["id"],
        "domainId":            None,
        # NHI required properties (Appendix B.1)
        "ownerType":           owner,
        "lifecycle":           lc,
        "appId":               app_id,
        # Hygiene
        "credentialType":      cred_type,
        "rotationCadenceDays": rotation,
        # Privilege band
        "privilegeTier":       priv,
        "highvalue":           priv == "tier0",
        # Cross-tenant flag
        "isCrossTenant":       is_cross_tenant,
    })
    return nid


# 
# ManagedIdentity  (cloud, plane=Entra)
# 

def _create_managed_identity(
    index: int,
    tenant: Dict[str, Any],
    run_id: str,
    rng: random.Random,
) -> str:
    posture  = tenant.get("posture", "average")
    owner    = _sample_owner_type(rng, posture)
    lc       = _sample_lifecycle(rng)
    rotation = _sample_rotation_cadence(rng, posture)
    priv     = _sample_privilege_tier(rng, posture)
    mi_type  = weighted_choice(rng, {"SystemAssigned": 70, "UserAssigned": 30})

    nid = det_uuid("mi", run_id, tenant["id"], str(index))
    add_node(NodeLabel.ManagedIdentity, nid, {
        "id":                  nid,
        "name":                f"MI_{tenant['name'].split('.')[0]}_{index}",
        "plane":               Plane.Entra.value,
        "runId":               run_id,
        "tenantId":            tenant["id"],
        "domainId":            None,
        "ownerType":           owner,
        "lifecycle":           lc,
        "miType":              mi_type,
        "rotationCadenceDays": rotation,
        "privilegeTier":       priv,
        "highvalue":           priv == "tier0",
    })
    return nid


# 
# AutomationAccount  (on-prem, plane=AD or Hybrid)
# 

def _create_automation_account(
    index: int,
    domain: Dict[str, Any],
    run_id: str,
    rng: random.Random,
) -> str:
    # AutomationAccounts are on-prem or hybrid — use domain posture if available
    posture  = "average"
    owner    = _sample_owner_type(rng, posture)
    lc       = _sample_lifecycle(rng)
    rotation = _sample_rotation_cadence(rng, posture)
    priv     = _sample_privilege_tier(rng, posture)
    kind     = weighted_choice(rng, {
        "service": 40, "scheduled-task": 30, "deployment": 20, "script": 10
    })

    nid = det_uuid("aa", run_id, domain["id"], str(index))
    add_node(NodeLabel.AutomationAccount, nid, {
        "id":                  nid,
        "name":                f"AA_{domain['name'].split('.')[0]}_{index}",
        "plane":               Plane.AD.value,
        "runId":               run_id,
        "tenantId":            None,
        "domainId":            domain["id"],
        "ownerType":           owner,
        "lifecycle":           lc,
        "automationKind":      kind,
        "rotationCadenceDays": rotation,
        "privilegeTier":       priv,
        "highvalue":           priv == "tier0",
    })
    return nid


# 
# Cross-tenant shared services pool  (paper §5.5)
# 

def _create_cross_tenant_pool(
    total_generic: int,
    tenants: List[Dict[str, Any]],
    run_id: str,
    rng: random.Random,
) -> List[str]:
    """
    N_cross = floor(0.03 · total_generic)
    These are ServicePrincipals not bound to a single tenant.
    We assign them to the parent tenant but mark isCrossTenant=True.
    """
    n_cross = math.floor(0.03 * total_generic)
    if not tenants or n_cross == 0:
        return []

    parent_tenant = next((t for t in tenants if t.get("orgType") == "parent"), tenants[0])
    ids = []
    for i in range(n_cross):
        nid = _create_service_principal(
            index=10000 + i,
            tenant=parent_tenant,
            run_id=run_id,
            rng=rng,
            is_cross_tenant=True,
        )
        ids.append(nid)
    return ids


# 
# Top-level entry point
# 

def create_non_humans(
    domains: List[Dict[str, Any]],
    tenants: List[Dict[str, Any]],
    users_per_tenant: Dict[str, int],
    config: Dict[str, Any],
    seed: Dict[str, Any],
    run_id: str,
) -> Dict[str, Any]:
    """
    Generate generic NonHumanIdentity nodes using the paper's balanced
    automation priors (§5.5 + Appendix B.4/B.5/B.6).

    Parameters
    ----------
    users_per_tenant : {tenant_id: U_t} — from user_generator.create_humans()

    Returns
    -------
    {
      "service_principals":  {tenant_id: [node_ids]},
      "managed_identities":  {tenant_id: [node_ids]},
      "automation_accounts": {domain_id: [node_ids]},
      "cross_tenant_pool":   [node_ids],
      "total_generic":       int,
    }
    """
    rng = random.Random(seed.get("nhSeed", seed["globalSeed"] ^ 0xFF))

    sp_by_tenant:  Dict[str, List[str]] = {}
    mi_by_tenant:  Dict[str, List[str]] = {}
    aa_by_domain:  Dict[str, List[str]] = {}
    total_generic  = 0

    for tenant in tenants:
        u_t   = users_per_tenant.get(tenant["id"], config["User"]["nUsers"])
        n_gen = n_generic(u_t)
        total_generic += n_gen

        # Cloud split: 55% cloud, 45% on-prem (on-prem assigned to domains below)
        n_cloud  = round(n_gen * 0.55)

        # Cloud: 70% SP, 30% MI
        n_sp = round(n_cloud * 0.70)
        n_mi = n_cloud - n_sp

        sp_ids: List[str] = []
        for i in range(n_sp):
            sp_ids.append(_create_service_principal(i, tenant, run_id, rng))
        sp_by_tenant[tenant["id"]] = sp_ids

        mi_ids: List[str] = []
        for i in range(n_mi):
            mi_ids.append(_create_managed_identity(i, tenant, run_id, rng))
        mi_by_tenant[tenant["id"]] = mi_ids

    # On-prem: distribute AutomationAccounts across domains
    # Total on-prem = sum of 45% per tenant, spread across domains
    n_onprem_total = round(total_generic * 0.45)
    n_per_domain   = max(1, n_onprem_total // max(1, len(domains)))

    for domain in domains:
        aa_ids: List[str] = []
        for i in range(n_per_domain):
            aa_ids.append(_create_automation_account(i, domain, run_id, rng))
        aa_by_domain[domain["id"]] = aa_ids

    # Cross-tenant pool
    cross_pool = _create_cross_tenant_pool(total_generic, tenants, run_id, rng)

    return {
        "service_principals":  sp_by_tenant,
        "managed_identities":  mi_by_tenant,
        "automation_accounts": aa_by_domain,
        "cross_tenant_pool":   cross_pool,
        "total_generic":       total_generic,
    }
