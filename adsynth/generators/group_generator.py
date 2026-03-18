"""
generators/group_generator.py — Group principal generation
===========================================================
Implements the group portion of CreateHumans() (paper §6.3).

Produces:
  • AD Group nodes per domain  (plane=AD)   with MEMBER_OF edges
  • Entra AzureADGroup nodes per tenant     with CLOUD_MEMBER_OF edges
  • Default groups: "Domain Users" (AD), "All Users" / "Global Admins" (Entra)
    — matching ADSynth az_default_groups.py convention

Paper §4.3.1 membership edges:
  MEMBER_OF:       (User|Group|NonHumanIdentity) -> Group
  CLOUD_MEMBER_OF: (User|AzureADGroup|NonHumanIdentity) -> AzureADGroup
"""

import random
from typing import Any, Dict, List

from adsynth.hybrid_system.schema_registry import NodeLabel, RelType, Plane
from adsynth.hybrid_system.export_writer import add_node, add_edge
from adsynth.generators.common import det_uuid, rand_uuid, weighted_choice

# Group name building blocks (kept domain-agnostic per paper design)
_DEPARTMENTS = ["IT", "HR", "Finance", "Security", "Dev", "Ops",
                "Legal", "Compliance", "Platform", "Data"]
_ACCESS_RIGHTS = ["Read", "Modify", "Write", "FullAccess"]


# 
# AD Group generation
# 

def _make_ad_group_name(dept: str, right: str, index: int, domain_fqdn: str) -> str:
    """Mirrors ADSynth group naming: T{tier}_{dept}_Folder{n}_{right}@{domain}
    Simplified (no tier) for Week 3: {dept}_Resource{index}_{right}@{domain}"""
    return f"{dept}_Resource{index}_{right}@{domain_fqdn}".upper()


def create_ad_groups(
    domain: Dict[str, Any],
    config: Dict[str, Any],
    seed: Dict[str, Any],
    run_id: str,
) -> List[Dict[str, Any]]:
    """
    Generate AD security groups for one domain.
    Also creates the mandatory 'DOMAIN USERS' group that all AD users join.
    Returns list of {id, name, domainId, is_default}.
    """
    rng      = random.Random(seed["groupSeed"] ^ hash(domain["id"]) & 0xFFFFFFFF)
    n_groups = config["Group"]["nADGroups"]
    fqdn     = domain["fqdn"]

    groups: List[Dict[str, Any]] = []

    # --- Default group: DOMAIN USERS (ADSynth always creates this) ---
    du_name = f"DOMAIN USERS@{fqdn}".upper()
    du_id   = det_uuid("ad-group", run_id, du_name)
    sid     = f"{domain['sid']}-513"   # well-known RID for Domain Users

    add_node(NodeLabel.Group, du_id, {
        "id":       du_id,
        "name":     du_name,
        "plane":    Plane.AD.value,
        "runId":    run_id,
        "tenantId": None,
        "domainId": domain["id"],
        "objectid": sid,
        "sid":      sid,
        "domain":   fqdn,
        "highvalue": False,
        "admincount": False,
    })
    groups.append({"id": du_id, "name": du_name, "domainId": domain["id"], "is_default": True})

    # --- Regular security groups ---
    for i in range(n_groups):
        dept  = rng.choice(_DEPARTMENTS)
        right = rng.choice(_ACCESS_RIGHTS)
        name  = _make_ad_group_name(dept, right, i, fqdn)
        gid   = det_uuid("ad-group", run_id, name)
        gsid  = f"{domain['sid']}-{2000 + i}"

        add_node(NodeLabel.Group, gid, {
            "id":       gid,
            "name":     name,
            "plane":    Plane.AD.value,
            "runId":    run_id,
            "tenantId": None,
            "domainId": domain["id"],
            "objectid": gsid,
            "sid":      gsid,
            "domain":   fqdn,
            "highvalue":  False,
            "admincount": False,
        })
        groups.append({"id": gid, "name": name, "domainId": domain["id"], "is_default": False})

    return groups


def assign_ad_group_memberships(
    ad_users: List[Dict[str, Any]],
    ad_groups: List[Dict[str, Any]],
    config: Dict[str, Any],
    seed: Dict[str, Any],
) -> int:
    """
    Add MEMBER_OF edges from AD users to AD groups.

    Every enabled user joins 'DOMAIN USERS' (mandatory).
    Each user also joins 1–3 random security groups.
    Returns count of edges added.
    """
    rng = random.Random(seed["groupSeed"] ^ 0xABCD)
    n_groups_per_user_min = config["Group"].get("nGroupsPerUserMin", 1)
    n_groups_per_user_max = config["Group"].get("nGroupsPerUserMax", 3)
    edges_added = 0

    # Index groups by domain
    groups_by_domain: Dict[str, List[Dict]] = {}
    default_by_domain: Dict[str, str] = {}
    for g in ad_groups:
        groups_by_domain.setdefault(g["domainId"], []).append(g)
        if g.get("is_default"):
            default_by_domain[g["domainId"]] = g["id"]

    for user in ad_users:
        d_id = user["domainId"]
        domain_groups = [g for g in groups_by_domain.get(d_id, []) if not g.get("is_default")]

        # Mandatory: DOMAIN USERS
        du_id = default_by_domain.get(d_id)
        if du_id:
            add_edge(RelType.MEMBER_OF, user["id"], du_id)
            edges_added += 1

        # Random security groups
        if domain_groups:
            n = rng.randint(n_groups_per_user_min, min(n_groups_per_user_max, len(domain_groups)))
            for g in rng.sample(domain_groups, n):
                add_edge(RelType.MEMBER_OF, user["id"], g["id"])
                edges_added += 1

    return edges_added


# 
# Entra (AzureADGroup) generation
# 

def create_entra_groups(
    tenant: Dict[str, Any],
    config: Dict[str, Any],
    seed: Dict[str, Any],
    run_id: str,
) -> List[Dict[str, Any]]:
    """
    Generate Entra cloud groups for one tenant.
    Always creates default groups: 'All Users' and 'Global Admins'
    (matching ADSynth az_default_groups.py convention).
    Returns list of {id, name, tenantId, is_privileged, is_default}.
    """
    rng      = random.Random(seed["groupSeed"] ^ hash(tenant["id"]) & 0xFFFFFFFF)
    n_groups = config["Group"]["nEntraGroups"]
    groups: List[Dict[str, Any]] = []

    # --- Default groups (ADSynth always creates these) ---
    defaults = [
        ("All Users",     False),
        ("Global Admins", True),
    ]
    for name, is_priv in defaults:
        gid = det_uuid("entra-group", run_id, tenant["id"], name)
        add_node(NodeLabel.AzureADGroup, gid, {
            "id":           gid,
            "name":         name,
            "plane":        Plane.Entra.value,
            "runId":        run_id,
            "tenantId":     tenant["id"],
            "domainId":     None,
            "objectId":     rand_uuid(rng),
            "displayName":  name,
            "isPrivileged": is_priv,
        })
        groups.append({
            "id": gid, "name": name, "tenantId": tenant["id"],
            "is_privileged": is_priv, "is_default": True,
        })

    # --- Regular cloud groups ---
    for i in range(n_groups):
        name = f"Group_{i + 1}"
        gid  = det_uuid("entra-group", run_id, tenant["id"], name)
        add_node(NodeLabel.AzureADGroup, gid, {
            "id":           gid,
            "name":         name,
            "plane":        Plane.Entra.value,
            "runId":        run_id,
            "tenantId":     tenant["id"],
            "domainId":     None,
            "objectId":     rand_uuid(rng),
            "displayName":  name,
            "isPrivileged": False,
        })
        groups.append({
            "id": gid, "name": name, "tenantId": tenant["id"],
            "is_privileged": False, "is_default": False,
        })

    return groups


def assign_entra_group_memberships(
    entra_users: Dict[str, List[Dict[str, Any]]],
    entra_groups: Dict[str, List[Dict[str, Any]]],
    config: Dict[str, Any],
    seed: Dict[str, Any],
) -> int:
    """
    Add CLOUD_MEMBER_OF edges from Entra users to Entra groups.
    Each user joins 1–2 random groups per tenant.
    Returns count of edges added.
    """
    rng = random.Random(seed["groupSeed"] ^ 0xDEAD)
    edges_added = 0

    for tenant_id, users in entra_users.items():
        groups = [g for g in entra_groups.get(tenant_id, []) if not g.get("is_default")]
        if not groups:
            continue
        for user in users:
            n = rng.randint(1, min(2, len(groups)))
            for g in rng.sample(groups, n):
                add_edge(RelType.CLOUD_MEMBER_OF, user["id"], g["id"])
                edges_added += 1

    return edges_added


# 
# Top-level entry point
# 

def create_groups(
    domains: List[Dict[str, Any]],
    tenants: List[Dict[str, Any]],
    ad_users: List[Dict[str, Any]],
    entra_users: Dict[str, List[Dict[str, Any]]],
    config: Dict[str, Any],
    seed: Dict[str, Any],
    run_id: str,
) -> Dict[str, Any]:
    """
    Generate all group principals and assign memberships.

    Returns:
      {
        "ad_groups":    {domain_id: [group dicts]},
        "entra_groups": {tenant_id: [group dicts]},
      }
    """
    all_ad_groups: Dict[str, List[Dict]] = {}
    all_entra_groups: Dict[str, List[Dict]] = {}
    flat_ad_groups: List[Dict] = []

    for domain in domains:
        grps = create_ad_groups(domain, config, seed, run_id)
        all_ad_groups[domain["id"]] = grps
        flat_ad_groups.extend(grps)

    for tenant in tenants:
        grps = create_entra_groups(tenant, config, seed, run_id)
        all_entra_groups[tenant["id"]] = grps

    # Membership edges
    assign_ad_group_memberships(ad_users, flat_ad_groups, config, seed)
    assign_entra_group_memberships(entra_users, all_entra_groups, config, seed)

    return {
        "ad_groups":    all_ad_groups,
        "entra_groups": all_entra_groups,
    }
