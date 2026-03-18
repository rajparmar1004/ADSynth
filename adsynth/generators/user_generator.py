"""
generators/user_generator.py — Human principal generation
==========================================================
Implements CreateHumans() from Algorithm 1 (paper §6.3).

Produces:
  • AD User nodes per domain  (plane=AD)
  • Entra User nodes per tenant  (plane=Entra)
  • SYNCED_TO edges for the hybrid user mapping  (§6.3 + §4.4.2)
  • "Unsynced user" support — not all AD users get a cloud counterpart

AD user properties match ADSynth synthesizer/objects.py generate_users()
field-for-field (domain, objectid, displayname, name, enabled, pwdlastset,
lastlogon, highvalue, dontreqpreauth, hasspn, passwordnotreqd,
pwdneverexpires, sidhistory, unconstraineddelegation, admincount).

Entra user properties match ADSynth az_default_users.py az_create_users().

Per paper §5.5 NHI prior formula — N_generic(t) = clamp(floor(0.14·U_t), 6, 2500)
is used downstream by nhi_generator.py; U_t (users per tenant) is returned
so that module can compute it without a separate pass.
"""

import random
import time
from typing import Any, Dict, List, Tuple

from adsynth.hybrid_system.schema_registry import NodeLabel, RelType, Plane
from adsynth.hybrid_system.export_writer import add_node, add_edge
from adsynth.generators.common import (
    det_uuid, rand_uuid,
    make_domain_sid, make_object_sid,
    random_full_name, make_upn_ad, make_upn_entra, make_display_name,
    get_user_timestamp, weighted_choice,
)

# Starting RID for user objects (below 1000 reserved for system accounts)
_BASE_RID = 1100


# 
# AD User generation
# 

def _create_ad_user(
    index: int,
    first: str,
    last: str,
    domain: Dict[str, Any],
    run_id: str,
    rng: random.Random,
    current_time: int,
    config: Dict[str, Any],
) -> str:
    """Create one AD User node. Returns its objectid (SID)."""
    user_cfg  = config["User"]
    enabled_w = user_cfg.get("enabled", 90)

    enabled = rng.randint(1, 100) <= enabled_w
    upn     = make_upn_ad(first, last, index, domain["fqdn"])
    sid     = make_object_sid(domain["sid"], _BASE_RID + index)

    pwdlastset = get_user_timestamp(rng, current_time, enabled)
    lastlogon  = get_user_timestamp(rng, current_time, enabled)

    # ADSynth-style boolean properties
    dontreqpreauth       = rng.randint(1, 100) <= 2
    hasspn               = rng.randint(1, 100) <= 5
    passwordnotreqd      = rng.randint(1, 100) <= 2
    pwdneverexpires      = rng.randint(1, 100) <= 15
    unconstraineddelegation = rng.randint(1, 100) <= 3
    sidhistory           = ""   # populated during misconfig injection (Week 6)

    node_id = det_uuid("ad-user", run_id, sid)

    props = {
        "id":                    node_id,
        "name":                  upn,
        "plane":                 Plane.AD.value,
        "runId":                 run_id,
        "tenantId":              None,
        "domainId":              domain["id"],
        # ADSynth-compatible fields
        "objectid":              sid,
        "domain":                domain["fqdn"],
        "displayname":           make_display_name(first, last),
        "upn":                   upn,
        "enabled":               enabled,
        "pwdlastset":            pwdlastset,
        "lastlogon":             lastlogon,
        "lastlogontimestamp":    lastlogon,
        "highvalue":             False,
        "dontreqpreauth":        dontreqpreauth,
        "hasspn":                hasspn,
        "passwordnotreqd":       passwordnotreqd,
        "pwdneverexpires":       pwdneverexpires,
        "sensitive":             False,
        "serviceprincipalnames": "",
        "sidhistory":            sidhistory,
        "unconstraineddelegation": unconstraineddelegation,
        "description":           "",
        "admincount":            False,
    }
    add_node(NodeLabel.User, node_id, props)
    return node_id


def create_ad_users(
    domain: Dict[str, Any],
    config: Dict[str, Any],
    seed: Dict[str, Any],
    run_id: str,
) -> List[Dict[str, Any]]:
    """
    Generate AD User nodes for one domain.
    Returns list of {id, upn, enabled, domainId}.
    """
    rng          = random.Random(seed["userSeed"] ^ hash(domain["id"]) & 0xFFFFFFFF)
    current_time = int(time.time())
    n_users      = config["User"]["nUsers"]

    users: List[Dict[str, Any]] = []

    for i in range(1, n_users + 1):
        first, last = random_full_name(rng)
        node_id     = _create_ad_user(i, first, last, domain, run_id, rng, current_time, config)

        # Retrieve properties we stored
        upn     = make_upn_ad(first, last, i, domain["fqdn"])
        enabled = rng.randint(1, 100) <= config["User"].get("enabled", 90)  # re-sample for record

        users.append({
            "id":       node_id,
            "upn":      upn,
            "enabled":  enabled,
            "domainId": domain["id"],
            "first":    first,
            "last":     last,
        })

    return users


# 
# Entra User generation
# 

def _create_entra_user(
    first: str,
    last: str,
    tenant: Dict[str, Any],
    run_id: str,
    rng: random.Random,
    enabled: bool,
) -> str:
    """Create one Entra User node. Returns its node id."""
    upn     = make_upn_entra(first, last, tenant["name"])
    node_id = det_uuid("entra-user", run_id, upn)

    props = {
        "id":          node_id,
        "name":        upn,
        "plane":       Plane.Entra.value,
        "runId":       run_id,
        "tenantId":    tenant["id"],
        "domainId":    None,
        # Entra-specific
        "objectid":    rand_uuid(rng),
        "upn":         upn,
        "displayname": make_display_name(first, last),
        "enabled":     enabled,
        "tenantid":    tenant["id"],
    }
    add_node(NodeLabel.User, node_id, props)
    return node_id


def create_entra_default_users(
    tenant: Dict[str, Any],
    run_id: str,
    rng: random.Random,
) -> List[str]:
    """
    Create the two default Entra users that ADSynth always creates:
    Global Admin + Guest User.  Returns list of node ids.
    """
    defaults = [
        ("Global",  "Admin",  True),
        ("Guest",   "User",   False),
    ]
    ids = []
    for first, last, enabled in defaults:
        nid = _create_entra_user(first, last, tenant, run_id, rng, enabled)
        ids.append(nid)
    return ids


def create_entra_users(
    tenant: Dict[str, Any],
    config: Dict[str, Any],
    seed: Dict[str, Any],
    run_id: str,
) -> List[Dict[str, Any]]:
    """
    Generate Entra User nodes for one tenant.
    Returns list of {id, upn, enabled, tenantId}.
    """
    rng     = random.Random(seed["userSeed"] ^ hash(tenant["id"]) & 0xFFFFFFFF)
    n_users = config["User"]["nUsers"]

    users: List[Dict[str, Any]] = []

    # Always create default users first (ADSynth convention)
    for nid in create_entra_default_users(tenant, run_id, rng):
        users.append({"id": nid, "tenantId": tenant["id"], "enabled": True})

    for _ in range(n_users):
        first, last = random_full_name(rng)
        enabled     = rng.randint(1, 100) <= config["User"].get("enabled", 90)
        nid         = _create_entra_user(first, last, tenant, run_id, rng, enabled)
        upn         = make_upn_entra(first, last, tenant["name"])
        users.append({
            "id":       nid,
            "upn":      upn,
            "enabled":  enabled,
            "tenantId": tenant["id"],
            "first":    first,
            "last":     last,
        })

    return users


# 
# SYNCED_TO mapping  (paper §6.3 hybrid user mapping + §4.4.2 PHS invariant)
# 

def create_synced_to_edges(
    ad_users: List[Dict[str, Any]],
    links: List[Dict[str, Any]],
    config: Dict[str, Any],
    seed: Dict[str, Any],
    run_id: str,
) -> List[Tuple[str, str]]:
    """
    For each SYNC_LINK(d, t), create SYNCED_TO(ad_user → entra_user) edges
    for syncPercentage% of enabled AD users in domain d.

    Paper §6.3: "We explicitly allow some users to remain unsynced
    (cloud-only or on-prem-only) to avoid unrealistically synchronizing
    the entire directory."

    Returns list of (ad_user_id, entra_user_id) pairs created.
    """
    rng          = random.Random(seed["userSeed"] ^ 0xC0FFEE)
    sync_perc    = config["User"].get("syncPercentage", 80)
    synced_pairs: List[Tuple[str, str]] = []

    # Index AD users by domain id
    ad_by_domain: Dict[str, List[Dict]] = {}
    for u in ad_users:
        ad_by_domain.setdefault(u["domainId"], []).append(u)

    for link in links:
        domain = link["domain"]
        tenant = link["tenant"]
        domain_users = [u for u in ad_by_domain.get(domain["id"], []) if u.get("enabled", True)]

        if not domain_users:
            continue

        n_to_sync = max(1, int(len(domain_users) * sync_perc / 100))
        to_sync   = rng.sample(domain_users, min(n_to_sync, len(domain_users)))

        for ad_user in to_sync:
            # Create a corresponding Entra user for this specific sync link
            first = ad_user.get("first", "Sync")
            last  = ad_user.get("last",  "User")
            entra_upn = make_upn_entra(first, last, tenant["name"])
            entra_id  = det_uuid("synced-user", run_id, ad_user["id"], tenant["id"])

            # Only create if not already present
            from adsynth.hybrid_system.export_writer import get_node_by_id
            if get_node_by_id(entra_id) is None:
                props = {
                    "id":          entra_id,
                    "name":        entra_upn,
                    "plane":       Plane.Entra.value,
                    "runId":       run_id,
                    "tenantId":    tenant["id"],
                    "domainId":    None,
                    "objectid":    rand_uuid(rng),
                    "upn":         entra_upn,
                    "displayname": make_display_name(first, last),
                    "enabled":     True,
                    "tenantid":    tenant["id"],
                }
                add_node(NodeLabel.User, entra_id, props)

            # SYNCED_TO edge — validate=False because User→User is allowed
            # but only for AD→Entra direction; we enforce semantics via
            # the plane property rather than schema endpoint check
            add_edge(RelType.SYNCED_TO, ad_user["id"], entra_id, validate=False)
            synced_pairs.append((ad_user["id"], entra_id))

    return synced_pairs


# 
# Top-level entry point
# 

def create_humans(
    domains: List[Dict[str, Any]],
    tenants: List[Dict[str, Any]],
    links: List[Dict[str, Any]],
    config: Dict[str, Any],
    seed: Dict[str, Any],
    run_id: str,
) -> Dict[str, Any]:
    """
    Generate all human principals and hybrid user mappings.

    Returns:
      {
        "ad_users":     [list of AD user dicts],
        "entra_users":  {tenant_id: [list of Entra user dicts]},
        "synced_pairs": [(ad_id, entra_id), ...],
        "users_per_tenant": {tenant_id: count},   # for NHI prior formula
      }
    """
    all_ad_users: List[Dict[str, Any]] = []
    all_entra_users: Dict[str, List[Dict[str, Any]]] = {}

    # AD users — one batch per domain
    for domain in domains:
        domain_users = create_ad_users(domain, config, seed, run_id)
        all_ad_users.extend(domain_users)

    # Entra users — one batch per tenant
    for tenant in tenants:
        tenant_users = create_entra_users(tenant, config, seed, run_id)
        all_entra_users[tenant["id"]] = tenant_users

    # SYNCED_TO hybrid mapping (activates PHS invariant)
    synced_pairs = create_synced_to_edges(all_ad_users, links, config, seed, run_id)

    # U_t per tenant — needed by NHI prior formula N_generic(t)
    users_per_tenant = {
        t_id: len(users) for t_id, users in all_entra_users.items()
    }

    return {
        "ad_users":        all_ad_users,
        "entra_users":     all_entra_users,
        "synced_pairs":    synced_pairs,
        "users_per_tenant": users_per_tenant,
    }
