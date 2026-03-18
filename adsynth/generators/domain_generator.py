"""
generators/domain_generator.py — ADDomain node generation
==========================================================
Implements CreateDomains() and CreateTrusts() from Algorithm 1.

Per paper §6.2:
  - Instantiate ADDomain nodes (D)
  - Add DOMAIN_TRUSTS edges between domains (parameterised by trust density)

Each domain gets:
  - A deterministic id  (det_uuid based on run_id + domain index)
  - A realistic FQDN    (e.g. corp.local, corp2.local)
  - A random domain SID
  - plane = AD
"""

import random
from typing import List, Dict, Any, Tuple

from adsynth.hybrid_system.schema_registry import NodeLabel, RelType, Plane
from adsynth.hybrid_system.export_writer import add_node, add_edge
from adsynth.generators.common import (
    det_uuid, make_domain_sid, weighted_choice,
)



# Domain node creation


def create_domains(
    config: Dict[str, Any],
    seed: Dict[str, Any],
    run_id: str,
) -> List[Dict[str, Any]]:
    """
    Generate ADDomain nodes and add them to the graph.

    Returns a list of domain dicts:
      {id, name, fqdn, sid, plane}
    """
    rng = random.Random(seed["domainSeed"])

    domain_cfg = config["Domain"]
    n_domains    = domain_cfg["nDomains"]
    prefix       = domain_cfg["domainNamePrefix"]   # e.g. "corp"
    suffix       = domain_cfg["domainSuffix"]        # e.g. "local"

    domains: List[Dict[str, Any]] = []

    for i in range(n_domains):
        # FQDN: corp.local, corp2.local, corp3.local …
        fqdn = f"{prefix}.{suffix}" if i == 0 else f"{prefix}{i+1}.{suffix}"
        domain_id = det_uuid("domain", run_id, fqdn)
        sid = make_domain_sid(rng)

        props = {
            "id":       domain_id,
            "name":     fqdn,
            "plane":    Plane.AD.value,
            "runId":    run_id,
            "tenantId": None,
            "domainId": domain_id,
            "sid":      sid,
            # optional realism fields
            "fqdn":            fqdn,
            "functionalLevel": rng.choice(["2016", "2019", "2022"]),
        }

        add_node(NodeLabel.ADDomain, domain_id, props)

        domains.append({
            "id":    domain_id,
            "name":  fqdn,
            "fqdn":  fqdn,
            "sid":   sid,
            "plane": Plane.AD.value,
        })

    return domains



# Trust edge creation 

def create_trusts(
    domains: List[Dict[str, Any]],
    config: Dict[str, Any],
    seed: Dict[str, Any],
    run_id: str,
) -> List[Tuple[str, str]]:
    """
    Add DOMAIN_TRUSTS edges between domains.

    Per paper §6.2: trust density is a knob in Θ.
    With 1 domain there are no trusts.
    With N domains we add nDomainTrusts directed edges chosen randomly
    among valid (src != dst) pairs, without duplicates.

    Returns list of (src_id, dst_id) trust pairs added.
    """
    rng = random.Random(seed["domainSeed"] ^ 0xDEADBEEF)

    n_trusts = config["Domain"].get("nDomainTrusts", 0)
    if len(domains) < 2 or n_trusts == 0:
        return []

    # All valid ordered pairs (src, dst) where src != dst
    all_pairs = [
        (a["id"], b["id"])
        for a in domains
        for b in domains
        if a["id"] != b["id"]
    ]
    rng.shuffle(all_pairs)

    added: List[Tuple[str, str]] = []
    for src_id, dst_id in all_pairs[:n_trusts]:
        trust_type = rng.choice(["ParentChild", "External", "Forest"])
        add_edge(RelType.DOMAIN_TRUSTS, src_id, dst_id, properties={
            "trustType": trust_type,
            "direction": "Bidirectional" if rng.random() < 0.6 else "Outbound",
        })
        added.append((src_id, dst_id))

    return added



# Self-test
 

if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

    from adsynth.hybrid_system.export_writer import reset_graph, HYBRID_NODES, HYBRID_EDGES
    from adsynth.hybrid_system.hybrid_config import DEFAULT_HYBRID_CONFIG
    import copy

    reset_graph()
    cfg = copy.deepcopy(DEFAULT_HYBRID_CONFIG)
    cfg["Domain"]["nDomains"] = 3
    cfg["Domain"]["nDomainTrusts"] = 2
    seed = {"domainSeed": 42}

    domains = create_domains(cfg, seed, "test-run")
    trusts  = create_trusts(domains, cfg, seed, "test-run")

    print(f"Domains created : {len(domains)}")
    for d in domains:
        print(f"  {d['fqdn']}  sid={d['sid']}  id={d['id'][:8]}…")
    print(f"Trusts created  : {len(trusts)}")
    for s, d in trusts:
        print(f"  {s[:8]}… -> {d[:8]}…")
    print(f"\nGraph: {len(HYBRID_NODES)} nodes, {len(HYBRID_EDGES)} edges")
    print("domain_generator self-test: PASS")
