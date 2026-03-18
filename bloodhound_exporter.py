"""
bloodhound_exporter.py — Export graph to BloodHound CE OpenGraph format
========================================================================
Reads HYBRID_NODES and HYBRID_EDGES from export_writer and produces a
zip file that BloodHound Community Edition can ingest via its
"Upload Files" button.

BloodHound CE OpenGraph ingestion format (version 5):
  - One JSON file per entity type inside a zip
  - Each file:  {"data": [...], "meta": {"type": ..., "count": ..., "version": 5}}
  - Nodes use "ObjectIdentifier" as the primary key
  - Edges are embedded as arrays inside the SOURCE node record
    e.g. node["Members"] = [{"ObjectIdentifier": "...", "IsInherited": false}]

Node type mapping (your label -> BloodHound type string):
  ADDomain        -> "domains"
  Tenant          -> "aztenants"
  User  (AD)      -> "users"
  User  (Entra)   -> "azusers"
  Group           -> "groups"
  AzureADGroup    -> "azgroups"
  ServicePrincipal-> "azserviceprincipals"
  ManagedIdentity -> "azmanaged"        (custom, BH will show as generic)
  AutomationAccount -> "computers"      (closest native BH type for on-prem)
  SyncIdentity    -> "azserviceprincipals"  (closest cloud principal type)
  Computer        -> "computers"
  Server          -> "computers"

Edge type mapping (your relType -> BloodHound relationship):
  MEMBER_OF       -> embedded in source node as "Members" array
  CLOUD_MEMBER_OF -> embedded as "Members" array in AzureADGroup
  SYNCED_TO       -> "AZContains" (closest hybrid seam edge BH understands)
  DOMAIN_TRUSTS   -> embedded as "Trusts" array in domain node
  ADMIN_TO        -> embedded as "LocalAdmins" in target computer
  All others      -> written as generic edges in "rels" file

Usage:
  # After running generate_graph():
  from bloodhound_exporter import export_bloodhound
  export_bloodhound("./generated_datasets/myrun", "myrun")

  # Or run standalone (reads graph.jsonl produced by run.py):
  python bloodhound_exporter.py --jsonl ./generated_datasets/myrun/graph.jsonl
"""

import json
import os
import sys
import zipfile
import argparse
from collections import defaultdict
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Label -> BloodHound type string
# ---------------------------------------------------------------------------

def _bh_type(label: str, plane: Optional[str] = None) -> str:
    """
    Map your NodeLabel + plane to BloodHound CE entity type string.
    The type string controls which file the node goes into and how
    BloodHound renders it.
    """
    mapping = {
        "ADDomain":         "domains",
        "Tenant":           "aztenants",
        "Group":            "groups",
        "AzureADGroup":     "azgroups",
        "AzureADRole":      "azroles",
        "ServicePrincipal": "azserviceprincipals",
        "ManagedIdentity":  "azmanaged",
        "AutomationAccount":"computers",
        "SyncIdentity":     "azserviceprincipals",
        "Computer":         "computers",
        "Server":           "computers",
        "OU":               "ous",
        "GPO":              "gpos",
        "ConditionalAccessPolicy": "azconditionalaccesses",
    }

    # User is split by plane
    if label == "User":
        if plane and plane == "Entra":
            return "azusers"
        return "users"  # AD user (default)

    return mapping.get(label, "base")


# ---------------------------------------------------------------------------
# Build ObjectIdentifier from node properties
# ---------------------------------------------------------------------------

def _object_id(node: Dict[str, Any]) -> str:
    """
    BloodHound requires a stable ObjectIdentifier.
    Priority:  sid > objectid > tenantGuid > appId > id
    All returned uppercased (BloodHound convention for SIDs).
    """
    props = node["properties"]
    for key in ("sid", "objectid", "objectId", "tenantGuid", "appId"):
        val = props.get(key)
        if val and str(val).strip():
            return str(val).upper()
    return node["id"].upper()


# ---------------------------------------------------------------------------
# Build per-type node record in BloodHound format
# ---------------------------------------------------------------------------

def _build_bh_node(node: Dict[str, Any], bh_type: str) -> Dict[str, Any]:
    """
    Convert one internal node dict to BloodHound CE node record.
    BloodHound expects:
      {
        "ObjectIdentifier": "...",
        "Properties": {
          "name":        "...",
          "domain":      "...",    # required for AD objects
          "distinguishedname": "",
          "description": "",
          "enabled":     true/false,
          "highvalue":   false,
          ...
        },
        # Arrays populated later when edges are processed:
        "Members":   [],   # for groups
        "Aces":      [],
        "Trusts":    [],   # for domains
      }
    """
    props  = node["properties"]
    obj_id = _object_id(node)

    # Base properties BloodHound always wants
    bh_props: Dict[str, Any] = {
        "name":              str(props.get("name", "")).upper(),
        "domain":            str(props.get("domain", props.get("fqdn", ""))).upper(),
        "distinguishedname": props.get("distinguishedname", ""),
        "description":       props.get("description", ""),
        "enabled":           bool(props.get("enabled", True)),
        "highvalue":         bool(props.get("highvalue", False)),
        "admincount":        bool(props.get("admincount", False)),
        # Extra paper-specific properties — BH will ignore unknowns gracefully
        "plane":             props.get("plane", ""),
        "runId":             props.get("runId", ""),
    }

    # Type-specific extras
    if bh_type == "users":
        bh_props.update({
            "samaccountname":        props.get("name", "").split("@")[0],
            "userprincipalname":     props.get("upn", props.get("name", "")),
            "pwdlastset":            props.get("pwdlastset", -1),
            "lastlogon":             props.get("lastlogon", -1),
            "lastlogontimestamp":    props.get("lastlogontimestamp", -1),
            "dontreqpreauth":        bool(props.get("dontreqpreauth", False)),
            "passwordnotreqd":       bool(props.get("passwordnotreqd", False)),
            "pwdneverexpires":       bool(props.get("pwdneverexpires", False)),
            "sensitive":             bool(props.get("sensitive", False)),
            "unconstraineddelegation": bool(props.get("unconstraineddelegation", False)),
            "hasspn":                bool(props.get("hasspn", False)),
        })

    elif bh_type == "azusers":
        bh_props.update({
            "userprincipalname": props.get("upn", props.get("name", "")),
            "tenantid":          props.get("tenantId", props.get("tenantid", "")),
        })

    elif bh_type == "domains":
        bh_props.update({
            "functionallevel": props.get("functionallevel", ""),
            "sid":             props.get("sid", obj_id),
        })

    elif bh_type == "aztenants":
        bh_props.update({
            "tenantid": props.get("tenantGuid", props.get("tenantId", "")),
        })

    elif bh_type in ("groups", "azgroups"):
        bh_props.update({
            "objectid": obj_id,
        })

    elif bh_type == "azserviceprincipals":
        bh_props.update({
            "appid":             props.get("appId", ""),
            "tenantid":          props.get("tenantId", ""),
            "serviceprincipaltype": props.get("ownerType", ""),
        })

    elif bh_type == "computers":
        bh_props.update({
            "operatingsystem":   props.get("os", props.get("operatingsystem", "")),
            "unconstraineddelegation": bool(props.get("unconstraineddelegation", False)),
            "enabled":           True,
        })

    record: Dict[str, Any] = {
        "ObjectIdentifier": obj_id,
        "Properties":       bh_props,
        "Aces":             [],
        "IsDeleted":        False,
        "IsACLProtected":   False,
    }

    # Pre-allocate edge arrays relevant to each type
    if bh_type in ("groups", "azgroups"):
        record["Members"] = []
    if bh_type == "domains":
        record["Trusts"]       = []
        record["ChildObjects"] = []
        record["Links"]        = []
    if bh_type == "computers":
        record["LocalAdmins"]  = []
        record["Sessions"]     = []

    return record


# ---------------------------------------------------------------------------
# Main export function
# ---------------------------------------------------------------------------

def export_bloodhound(
    nodes: List[Dict[str, Any]],
    edges: List[Dict[str, Any]],
    output_dir: str,
    run_id: str = "export",
) -> str:
    """
    Convert HYBRID_NODES + HYBRID_EDGES into BloodHound CE zip.

    Parameters
    ----------
    nodes      : list of internal node dicts (from HYBRID_NODES)
    edges      : list of internal edge dicts (from HYBRID_EDGES)
    output_dir : directory to write the zip into
    run_id     : used as zip filename prefix

    Returns
    -------
    Full path to the written zip file.
    """
    os.makedirs(output_dir, exist_ok=True)

    # ── Step 1: Build per-type node records ──────────────────────────────────
    # bh_records[type_str] = {obj_id: record_dict}
    bh_records: Dict[str, Dict[str, Dict]] = defaultdict(dict)
    # internal_id -> obj_id  (needed for edge lookup)
    id_to_objid: Dict[str, str] = {}

    for node in nodes:
        label = node["labels"][0] if node["labels"] else "base"
        plane = node["properties"].get("plane", "")
        bh_type = _bh_type(label, plane)
        obj_id  = _object_id(node)

        id_to_objid[node["id"]] = obj_id

        if obj_id not in bh_records[bh_type]:
            bh_records[bh_type][obj_id] = _build_bh_node(node, bh_type)

    # ── Step 2: Embed edges into node records ─────────────────────────────────
    # Edges that don't embed go into a generic relationships list
    generic_rels: List[Dict[str, Any]] = []

    for edge in edges:
        src_id  = id_to_objid.get(edge["start"], edge["start"].upper())
        dst_id  = id_to_objid.get(edge["end"],   edge["end"].upper())
        rel     = edge["relType"]

        member_ref = {"ObjectIdentifier": dst_id, "IsInherited": False}

        if rel == "MEMBER_OF":
            # Find the Group record and add member
            group_rec = bh_records["groups"].get(dst_id)
            if group_rec and "Members" in group_rec:
                group_rec["Members"].append({"ObjectIdentifier": src_id, "IsInherited": False})

        elif rel == "CLOUD_MEMBER_OF":
            group_rec = bh_records["azgroups"].get(dst_id)
            if group_rec and "Members" in group_rec:
                group_rec["Members"].append({"ObjectIdentifier": src_id, "IsInherited": False})

        elif rel == "DOMAIN_TRUSTS":
            domain_rec = bh_records["domains"].get(src_id)
            if domain_rec and "Trusts" in domain_rec:
                domain_rec["Trusts"].append({
                    "TargetDomainSid":  dst_id,
                    "TargetDomainName": "",
                    "IsTransitive":     True,
                    "TrustDirection":   2,   # Bidirectional
                    "TrustType":        "ParentChild",
                    "SidFilteringEnabled": False,
                })

        elif rel == "ADMIN_TO":
            computer_rec = bh_records["computers"].get(dst_id)
            if computer_rec and "LocalAdmins" in computer_rec:
                computer_rec["LocalAdmins"].append({"ObjectIdentifier": src_id, "IsInherited": False})

        else:
            # All other edges go to generic rels file
            # BloodHound CE OpenGraph supports a "rels" type for arbitrary edges
            generic_rels.append({
                "StartNode": src_id,
                "EndNode":   dst_id,
                "RelType":   rel,
            })

    # ── Step 3: Write zip ─────────────────────────────────────────────────────
    zip_path = os.path.join(output_dir, f"{run_id}_bloodhound.zip")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:

        for bh_type, records in bh_records.items():
            if not records:
                continue
            data = list(records.values())
            payload = {
                "data": data,
                "meta": {
                    "methods": 0,
                    "type":    bh_type,
                    "count":   len(data),
                    "version": 5,
                },
            }
            filename = f"{bh_type}.json"
            zf.writestr(filename, json.dumps(payload, indent=2))
            print(f"  [BH] {filename:<35} {len(data):>5} records")

        # Generic relationships file (custom edges BloodHound may partially render)
        if generic_rels:
            payload = {
                "data": generic_rels,
                "meta": {
                    "methods": 0,
                    "type":    "rels",
                    "count":   len(generic_rels),
                    "version": 5,
                },
            }
            zf.writestr("rels.json", json.dumps(payload, indent=2))
            print(f"  [BH] {'rels.json':<35} {len(generic_rels):>5} records")

    print(f"\n  BloodHound zip written: {zip_path}")
    return zip_path


# ---------------------------------------------------------------------------
# Standalone mode: reads graph.jsonl produced by run.py
# ---------------------------------------------------------------------------

def load_jsonl(path: str):
    """Load HYBRID_NODES and HYBRID_EDGES from a graph.jsonl file."""
    nodes, edges = [], []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if rec.get("type") == "node":
                nodes.append({
                    "id":         rec["id"],
                    "labels":     rec["labels"],
                    "properties": rec["properties"],
                })
            elif rec.get("type") == "relationship":
                edges.append({
                    "start":   rec["start"],
                    "end":     rec["end"],
                    "relType": rec["relType"],
                    "properties": rec.get("properties", {}),
                })
    return nodes, edges


def main():
    parser = argparse.ArgumentParser(
        description="Export graph.jsonl to BloodHound CE OpenGraph zip"
    )
    parser.add_argument(
        "--jsonl", required=True,
        help="Path to graph.jsonl produced by run.py"
    )
    parser.add_argument(
        "--output-dir", default=".",
        help="Directory to write the BloodHound zip (default: current dir)"
    )
    parser.add_argument(
        "--run-id", default="export",
        help="Prefix for the output zip filename (default: export)"
    )
    args = parser.parse_args()

    print(f"\nLoading {args.jsonl} ...")
    nodes, edges = load_jsonl(args.jsonl)
    print(f"  Loaded {len(nodes)} nodes, {len(edges)} edges")

    print(f"\nConverting to BloodHound CE format ...")
    zip_path = export_bloodhound(nodes, edges, args.output_dir, args.run_id)

    print(f"\nDone. Upload this file to BloodHound CE:")
    print(f"  {zip_path}")
    print(f"\nInstructions:")
    print(f"  1. Open BloodHound CE in your browser (default: http://localhost:8080)")
    print(f"  2. Click 'Upload Files' in the top-right")
    print(f"  3. Select: {os.path.basename(zip_path)}")
    print(f"  4. Wait for ingestion to complete")
    print(f"  5. Use the Search bar to find nodes and explore the graph")


if __name__ == "__main__":
    main()
