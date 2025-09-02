import uuid
from adsynth.DATABASE import NODES, EDGES, NODE_GROUPS
from adsynth.utils.parameters import get_int_param_value

def az_create_default_groups(tenant_id, params):
    default_groups = [ # These are the default system groups BloodHound expects
        {"name": "All Users", "is_privileged": False},
        {"name": "Global Admins", "is_privileged": True}
    ]
    groups = []
    for group in default_groups:
        group_id = str(uuid.uuid4()).upper()
        NODES.append({
            "id": group_id,
            "name": group["name"],
            "type": "AZGroup",
            "labels": ["AZGroup"],
            "objectid": group_id,
            "tenantid": tenant_id,
            "displayName": group["name"]
        })
        NODE_GROUPS["AZGroup"] = NODE_GROUPS.get("AZGroup", [])
        NODE_GROUPS["AZGroup"].append(group_id)
        EDGES.append({
            "source": tenant_id,
            "target": group_id,
            "type": "AZContains",
            "labels": ["AZContains"]
        })
        groups.append(group_id)

    return groups


def az_create_groups(tenant_id, params):
    num_groups = get_int_param_value("AZGroup", "nGroups", params)
    member_range = params["AZGroup"].get("nMembersPerGroup", [1, 10])

    groups = []
    # First, populate with default system groups
    groups = az_create_default_groups(tenant_id, params)
    # Now generate arbitrary groups
    for i in range(num_groups):
        group_id = str(uuid.uuid4()).upper()
        NODES.append({
            "id": group_id,
            "name": f"Group_{i+1}",
            "type": "AZGroup",
            "labels": ["AZGroup"],
            "objectid": group_id,
            "tenantid": tenant_id,
            "displayName": f"Group_{i+1}",
        })
        NODE_GROUPS["AZGroup"] = NODE_GROUPS.get("AZGroup", [])
        NODE_GROUPS["AZGroup"].append(group_id)
        EDGES.append({
            "source": tenant_id,
            "target": group_id,
            "type": "AZContains",
            "labels": ["AZContains"]
        })
        groups.append(group_id)
    
    return (groups)