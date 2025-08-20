import uuid
from adsynth.DATABASE import NODES, EDGES, NODE_GROUPS
from adsynth.utils.parameters import get_int_param_value

def az_create_roles(tenant_id, params):
    num_roles = get_int_param_value("AZRole", "nRoles", params)
    role_names = params["AZRole"].get("defaultRoles", ["Global Administrator", "Contributor", "Reader"])
    roles = []

    for i in range(num_roles):
        name = role_names[i % len(role_names)]  # Cycle through the default roles if needed
        role_id = str(uuid.uuid4()).upper()
        NODES.append({
            "id": role_id,
            "name": name,
            "type": "AZRole",
            "labels": ["AZRole"],
            "objectid": role_id,
            "tenantid": tenant_id,
            "roleTemplateId": str(uuid.uuid4()).upper(),
            "displayName": name
        })
        NODE_GROUPS["AZRole"] = NODE_GROUPS.get("AZRole", [])
        NODE_GROUPS["AZRole"].append(role_id)
        EDGES.append({
            "source": tenant_id,
            "target": role_id,
            "type": "AZContains",
            "labels": ["AZContains"]
        })
        roles.append(role_id)
    
    return (roles)