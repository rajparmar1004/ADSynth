import uuid
from adsynth.DATABASE import NODES, EDGES, NODE_GROUPS
from adsynth.utils.parameters import get_int_param_value


def az_create_service_principals(tenant_id, params):
    num_sps = get_int_param_value("AZServicePrincipal", "nServicePrincipals", params)

    sps = []
    for i in range(num_sps):
        sp_id = str(uuid.uuid4()).upper()
        NODES.append({
            "id": sp_id,
            "name": f"SP_{i+1}",
            "type": "AZServicePrincipal",
            "labels": ["AZServicePrincipal"],
            "objectid": sp_id,
            "tenantid": tenant_id,
            "appId": str(uuid.uuid4()).upper(),
            "displayName": f"Service Principal {i+1}"
        })
        NODE_GROUPS["AZServicePrincipal"] = NODE_GROUPS.get("AZServicePrincipal", [])
        NODE_GROUPS["AZServicePrincipal"].append(sp_id)
        EDGES.append({
            "source": tenant_id,
            "target": sp_id,
            "type": "AZContains",
            "labels": ["AZContains"]
        })
        sps.append(sp_id)

    return (sps)