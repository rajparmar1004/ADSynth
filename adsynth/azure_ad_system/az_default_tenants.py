import uuid
from adsynth.DATABASE import NODES, NODE_GROUPS

def az_create_tenant(tenant_name):
    
    # Create a tenant
    tenant_id = str(uuid.uuid4()).upper() # Note: Azure uses object IDs formatted like UUID
    NODES.append({
        "id": tenant_id,
        "name": tenant_name,
        "type": "AZTenant",
        "labels": ["AZTenant"],
        "objectid": tenant_id,
        "displayName": tenant_name
    })
    NODE_GROUPS["AZTenant"] = NODE_GROUPS.get("AZTenant", [])
    NODE_GROUPS["AZTenant"].append(tenant_id)

    return (tenant_id)