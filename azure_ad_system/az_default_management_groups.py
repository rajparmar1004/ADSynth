import uuid
import random
from adsynth.DATABASE import NODES, EDGES, NODE_GROUPS
from adsynth.utils.parameters import get_int_param_value

def az_create_management_groups(tenant_id, subscriptions, params):
    num_mgs = get_int_param_value("AZManagementGroup", "nManagementGroups", params)
    subs_per_group = params["AZManagementGroup"].get("subscriptionsPerGroup", [1, 3])
    
    management_groups = []
    for i in range(num_mgs):
        mg_id = str(uuid.uuid4()).upper()
        NODES.append({
            "id": mg_id,
            "name": f"MG_{i+1}",
            "type": "AZManagementGroup",
            "labels": ["AZManagementGroup"],
            "objectid": mg_id,
            "tenantid": tenant_id,
            "displayName": f"Management Group {i+1}"
        })
        NODE_GROUPS["AZManagementGroup"] = NODE_GROUPS.get("AZManagementGroup", [])
        NODE_GROUPS["AZManagementGroup"].append(mg_id)
        EDGES.append({
            "source": tenant_id,
            "target": mg_id,
            "type": "AZContains",
            "labels": ["AZContains"]
        })
        
        # Link to subscriptions
        num_subs = random.randint(subs_per_group[0], min(subs_per_group[1], len(subscriptions)))
        selected_subs = random.sample(subscriptions, num_subs) if subscriptions else []
        for sub_id in selected_subs:
            EDGES.append({
                "source": mg_id,
                "target": sub_id,
                "type": "AZContains",
                "labels": ["AZContains"]
            })
        
        management_groups.append(mg_id)
    
    return management_groups