import uuid
import random
from adsynth.DATABASE import NODES, EDGES, NODE_GROUPS
from adsynth.utils.parameters import get_int_param_value

def az_create_key_vaults(tenant_id, subscriptions, params):
    num_kvs = get_int_param_value("AZKeyVault", "nKeyVaults", params)
    
    key_vaults = []
    for i in range(num_kvs):
        kv_id = str(uuid.uuid4()).upper()
        subscription_id = random.choice(subscriptions) if subscriptions else tenant_id
        NODES.append({
            "id": kv_id,
            "name": f"KeyVault_{i+1}",
            "type": "AZKeyVault",
            "labels": ["AZKeyVault"],
            "objectid": kv_id,
            "tenantid": tenant_id,
            "subscriptionId": subscription_id,
            "displayName": f"Key Vault {i+1}"
        })
        NODE_GROUPS["AZKeyVault"] = NODE_GROUPS.get("AZKeyVault", [])
        NODE_GROUPS["AZKeyVault"].append(kv_id)
        EDGES.append({
            "source": kv_id,
            "target": subscription_id,
            "type": "AZContains",
            "labels": ["AZContains"]
        })
        
        key_vaults.append(kv_id)
    
    return key_vaults