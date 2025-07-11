import uuid
import random
from adsynth.DATABASE import NODES, EDGES, NODE_GROUPS
from adsynth.utils.parameters import get_int_param_value, get_perc_param_value

def az_create_vms(tenant_id, subscriptions, params):
    num_vms = get_int_param_value("AZVM", "nVMs", params)
    
    vms = []
    for i in range(num_vms):
        vm_id = str(uuid.uuid4()).upper()
        subscription_id = random.choice(subscriptions) if subscriptions else tenant_id

        NODES.append({
            "id": vm_id,
            "name": f"VM_{i+1}",
            "type": "AZVM",
            "labels": ["AZVM"],
            "objectid": vm_id,
            "tenantid": tenant_id,
            "subscriptionId": subscription_id,
            "displayName": f"Virtual Machine {i+1}",
        })
        NODE_GROUPS["AZVM"] = NODE_GROUPS.get("AZVM", [])
        NODE_GROUPS["AZVM"].append(vm_id)
        EDGES.append({
            "source": subscription_id,
            "target": vm_id,
            "type": "AZContains",
            "labels": ["AZContains"]
        })
        
        vms.append(vm_id)
    
    return vms