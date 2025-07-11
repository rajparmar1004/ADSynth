import uuid
from adsynth.DATABASE import NODES, EDGES, NODE_GROUPS
from adsynth.utils.parameters import get_int_param_value

def az_create_subscriptions(tenant_name, tenant_id, params):
    num_subscriptions = get_int_param_value("AZSubscription", "nSubscriptions", params)
    subscriptions = []
    for i in range(num_subscriptions):
        subscription_name = f"{tenant_name}_Subscription_{i+1}"
        subscription_id = str(uuid.uuid4()).upper()
        NODES.append({
            "id": subscription_id,
            "name": subscription_name,
            "type": "AZSubscription",
            "labels": ["AZSubscription"],
            "objectid": subscription_id,
            "tenantid": tenant_id,
            "subscriptionId": str(uuid.uuid4()).upper()
        })
        NODE_GROUPS["AZSubscription"] = NODE_GROUPS.get("AZSubs cription", [])
        NODE_GROUPS["AZSubscription"].append(subscription_id)
        EDGES.append({
            "source": tenant_id,
            "target": subscription_id,
            "type": "AZContains",
            "labels": ["AZContains"]
        })
        subscriptions.append(subscription_id)
    
    return (subscriptions)