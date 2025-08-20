import uuid
import random
from adsynth.DATABASE import NODES, EDGES, NODE_GROUPS
from adsynth.utils.parameters import get_int_param_value, get_perc_param_value


def az_create_default_applications(tenant_id, service_principals, params):
    default_apps = [
        # From what I could find, the Azure Portal is the only default system application for new tenants
        # Could potentially include other Microsoft applications
        {"name": "Azure Portal", "displayName": "Azure Portal"}
    ]
    apps = []
    for app in default_apps:
        app_id = str(uuid.uuid4()).upper()
        NODES.append({
            "id": app_id,
            "name": app["name"],
            "type": "AZApp",
            "labels": ["AZApp"],
            "objectid": app_id,
            "tenantid": tenant_id,
            "appId": str(uuid.uuid4()).upper(),
            "displayName": app["displayName"]
        })
        NODE_GROUPS["AZApp"] = NODE_GROUPS.get("AZApp", [])
        NODE_GROUPS["AZApp"].append(app_id)
        EDGES.append({
            "source": tenant_id,
            "target": app_id,
            "type": "AZContains",
            "labels": ["AZContains"]
        })
        if service_principals:
            sp_id = random.choice(service_principals)
            EDGES.append({
                "source": app_id,
                "target": sp_id,
                "type": "AZRunsAs",
                "labels": ["AZRunsAs"]
            })
        apps.append(app_id)
    return apps


def az_create_applications(tenant_id, service_principals, params):
    num_apps = get_int_param_value("AZApp", "nApplications", params)
    sp_assign_prob = get_perc_param_value("AZApp", "spAssignmentProbability", params)

    apps = []

    # First create default applications
    apps = az_create_default_applications(tenant_id, service_principals, params)
    # Then generate arbitrary applications
    for i in range(num_apps):
        app_id = str(uuid.uuid4()).upper()
        NODES.append({
            "id": app_id,
            "name": f"App_{i+1}",
            "type": "AZApp",
            "labels": ["AZApp"],
            "objectid": app_id,
            "tenantid": tenant_id,
            "appId": str(uuid.uuid4()).upper(),
            "displayName": f"Application {i+1}"
        })
        NODE_GROUPS["AZApp"] = NODE_GROUPS.get("AZApp", [])
        NODE_GROUPS["AZApp"].append(app_id)
        EDGES.append({
            "source": tenant_id,
            "target": app_id,
            "type": "AZContains",
            "labels": ["AZContains"]
        })
        if service_principals and random.random() * 100 < sp_assign_prob:
            sp_id = random.choice(service_principals)
            EDGES.append({
                "source": app_id,
                "target": sp_id,
                "type": "AZRunsAs",
                "labels": ["AZRunsAs"]
            })
    apps.append(app_id)

    return (apps)