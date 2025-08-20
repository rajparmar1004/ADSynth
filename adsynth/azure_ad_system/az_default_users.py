import uuid
import random
from adsynth.DATABASE import NODES, EDGES, NODE_GROUPS
from adsynth.utils.parameters import get_int_param_value, get_perc_param_value

def az_create_default_users(tenant_name, tenant_id, roles, params):
    """
    Create default Azure AD users (Global Admin, Guest User) and assign
    Global Administrator role to Global Admin user.
    """
    default_users = [
        {"name": "Global Admin", "upn": f"admin@{tenant_name.lower()}", "is_admin": True},
        {"name": "Guest User", "upn": f"guest@{tenant_name.lower()}", "is_admin": False}
    ]
    users = []
    
    # Find Global Administrator role ID
    global_admin_role = next((r for r in roles if any(n["name"] == "Global Administrator" and n["id"] == r for n in NODES)), None)
    
    for user in default_users:
        user_id = str(uuid.uuid4()).upper()
        NODES.append({
            "id": user_id,
            "name": user["name"],
            "userPrincipalName": user["upn"],
            "type": "AZUser",
            "labels": ["AZUser"],
            "objectid": user_id,
            "tenantid": tenant_id,
            "enabled": True,
            "displayName": user["name"],
            "isAdmin": user["is_admin"]
        })
        NODE_GROUPS["AZUser"] = NODE_GROUPS.get("AZUser", [])
        NODE_GROUPS["AZUser"].append(user_id)
        EDGES.append({
            "source": user_id,
            "target": tenant_id,
            "type": "AZContains",
            "labels": ["AZContains"]
        })
        
        # Assign Global Administrator role to Global Admin user
        if user["is_admin"] and global_admin_role:
            EDGES.append({
                "source": user_id,
                "target": global_admin_role,
                "type": "AZHasRole",
                "labels": ["AZHasRole"],
                "scope": tenant_id
            })
        
        users.append(user_id)
    
    return users


def az_create_users(tenant_name, tenant_id, roles, first_names, last_names, params):
    num_users = get_int_param_value("AZUser", "nUsers", params)
    enabled_perc = get_perc_param_value("AZUser", "enabled", params)

    users = []

    # First, populate with default users
    users = az_create_default_users(tenant_name, tenant_id, roles, params)
    # Now randomly generate users
    for i in range(num_users):
        user_id = str(uuid.uuid4()).upper()
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        upn = f"{first_name.lower()}.{last_name.lower()}@{tenant_name.lower()}"
        enabled = random.random() * 100 < enabled_perc
        NODES.append({
            "id": user_id,
            "name": f"{first_name} {last_name}",
            "userPrincipalName": upn,
            "type": "AZUser",
            "labels": ["AZUser"],
            "objectid": user_id,
            "tenantid": tenant_id, 
            "enabled": enabled,
            "displayName": f"{first_name} {last_name}"
        })
        NODE_GROUPS["AZUser"] = NODE_GROUPS.get("AZUser", [])
        NODE_GROUPS["AZUser"].append(user_id)
        EDGES.append({
            "source": tenant_id,
            "target": user_id,
            "type": "AZContains",
            "labels": ["AZContains"]
        })
        users.append(user_id)

    return (users)