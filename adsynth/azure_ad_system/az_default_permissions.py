from adsynth.DATABASE import EDGES
from adsynth.utils.parameters import get_perc_param_value
import random

def az_create_permissions(users, groups, service_principals, key_vaults, vms, params):
    #print(f"Users: {len(users)}, Groups: {len(groups)}")

    # AZResetPassword
    reset_password_prob = get_perc_param_value("AZMisconfig", "reset_password", params)
    num_reset_users = int(len(users) * (reset_password_prob / 100))
    for user_id in random.sample(users, min(num_reset_users, len(users))):
        target_user = random.choice([u for u in users if u != user_id])
        EDGES.append({
            "source": user_id,
            "target": target_user,
            "type": "AZResetPassword",
            "labels": ["AZResetPassword"]
        })

    # AZAddMember
    add_member_prob = get_perc_param_value("AZMisconfig", "add_member", params)
    num_add_members = int(len(users) * (add_member_prob / 100))
    for user_id in random.sample(users, min(num_add_members, len(users))):
        target_group = random.choice(groups) if groups else None
        if target_group:
            EDGES.append({
                "source": user_id,
                "target": target_group,
                "type": "AZAddMembers",
                "labels": ["AZAddMembers"]
            })

    # AZAddSecret
    add_secret_prob = get_perc_param_value("AZMisconfig", "add_secret", params)
    num_add_secrets = int(len(users + service_principals) * (add_secret_prob / 100))
    principals = users + service_principals
    for principal_id in random.sample(principals, min(num_add_secrets, len(principals))):
        target_sp = random.choice([sp for sp in service_principals if sp != principal_id]) if service_principals else None
        if target_sp:
            EDGES.append({
                "source": principal_id,
                "target": target_sp,
                "type": "AZAddSecret",
                "labels": ["AZAddSecret"]
            })

    # AZOwns
    owns_resource_prob = get_perc_param_value("AZMisconfig", "owns_resource", params)
    num_owns = int(len(principals) * (owns_resource_prob / 100))
    resources = key_vaults + vms
    for principal_id in random.sample(principals, min(num_owns, len(principals))):
        target_resource = random.choice(resources) if resources else None
        if target_resource:
            EDGES.append({
                "source": principal_id,
                "target": target_resource,
                "type": "AZOwns",
                "labels": ["AZOwns"]
            })

    # Misconfigured group memberships
    misconfig_group_prob = get_perc_param_value("AZMisconfig", "misconfig_group_members", params)
    num_misconfig_members = int(len(users) * (misconfig_group_prob / 100))
    for user_id in random.sample(users, min(num_misconfig_members, len(users))):
        target_group = random.choice(groups) if groups else None
        if target_group:
            EDGES.append({
                "source": user_id,
                "target": target_group,
                "type": "AZMemberOf",
                "labels": ["AZMemberOf"]
            })

    return len(EDGES)