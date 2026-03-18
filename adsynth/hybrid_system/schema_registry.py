"""
Hybrid Identity Graph Schema Registry
======================================

This registry defines:
  - Plane enum       : separates AD / Entra / Hybrid entities
  - NodeLabel enum   : all node types in the hybrid ontology
  - RelType enum     : all relationship types
  - Required/optional properties per node type
  - Allowed endpoint constraints per relationship type
  - Validation helpers (isAllowedEdge, check_required_properties)

References: Paper Appendix A (schema registry) and Appendix B (non-human identity modeling)
"""

from enum import Enum
from typing import Set, Dict, List, Tuple, Any


# Schema Version

SCHEMA_VERSION = "1.0.0"


# Plane Enum
# Separates on-prem vs cloud vs cross-boundary entities (Paper §4.1)

class Plane(str, Enum):
    AD     = "AD"      # On-premises Active Directory
    Entra  = "Entra"   # Microsoft Entra ID (cloud)
    Hybrid = "Hybrid"  # Spans both planes (e.g., SyncIdentity)


# NodeLabel Enum  (Paper §4.2 + Appendix A.1.1)


class NodeLabel(str, Enum):
    # Identity containers
    ADDomain   = "ADDomain"   # On-prem AD domain boundary
    Tenant     = "Tenant"     # Entra ID tenant boundary

    # Human principals
    User             = "User"           # Human user (AD or Entra)
    Group            = "Group"          # AD security group
    AzureADGroup     = "AzureADGroup"   # Cloud group
    AzureADRole      = "AzureADRole"    # Directory role (e.g. Global Admin)

    # Non-human principals  (Paper §4.2.3 + §5)
    NonHumanIdentity = "NonHumanIdentity"  # Supertype
    SyncIdentity     = "SyncIdentity"      # Per-link sync principal
    ServicePrincipal = "ServicePrincipal"  # Cloud service principal
    ManagedIdentity  = "ManagedIdentity"   # Azure managed identity
    AutomationAccount = "AutomationAccount"  # On-prem automation

    # Assets
    Computer = "Computer"  # AD-joined machine
    Server   = "Server"    # Specialization: DC / EntraConnect / PTAAgent / ADFS

    # Policy objects (optional, for realism)
    OU                      = "OU"
    GPO                     = "GPO"
    ConditionalAccessPolicy = "ConditionalAccessPolicy"



# RelType Enum  (Paper §4.3 + Appendix A.1.1)


class RelType(str, Enum):
    # ── Containment / membership 
    MEMBER_OF        = "MEMBER_OF"         # User|Group|NHI -> Group
    CLOUD_MEMBER_OF  = "CLOUD_MEMBER_OF"   # User|AZGroup|NHI -> AzureADGroup
    HAS_AZ_ROLE      = "HAS_AZ_ROLE"       # User|AZGroup|NHI -> AzureADRole

    # ── Privilege / delegation (attack-path edges) ─
    ADMIN_TO         = "ADMIN_TO"          # Principal -> Computer|Server
    CAN_RDP          = "CAN_RDP"           # Principal -> Computer|Server
    HAS_SESSION      = "HAS_SESSION"       # User -> Computer|Server
    DELEGATED_RIGHT  = "DELEGATED_RIGHT"   # Principal -> ADDomain|OU|User|Group|Computer|Server
    HAS_AD_RIGHT     = "HAS_AD_RIGHT"      # Principal -> ADDomain

    # Multi-tenant / trust 
    DOMAIN_TRUSTS    = "DOMAIN_TRUSTS"     # ADDomain -> ADDomain
    SYNC_LINK        = "SYNC_LINK"         # ADDomain -> Tenant

    # Hybrid seam edges 
    SYNCED_TO        = "SYNCED_TO"         # User(AD) -> User(Entra)
    SERVICES_LINK    = "SERVICES_LINK"     # SyncIdentity -> ADDomain
    SYNCS_TO         = "SYNCS_TO"          # SyncIdentity -> Tenant
    RUNS_ON          = "RUNS_ON"           # NonHumanIdentity -> Server
    IS_FEDERATED_WITH = "IS_FEDERATED_WITH"  # ADDomain -> Tenant  (AD FS)
    HAS_PTA_AGENT    = "HAS_PTA_AGENT"     # Tenant -> Server     (PTA)



# Helper sets — used in endpoint constraint checks


# All labels that count as "Principal" (can be source of privilege edges)
PRINCIPAL_LABELS: Set[NodeLabel] = {
    NodeLabel.User,
    NodeLabel.Group,
    NodeLabel.AzureADGroup,
    NodeLabel.NonHumanIdentity,
    NodeLabel.SyncIdentity,
    NodeLabel.ServicePrincipal,
    NodeLabel.ManagedIdentity,
    NodeLabel.AutomationAccount,
}

# All labels that count as "Asset"
ASSET_LABELS: Set[NodeLabel] = {
    NodeLabel.Computer,
    NodeLabel.Server,
}

# Labels that can be targets of DELEGATED_RIGHT
DELEGATED_RIGHT_TARGETS: Set[NodeLabel] = {
    NodeLabel.ADDomain,
    NodeLabel.OU,
    NodeLabel.User,
    NodeLabel.Group,
    NodeLabel.Computer,
    NodeLabel.Server,
}



# Required properties per NodeLabel  (Paper Appendix A.1.2 + B.1)

#
# All nodes MUST also have: id, name, plane, runId
# (tenantId and domainId are recommended where applicable)

REQUIRED_PROPERTIES: Dict[NodeLabel, Set[str]] = {
    NodeLabel.ADDomain:   {"sid"},
    NodeLabel.Tenant:     {"tenantGuid"},

    NodeLabel.User:       {"upn"},          # or samAccountName for AD users
    NodeLabel.Group:      {"sid"},
    NodeLabel.AzureADGroup: {"objectId"},
    NodeLabel.AzureADRole:  {"roleId"},     # or templateId

    # Non-human principals — all share ownerType + lifecycle
    NodeLabel.NonHumanIdentity: {"ownerType", "lifecycle"},
    NodeLabel.SyncIdentity:     {"ownerType", "lifecycle", "syncMode", "linkKey"},
    NodeLabel.ServicePrincipal: {"ownerType", "lifecycle", "appId"},
    NodeLabel.ManagedIdentity:  {"ownerType", "lifecycle", "miType"},
    NodeLabel.AutomationAccount: {"ownerType", "lifecycle", "automationKind"},

    NodeLabel.Computer: {"hostname"},
    NodeLabel.Server:   {"hostname"},

    # Policy objects — no extra required fields beyond globals
    NodeLabel.OU:  set(),
    NodeLabel.GPO: set(),
    NodeLabel.ConditionalAccessPolicy: set(),
}

# Global required properties present on every node
GLOBAL_REQUIRED_PROPERTIES: Set[str] = {"id", "name", "plane", "runId"}



# Allowed values for enum-style properties


VALID_PLANES: Set[str] = {p.value for p in Plane}

VALID_OWNER_TYPES: Set[str] = {"Team", "System", "Unknown"}

VALID_LIFECYCLES: Set[str] = {"LongLived", "Ephemeral"}

VALID_SYNC_MODES: Set[str] = {"PHS", "PTA", "ADFS", "Mixed"}

VALID_MI_TYPES: Set[str] = {"SystemAssigned", "UserAssigned"}

VALID_AUTOMATION_KINDS: Set[str] = {"service", "scheduled-task", "deployment", "script"}

VALID_SERVER_ROLES: Set[str] = {"DomainController", "EntraConnect", "PTAAgent", "ADFS"}



# Allowed relationship endpoints  (Paper Appendix A.2 + B.2)
# Each entry: RelType -> list of (src_labels, dst_labels) pairs
# A relationship is allowed if src_label ∈ src_set AND dst_label ∈ dst_set
# for any pair in the list.


ALLOWED_ENDPOINTS: Dict[RelType, List[Tuple[Set[NodeLabel], Set[NodeLabel]]]] = {

    RelType.MEMBER_OF: [(
        {NodeLabel.User, NodeLabel.Group} | PRINCIPAL_LABELS,
        {NodeLabel.Group}
    )],

    RelType.CLOUD_MEMBER_OF: [(
        {NodeLabel.User, NodeLabel.AzureADGroup} | PRINCIPAL_LABELS,
        {NodeLabel.AzureADGroup}
    )],

    RelType.HAS_AZ_ROLE: [(
        {NodeLabel.User, NodeLabel.AzureADGroup} | PRINCIPAL_LABELS,
        {NodeLabel.AzureADRole}
    )],

    RelType.ADMIN_TO: [(PRINCIPAL_LABELS, ASSET_LABELS)],

    RelType.CAN_RDP:  [(PRINCIPAL_LABELS, ASSET_LABELS)],

    RelType.HAS_SESSION: [({NodeLabel.User}, ASSET_LABELS)],

    RelType.DELEGATED_RIGHT: [(PRINCIPAL_LABELS, DELEGATED_RIGHT_TARGETS)],

    RelType.HAS_AD_RIGHT: [(PRINCIPAL_LABELS, {NodeLabel.ADDomain})],

    RelType.DOMAIN_TRUSTS: [({NodeLabel.ADDomain}, {NodeLabel.ADDomain})],

    RelType.SYNC_LINK: [({NodeLabel.ADDomain}, {NodeLabel.Tenant})],

    # plane=AD User -> plane=Entra User
    RelType.SYNCED_TO: [({NodeLabel.User}, {NodeLabel.User})],

    RelType.SERVICES_LINK: [({NodeLabel.SyncIdentity}, {NodeLabel.ADDomain})],

    RelType.SYNCS_TO: [({NodeLabel.SyncIdentity}, {NodeLabel.Tenant})],

    RelType.RUNS_ON: [(
        {NodeLabel.NonHumanIdentity, NodeLabel.SyncIdentity,
         NodeLabel.ServicePrincipal, NodeLabel.ManagedIdentity,
         NodeLabel.AutomationAccount},
        {NodeLabel.Server}
    )],

    RelType.IS_FEDERATED_WITH: [({NodeLabel.ADDomain}, {NodeLabel.Tenant})],

    RelType.HAS_PTA_AGENT: [({NodeLabel.Tenant}, {NodeLabel.Server})],
}



# Validation functions


def is_allowed_edge(rel_type: RelType,
                    src_label: NodeLabel,
                    dst_label: NodeLabel) -> bool:
    """
    Return True if (src_label) -[rel_type]-> (dst_label) is schema-valid.
    Uses the ALLOWED_ENDPOINTS table above.
    """
    if rel_type not in ALLOWED_ENDPOINTS:
        return False
    for src_set, dst_set in ALLOWED_ENDPOINTS[rel_type]:
        if src_label in src_set and dst_label in dst_set:
            return True
    return False


def check_required_properties(label: NodeLabel,
                               properties: Dict[str, Any]) -> List[str]:
    """
    Return a list of missing required property names for a given node label.
    Checks both global required properties and label-specific ones.
    Returns an empty list if all required properties are present.
    """
    missing = []

    # Global checks
    for prop in GLOBAL_REQUIRED_PROPERTIES:
        if prop not in properties:
            missing.append(prop)

    # Label-specific checks
    label_required = REQUIRED_PROPERTIES.get(label, set())
    for prop in label_required:
        if prop not in properties:
            missing.append(prop)

    return missing


def validate_enum_property(prop_name: str, value: str, valid_set: Set[str]) -> List[str]:
    """
    Validate that a string property value is within an allowed set.
    Returns a list of error messages (empty if valid).
    """
    if value not in valid_set:
        return [f"Property '{prop_name}' has invalid value '{value}'. "
                f"Must be one of: {sorted(valid_set)}"]
    return []


def validate_node(label: NodeLabel, properties: Dict[str, Any]) -> List[str]:
    """
    Full node validation: required properties + enum values.
    Returns a list of error strings. Empty list = valid.
    """
    errors = check_required_properties(label, properties)

    # plane check
    if "plane" in properties:
        errors += validate_enum_property("plane", properties["plane"], VALID_PLANES)

    # Non-human identity specific checks
    if label in {NodeLabel.NonHumanIdentity, NodeLabel.SyncIdentity,
                 NodeLabel.ServicePrincipal, NodeLabel.ManagedIdentity,
                 NodeLabel.AutomationAccount}:
        if "ownerType" in properties:
            errors += validate_enum_property(
                "ownerType", properties["ownerType"], VALID_OWNER_TYPES)
        if "lifecycle" in properties:
            errors += validate_enum_property(
                "lifecycle", properties["lifecycle"], VALID_LIFECYCLES)

    if label == NodeLabel.SyncIdentity:
        if "syncMode" in properties:
            errors += validate_enum_property(
                "syncMode", properties["syncMode"], VALID_SYNC_MODES)

    if label == NodeLabel.ManagedIdentity:
        if "miType" in properties:
            errors += validate_enum_property(
                "miType", properties["miType"], VALID_MI_TYPES)

    if label == NodeLabel.AutomationAccount:
        if "automationKind" in properties:
            errors += validate_enum_property(
                "automationKind", properties["automationKind"], VALID_AUTOMATION_KINDS)

    if label == NodeLabel.Server:
        if "serverRole" in properties:
            errors += validate_enum_property(
                "serverRole", properties["serverRole"], VALID_SERVER_ROLES)

    return errors



# Utility: build SyncIdentity linkKey and id from (domainId, tenantId)


def make_sync_identity_id(domain_id: str, tenant_id: str) -> str:
    """Deterministic SyncIdentity node id: 'sync:<domainId>:<tenantId>'"""
    return f"sync:{domain_id}:{tenant_id}"


def make_sync_identity_link_key(domain_id: str, tenant_id: str) -> str:
    """Deterministic linkKey encoding (domain, tenant): 'domainId->tenantId'"""
    return f"{domain_id}->{tenant_id}"



# Print registry summary (useful for debugging / the Week 1 deliverable check)


def print_registry_summary() -> None:
    print(f"\n{'='*60}")
    print(f"Hybrid Identity Graph Schema Registry  v{SCHEMA_VERSION}")
    print(f"{'='*60}")
    print(f"  Node labels   : {len(NodeLabel)}")
    print(f"  Rel types     : {len(RelType)}")
    print(f"  Plane values  : {[p.value for p in Plane]}")
    print(f"\nNode labels:")
    for nl in NodeLabel:
        req = REQUIRED_PROPERTIES.get(nl, set())
        print(f"  {nl.value:<25} required props: {sorted(req) if req else '(globals only)'}")
    print(f"\nRelationship types:")
    for rt in RelType:
        print(f"  {rt.value}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    print_registry_summary()

    # Quick self-test
    print("Self-test: is_allowed_edge")
    tests = [
        (RelType.SYNC_LINK,     NodeLabel.ADDomain,      NodeLabel.Tenant,        True),
        (RelType.SERVICES_LINK, NodeLabel.SyncIdentity,  NodeLabel.ADDomain,      True),
        (RelType.SYNCS_TO,      NodeLabel.SyncIdentity,  NodeLabel.Tenant,        True),
        (RelType.RUNS_ON,       NodeLabel.SyncIdentity,  NodeLabel.Server,        True),
        (RelType.HAS_PTA_AGENT, NodeLabel.Tenant,        NodeLabel.Server,        True),
        (RelType.SYNC_LINK,     NodeLabel.Tenant,        NodeLabel.ADDomain,      False),  # reversed
        (RelType.RUNS_ON,       NodeLabel.User,          NodeLabel.Server,        False),  # User can't RUNS_ON
    ]
    all_pass = True
    for rel, src, dst, expected in tests:
        result = is_allowed_edge(rel, src, dst)
        status = "PASS" if result == expected else "FAIL"
        if status == "FAIL":
            all_pass = False
        print(f"  [{status}] {src.value} -[{rel.value}]-> {dst.value}  "
              f"(expected={expected}, got={result})")
    print(f"\nAll tests passed: {all_pass}\n")
