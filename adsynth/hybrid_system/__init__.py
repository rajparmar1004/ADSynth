"""
hybrid_system — Week 1 foundations for the hybrid AD-Entra identity graph generator.

Modules:
  schema_registry        — NodeLabel/RelType enums, property requirements, endpoint constraints
  export_writer          — In-memory graph store + JSONL export
  invariant_validators   — Semantic invariant checks (Appendix A.3)
  hybrid_config          — Configuration object Θ definition and loading
  reproducibility_bundle — Bundle writer (config + seed + graph + manifest)
"""

from adsynth.hybrid_system.schema_registry import (
    SCHEMA_VERSION,
    Plane,
    NodeLabel,
    RelType,
    PRINCIPAL_LABELS,
    ASSET_LABELS,
    REQUIRED_PROPERTIES,
    GLOBAL_REQUIRED_PROPERTIES,
    VALID_PLANES,
    VALID_OWNER_TYPES,
    VALID_LIFECYCLES,
    VALID_SYNC_MODES,
    VALID_SERVER_ROLES,
    ALLOWED_ENDPOINTS,
    is_allowed_edge,
    check_required_properties,
    validate_node,
    make_sync_identity_id,
    make_sync_identity_link_key,
    print_registry_summary,
)

from adsynth.hybrid_system.export_writer import (
    HYBRID_NODES,
    HYBRID_EDGES,
    add_node,
    add_edge,
    get_node_by_id,
    reset_graph,
    write_graph_jsonl,
    write_graph_stats,
)

from adsynth.hybrid_system.invariant_validators import (
    validate_graph_invariants,
    print_validation_report,
    check_sync_identity_invariant,
    check_phs_invariant,
    check_pta_invariant,
    check_adfs_invariant,
)

from adsynth.hybrid_system.hybrid_config import (
    DEFAULT_HYBRID_CONFIG,
    load_hybrid_config,
    validate_config,
)

from adsynth.hybrid_system.reproducibility_bundle import (
    write_reproducibility_bundle,
    print_bundle_summary,
)

__all__ = [
    # schema_registry
    "SCHEMA_VERSION", "Plane", "NodeLabel", "RelType",
    "PRINCIPAL_LABELS", "ASSET_LABELS",
    "REQUIRED_PROPERTIES", "GLOBAL_REQUIRED_PROPERTIES",
    "VALID_PLANES", "VALID_OWNER_TYPES", "VALID_LIFECYCLES",
    "VALID_SYNC_MODES", "VALID_SERVER_ROLES", "ALLOWED_ENDPOINTS",
    "is_allowed_edge", "check_required_properties", "validate_node",
    "make_sync_identity_id", "make_sync_identity_link_key", "print_registry_summary",
    # export_writer
    "HYBRID_NODES", "HYBRID_EDGES",
    "add_node", "add_edge", "get_node_by_id", "reset_graph",
    "write_graph_jsonl", "write_graph_stats",
    # invariant_validators
    "validate_graph_invariants", "print_validation_report",
    "check_sync_identity_invariant", "check_phs_invariant",
    "check_pta_invariant", "check_adfs_invariant",
    # hybrid_config
    "DEFAULT_HYBRID_CONFIG", "load_hybrid_config", "validate_config",
    # reproducibility_bundle
    "write_reproducibility_bundle", "print_bundle_summary",
]
