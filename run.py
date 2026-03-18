#!/usr/bin/env python3
"""
run.py — Week 1 CLI entry point for the Hybrid Identity Graph Generator
========================================================================
Usage:
  python run.py [OPTIONS]

Options:
  --config PATH     Path to configuration JSON file Θ  (default: built-in defaults)
  --seed INT        Global random seed for reproducibility  (default: 42)
  --seed-file PATH  Path to JSON file containing the full seed vector s
  --output-dir DIR  Output directory for the run bundle  (default: ./generated_datasets)
  --run-id STR      Human-readable run identifier  (default: auto-generated)
  --validate        Run semantic invariant checks after generation  (default: True)
  --registry-info   Print the schema registry summary and exit
  --help            Show this message and exit

Week 1 deliverable: produces an empty graph (no entity generators yet) plus
the reproducibility bundle, demonstrating the full pipeline skeleton.

Example:
  python run.py --seed 1337 --output-dir ./runs/week1
"""

import argparse
import json
import os
import random
import sys
import time
import uuid

# ─── allow running from repo root without installing ──────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from adsynth.hybrid_system import (
    # schema
    SCHEMA_VERSION,
    print_registry_summary,
    validate_config,
    # graph
    reset_graph,
    HYBRID_NODES,
    HYBRID_EDGES,
    # config
    load_hybrid_config,
    DEFAULT_HYBRID_CONFIG,
    # validation
    print_validation_report,
    validate_graph_invariants,
    # bundle
    write_reproducibility_bundle,
    print_bundle_summary,
)


# Seed vector helpers

def build_seed_vector(global_seed: int) -> dict:
    """
    Expand a scalar global seed into a reproducible seed vector s.
    Each sub-seed is derived deterministically from the global seed.
    """
    rng = random.Random(global_seed)
    return {
        "globalSeed":      global_seed,
        "domainSeed":      rng.randint(0, 2**31),
        "tenantSeed":      rng.randint(0, 2**31),
        "userSeed":        rng.randint(0, 2**31),
        "groupSeed":       rng.randint(0, 2**31),
        "nhSeed":          rng.randint(0, 2**31),   # non-human identity
        "syncSeed":        rng.randint(0, 2**31),
        "misconfigSeed":   rng.randint(0, 2**31),
    }


def load_seed_vector(seed_file: str) -> dict:
    """Load a seed vector from a JSON file."""
    with open(seed_file, "r", encoding="utf-8") as fh:
        return json.load(fh)


# Week 2 generator

def generate_graph(config: dict, seed: dict) -> dict:
    """
    Week 3: topology + principal generation.
      1. Domains + trust edges
      2. Tenants
      3. Sync links + bridge components
      4. Human principals (AD users/groups, Entra users/groups, SYNCED_TO)
      5. Non-human principals (ServicePrincipal, ManagedIdentity, AutomationAccount)
    """
    from adsynth.generators.domain_generator import create_domains, create_trusts
    from adsynth.generators.tenant_generator import create_tenants
    from adsynth.generators.sync_link_generator import create_sync_links
    from adsynth.generators.user_generator import create_humans
    from adsynth.generators.group_generator import create_groups
    from adsynth.generators.nhi_generator import create_non_humans

    reset_graph()
    random.seed(seed["globalSeed"])

    run_id = seed.get("_run_id", "run")

    # Step 1 — topology
    domains = create_domains(config, seed, run_id)
    _trusts = create_trusts(domains, config, seed, run_id)
    tenants = create_tenants(config, seed, run_id)
    links   = create_sync_links(domains, tenants, config, seed, run_id)

    # Step 2 — human principals
    humans  = create_humans(domains, tenants, links, config, seed, run_id)

    # Step 2b — groups + membership edges
    groups  = create_groups(
        domains, tenants,
        humans["ad_users"], humans["entra_users"],
        config, seed, run_id,
    )

    # Step 2c — non-human principals
    nhi = create_non_humans(
        domains, tenants, humans["users_per_tenant"],
        config, seed, run_id,
    )

    return {
        "domains": domains, "tenants": tenants, "links": links,
        "humans": humans, "groups": groups, "nhi": nhi,
    }


# CLI

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run.py",
        description="Hybrid AD-Entra Identity Graph Generator — Week 1 CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--config", metavar="PATH",
        help="Path to configuration JSON file Θ",
    )
    parser.add_argument(
        "--seed", type=int, default=42, metavar="INT",
        help="Global random seed (default: 42)",
    )
    parser.add_argument(
        "--seed-file", metavar="PATH",
        help="Path to JSON file containing the full seed vector s (overrides --seed)",
    )
    parser.add_argument(
        "--output-dir", default="generated_datasets", metavar="DIR",
        help="Output directory for the run bundle (default: ./generated_datasets)",
    )
    parser.add_argument(
        "--run-id", metavar="STR",
        help="Human-readable run identifier (default: auto-generated)",
    )
    parser.add_argument(
        "--no-validate", action="store_true",
        help="Skip semantic invariant checks",
    )
    parser.add_argument(
        "--registry-info", action="store_true",
        help="Print schema registry summary and exit",
    )
    return parser


def main(argv=None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    # ── Schema registry info mode 
    if args.registry_info:
        print_registry_summary()
        return 0

    # ── Banner 
    print(f"\n{'='*60}")
    print(f"  Hybrid AD-Entra Identity Graph Generator")
    print(f"  Schema Version: {SCHEMA_VERSION}")
    print(f"{'='*60}\n")

    # ── Load configuration Θ 
    print(f"[1/5] Loading configuration...")
    config = load_hybrid_config(args.config)
    config_errors = validate_config(config)
    if config_errors:
        print("  ERROR: Configuration validation failed:")
        for e in config_errors:
            print(f"    ✗ {e}")
        return 1
    print(f"       Config loaded: {len(config)} top-level sections")
    if args.config:
        print(f"       Source: {args.config}")
    else:
        print(f"       Source: built-in defaults")

    # ── Build seed vector s ───────────────────────────────────────────────────
    print(f"\n[2/5] Building seed vector...")
    if args.seed_file:
        seed = load_seed_vector(args.seed_file)
        print(f"       Loaded from: {args.seed_file}")
    else:
        seed = build_seed_vector(args.seed)
        print(f"       globalSeed={seed['globalSeed']} → expanded to {len(seed)} sub-seeds")

    # ── Run ID ────────────────────────────────────────────────────────────────
    run_id = args.run_id or f"run-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    run_dir = os.path.join(args.output_dir, run_id)
    print(f"       runId: {run_id}")
    print(f"       runDir: {run_dir}")
    seed["_run_id"] = run_id   # make available to generators

    # ── Generate graph ────────────────────────────────────────────────────────
    print(f"\n[3/5] Generating graph...")
    t0 = time.time()
    generate_graph(config, seed)
    elapsed = time.time() - t0
    print(f"       Nodes: {len(HYBRID_NODES)}")
    print(f"       Edges: {len(HYBRID_EDGES)}")
    print(f"       Time:  {elapsed:.3f}s")

    # ── Semantic invariant validation ─────────────────────────────────────────
    if not args.no_validate:
        print(f"\n[4/5] Validating semantic invariants...")
        results = validate_graph_invariants()
        all_pass = print_validation_report(results)
        if not all_pass:
            print("  WARNING: Semantic invariant violations detected (see report above)")
    else:
        print(f"\n[4/5] Validation skipped (--no-validate)")

    # ── Write reproducibility bundle ──────────────────────────────────────────
    print(f"\n[5/5] Writing reproducibility bundle...")
    paths = write_reproducibility_bundle(run_id, run_dir, config, seed)
    print_bundle_summary(paths, run_id)

    # ── BloodHound CE export ───────────────────────────────────────────────────
    print(f"\n[+] Exporting to BloodHound CE format...")
    from bloodhound_exporter import export_bloodhound
    bh_zip = export_bloodhound(HYBRID_NODES, HYBRID_EDGES, run_dir, run_id)
    print(f"       Upload this to BloodHound CE: {bh_zip}")

    print(f"Done\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())