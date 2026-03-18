"""
generators/common.py — Shared primitives for all generators
============================================================
Name/surname pools loaded from ADSynth data files (first.pkl, last.pkl,
domain.pkl).  SID format, UPN format, and timestamp logic match ADSynth
conventions exactly.
"""

import os
import pickle
import random
import uuid
from typing import List


def _data_path() -> str:
    return os.path.join(os.getcwd(), "data")

def _load_pkl(filename: str) -> list:
    path = os.path.join(_data_path(), filename)
    with open(path, "rb") as f:
        return pickle.load(f)

_FIRST_NAMES: List[str] = []
_LAST_NAMES:  List[str] = []
_DOMAIN_NAMES: List[str] = []

def get_first_names() -> List[str]:
    global _FIRST_NAMES
    if not _FIRST_NAMES:
        _FIRST_NAMES = _load_pkl("first.pkl")
    return _FIRST_NAMES

def get_last_names() -> List[str]:
    global _LAST_NAMES
    if not _LAST_NAMES:
        _LAST_NAMES = _load_pkl("last.pkl")
    return _LAST_NAMES

def get_domain_names() -> List[str]:
    global _DOMAIN_NAMES
    if not _DOMAIN_NAMES:
        _DOMAIN_NAMES = _load_pkl("domain.pkl")
    return _DOMAIN_NAMES

def det_uuid(namespace: str, *parts: str) -> str:
    return str(uuid.uuid5(uuid.UUID(int=0), f"{namespace}:{'|'.join(parts)}"))

def rand_uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))

def make_domain_sid(rng: random.Random) -> str:
    """S-1-5-21-{9digits}-{9digits}-{10digits} — matches ADSynth domains.py."""
    sub1 = rng.randint(100_000_000,  999_999_999)
    sub2 = rng.randint(100_000_000,  999_999_999)
    sub3 = rng.randint(1_000_000_000, 9_999_999_999)
    return f"S-1-5-21-{sub1}-{sub2}-{sub3}"

def make_object_sid(domain_sid: str, rid: int) -> str:
    return f"{domain_sid}-{rid}"

def make_tenant_guid(rng: random.Random) -> str:
    return rand_uuid(rng)

def random_full_name(rng: random.Random) -> tuple:
    return rng.choice(get_first_names()), rng.choice(get_last_names())

def make_upn_ad(first: str, last: str, index: int, domain_fqdn: str) -> str:
    """ADSynth format: {F}{LAST}{index:05d}@{DOMAIN} uppercased."""
    return f"{first[0]}{last}{index:05d}@{domain_fqdn}".upper()

def make_upn_entra(first: str, last: str, tenant_name: str) -> str:
    """ADSynth az format: {first}.{last}@{tenant} lowercased."""
    return f"{first.lower()}.{last.lower()}@{tenant_name.lower()}"

def make_display_name(first: str, last: str) -> str:
    return f"{first} {last}"

def generate_timestamp(rng: random.Random, current_time: int) -> int:
    """Matches ADSynth utils/time.py generate_timestamp()."""
    choice = rng.randint(-1, 1)
    if choice == 1:
        return current_time - rng.randint(0, 31_536_000)
    return choice

def get_user_timestamp(rng: random.Random, current_time: int, enabled: bool) -> int:
    if enabled:
        return generate_timestamp(rng, current_time)
    return -1

def weighted_choice(rng: random.Random, choices: dict) -> str:
    keys    = list(choices.keys())
    weights = [choices[k] for k in keys]
    return rng.choices(keys, weights=weights, k=1)[0]

def pick_domain_name(rng: random.Random, suffix: str = "local") -> str:
    prefix = rng.choice(get_domain_names())
    return f"{prefix.lower()}.{suffix}"