"""Data protection: masking sensitive fields before they reach the lake (handbook 22).

A materialization masks the values of fields that rules recognise as sensitive by their names -
emails, phone numbers, national identifiers, secrets and more - before the writer stages them, so
no component after the writer ever holds them: not the seal, not the data files, not the live tail,
not a query.

- `catalog`: the kinds, their rules and partial forms, and the masks.
- `rules`: recognising a kind from a field's name.
- `mask`: hashing, partial masks and withholding.
- `policy`: a materialization's policy and the table property recording what a table protects.
- `plan`: the per-record walk, and the committer's classification of fields.
- `secrets`: the protection key.
"""

from midstream.protection.catalog import (
    DEFAULT_PROTECTION,
    KIND_IDS,
    KINDS,
    KINDS_BY_ID,
    MASK_HASH,
    MASK_PARTIAL,
    MASK_REDACT,
    MASKS,
)
from midstream.protection.mask import MissingProtectionKey
from midstream.protection.plan import Decision, ProtectionPlan, initial_protection, record_new_fields, record_policy, review
from midstream.protection.policy import (
    NONE,
    PROPERTY_PROTECTION,
    FieldProtection,
    PolicyError,
    ProtectionChange,
    ProtectionPolicy,
    TableProtection,
    parse_policy,
    protection_of_properties,
)

__all__ = [
    "DEFAULT_PROTECTION",
    "KIND_IDS",
    "KINDS",
    "KINDS_BY_ID",
    "MASK_HASH",
    "MASK_PARTIAL",
    "MASK_REDACT",
    "MASKS",
    "NONE",
    "PROPERTY_PROTECTION",
    "Decision",
    "FieldProtection",
    "MissingProtectionKey",
    "PolicyError",
    "ProtectionChange",
    "ProtectionPlan",
    "ProtectionPolicy",
    "TableProtection",
    "initial_protection",
    "parse_policy",
    "protection_of_properties",
    "record_new_fields",
    "record_policy",
    "review",
]
