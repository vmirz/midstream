"""Namespaced key builders for the global control-plane table."""

from __future__ import annotations

MATERIALIZATION_KEY_PREFIX = "materialization:"
MATERIALIZATION_STATUS_KEY_PREFIX = "materialization-status:"
MATERIALIZATION_SCHEMA_KEY_PREFIX = "materialization-schema:"
MATERIALIZATION_SCHEMA_VERSION_KEY_PREFIX = "materialization-schema-version:"
MATERIALIZATION_TABLE_KEY_PREFIX = "materialization-table:"
MATERIALIZATION_OP_KEY_PREFIX = "materialization-op:"
RUNTIME_BINDING_KEY_PREFIX = "runtime-binding:"
RUNTIME_AUTOSCALE_KEY_PREFIX = "runtime-autoscale:"

# Version numbers are zero padded so lexicographic prefix scans stay numerically ordered.
SCHEMA_VERSION_KEY_WIDTH = 10


def materialization_prefix() -> str:
    return MATERIALIZATION_KEY_PREFIX


def materialization_key(materialization_id: str) -> str:
    return f"{MATERIALIZATION_KEY_PREFIX}{materialization_id}"


def materialization_routing_key(materialization_id: str) -> str:
    return materialization_key(materialization_id)


def materialization_status_key(materialization_id: str) -> str:
    return f"{MATERIALIZATION_STATUS_KEY_PREFIX}{materialization_id}"


# A materialization has one table per source topic, so the records about a table are keyed by the
# materialization and then the table's name: `materialization-table:<id>:<table>`. Table names hold
# only letters, digits and underscores, so the `:` that ends a prefix never occurs inside one.


def materialization_schema_prefix(materialization_id: str) -> str:
    return f"{MATERIALIZATION_SCHEMA_KEY_PREFIX}{materialization_id}:"


def materialization_schema_key(materialization_id: str, table: str) -> str:
    return f"{materialization_schema_prefix(materialization_id)}{table}"


def materialization_schema_version_prefix(materialization_id: str, table: str | None = None) -> str:
    """The archive of one table's schemas, or with no table of every table of the materialization."""
    base = f"{MATERIALIZATION_SCHEMA_VERSION_KEY_PREFIX}{materialization_id}:"
    return base if table is None else f"{base}{table}:"


def materialization_schema_version_key(materialization_id: str, table: str, schema_version: int) -> str:
    padded = f"{schema_version:0{SCHEMA_VERSION_KEY_WIDTH}d}"
    return f"{materialization_schema_version_prefix(materialization_id, table)}{padded}"


def materialization_table_prefix(materialization_id: str) -> str:
    return f"{MATERIALIZATION_TABLE_KEY_PREFIX}{materialization_id}:"


def materialization_table_key(materialization_id: str, table: str) -> str:
    return f"{materialization_table_prefix(materialization_id)}{table}"


def materialization_operation_key(materialization_id: str, operation_id: str) -> str:
    return f"{materialization_operation_prefix(materialization_id)}{operation_id}"


def materialization_operation_prefix(materialization_id: str) -> str:
    return f"{MATERIALIZATION_OP_KEY_PREFIX}{materialization_id}:"


def runtime_binding_key(materialization_id: str) -> str:
    return f"{RUNTIME_BINDING_KEY_PREFIX}{materialization_id}"


def runtime_autoscale_key(materialization_id: str) -> str:
    return f"{RUNTIME_AUTOSCALE_KEY_PREFIX}{materialization_id}"
