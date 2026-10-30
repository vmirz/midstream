"""System views: what this control plane is, and which code it is running."""

from __future__ import annotations

import os
from typing import Any

from midstream.config import ControlPlaneConfig, domain_identity_from_environment
from midstream.identity.secrets import IdentitySecrets, get_identity_secrets
from midstream.protection.catalog import catalog_payload, parse_protection_text
from midstream.protection.secrets import ProtectionKey, get_protection_key
from midstream.settings import resolve_config
from midstream.storage.table import TableProtocol
from midstream.table_names import (
    DATASET_NAME_MAX_LENGTH,
    DUCKDB_CATALOG_NAMES,
    MATERIALIZATION_LABEL_MAX_LENGTH,
    RESERVED_TABLE_NAMES,
    TABLE_NAME_MAX_LENGTH,
)
from midstream.version import PACKAGE_VERSION

COMPONENT = "control-plane"


def version_view(
    *,
    config: ControlPlaneConfig | None = None,
    secrets: IdentitySecrets | None = None,
    protection_key: ProtectionKey | None = None,
) -> dict[str, Any]:
    """Report the running control-plane version, and which domain it is.

    Separate from the materialization views because it answers a question about the deployment
    rather than about any materialization: which build is serving this API. Runtimes report their
    own version through status events, so the console can show when the two have drifted apart.
    The shared console checks `domain` against its registry, and compares `federation_key_id` - the
    first 8 hex characters of the federation key's SHA-256, never the key - across domains, since
    every domain must share the key that signs access tokens (handbook 20). `protection_key_id` is
    the same for the key data protection hashes with: domains whose masked columns should join
    share it (handbook 22).
    """
    if config is None:
        domain_id, domain_name = domain_identity_from_environment(os.environ)
    else:
        domain_id, domain_name = config.domain_id, config.domain_name
    identity_secrets = get_identity_secrets() if secrets is None else secrets
    federation_key = identity_secrets.current_federation_key
    key = get_protection_key() if protection_key is None else protection_key
    return {
        "component": COMPONENT,
        "version": PACKAGE_VERSION,
        "domain": None if domain_id is None else {"id": domain_id, "name": domain_name or domain_id},
        "federation_key_id": None if federation_key is None else federation_key.key_id,
        "protection_key_id": None if key is None else key.key_id,
    }


def settings_view(
    *,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> dict[str, Any]:
    """Report what a new materialization takes: the deployment's, and the defaults of a create.

    A create request does not choose the destination, the namespace or the storage per replica:
    every materialization takes these. It may choose its replicas and partitioning, which default
    to the values here. A client creating a materialization shows them rather than guessing, so
    what it says will happen is what the control plane will do. They are the effective values -
    the deployment's, or an administrator's override. `default_destination_uri` is null when no
    destination is configured, and no materialization can then be created. `live_tail_enabled` says
    whether this deployment tails tables at all - every materialization's (handbook 19).
    `live_tail_default` is always true, for a 4.1 console's create dialog. `autoscaling_enabled` says
    whether the control plane scales runtimes that are far behind at all; a create may turn it off
    for its own materialization, and it is on unless it does. `protection` describes data protection
    (handbook 22): every kind and mask, the policy a create gets when it names none (`default`), and
    whether a protection key is configured - without one a policy that hashes is refused.
    """
    cfg = resolve_config(settings_table) if config is None else config
    key = get_protection_key()
    return {
        "default_destination_uri": cfg.default_destination_uri,
        "default_namespace": cfg.default_namespace,
        "default_replicas": cfg.default_runtime_replicas,
        "max_replicas": cfg.max_runtime_replicas,
        "max_topics": cfg.max_materialization_topics,
        "default_storage_size": cfg.default_runtime_storage_size,
        "default_partition_granularity": cfg.default_partition_granularity,
        "live_tail_enabled": cfg.live_tail_enabled,
        "live_tail_default": True,
        "autoscaling_enabled": cfg.runtime_autoscaling_enabled,
        "autoscale_max_replicas": cfg.runtime_autoscale_max_replicas,
        "autoscale_target_minutes": cfg.runtime_autoscale_target_minutes,
        # A materialization's name is a label; datasets and tables have names SQL reads.
        "label_max_length": MATERIALIZATION_LABEL_MAX_LENGTH,
        "dataset_name_max_length": DATASET_NAME_MAX_LENGTH,
        "table_name_max_length": TABLE_NAME_MAX_LENGTH,
        # What neither a table nor a dataset can be named; a dataset, a namespace in SQL, cannot
        # take DuckDB's own namespaces either.
        "reserved_names": sorted(RESERVED_TABLE_NAMES),
        "reserved_namespace_names": sorted(DUCKDB_CATALOG_NAMES),
        "protection": {
            **catalog_payload(),
            "default": parse_protection_text(cfg.default_protection),
            "key_configured": key is not None,
            "key_id": None if key is None else key.key_id,
        },
    }
