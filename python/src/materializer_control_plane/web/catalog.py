"""The Iceberg REST catalog facade: external engines list, read and commit to tables (spec §5.6).

Engines point at `<control plane>/catalog` as a REST catalog (`uri`) with a bearer token - an
access token (`tok_…`, made on the account page) or a session's. Every view authorizes first:
`catalog.read` on some dataset to use the catalog at all; then, on each dataset it touches,
`catalog.read` to list and load and `catalog.write` to commit.

Every dataset is a namespace, and holds the tables materializations put in it, whatever
materialization writes each: an engine reads `lake.<dataset>.<table>`, and one statement can join
tables of different datasets and materializations. A dataset the caller cannot read is not listed,
and answers as though it did not exist: an engine lists namespaces in order to load them, and one it
could not read would only produce errors. A name resolves in three keyed reads: the dataset's name
index, the table's claim in the directory, and the table's record in the state table. Loading a
table reads its pointer and metadata from GCS on a worker thread, under a deadline; what was read is
answered again for `catalog_metadata_cache_seconds`, so an engine planning many queries does not
cost a GCS read each.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import threading
import time
from typing import Any

from materializer_control_plane.config import ControlPlaneConfig
from materializer_control_plane.identity.authorization import require
from materializer_control_plane.identity.principals import Principal
from materializer_control_plane.lakehouse.catalog import GcsPointerCatalog, StaticTableDirectory, TableEntry
from materializer_control_plane.lakehouse.rest import (
    CatalogError,
    commit,
    config_response,
    load_table_result,
    namespace_response,
    namespaces_response,
    no_such_namespace,
    no_such_table,
    not_supported,
    tables_response,
)
from materializer_control_plane.models import Dataset, MaterializationTable
from materializer_control_plane.settings import resolve_config
from materializer_control_plane.storage import get_materialization_table
from materializer_control_plane.storage.identity import find_dataset_by_name, get_table_claim, iter_datasets, iter_table_claims
from materializer_control_plane.storage.table import TableProtocol
from materializer_control_plane.web.errors import WebError

#: Longest a request waits on GCS before it answers 503.
CATALOG_DEADLINE_SECONDS = 30.0
#: The tables a namespace holds: those the runtime writes, and those kept read-only after their
#: topics were removed.
CATALOG_TABLE_STATES = frozenset({"active", "retired"})

_ERROR_TYPES = {
    400: "BadRequestException",
    401: "NotAuthorizedException",
    403: "ForbiddenException",
    404: "NoSuchTableException",
    409: "CommitFailedException",
    429: "TooManyRequestsException",
    503: "ServiceUnavailableException",
}


@contextmanager
def catalog_errors() -> Iterator[None]:
    """Answer a web error (a refused authorization, say) in the shape Iceberg clients parse."""
    try:
        yield
    except WebError as error:
        raise CatalogError(error.status_code, _ERROR_TYPES.get(error.status_code, "RESTException"), error.message) from error


@dataclass(slots=True)
class _Cached:
    fetched_at: float
    metadata_location: str
    payload: dict[str, Any]


#: Loaded tables by (dataset id, table name, table UUID): a name freed by a dropped table and taken
#: by another table never answers with the other's metadata.
_CACHE: dict[tuple[str, str, str], _Cached] = {}
_CACHE_LOCK = threading.Lock()


def reset_catalog_cache() -> None:
    """Forget every cached table. Tests use this between cases."""
    with _CACHE_LOCK:
        _CACHE.clear()


def catalog_config_view(*, settings_table: TableProtocol | None = None, config: ControlPlaneConfig | None = None) -> dict[str, Any]:
    _require_enabled(_config(settings_table, config))
    return config_response()


def catalog_namespaces_view(
    control_plane_table: TableProtocol | None = None,
    *,
    directory_table: TableProtocol | None = None,
    principal: Principal | None = None,
    parent: str | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> dict[str, Any]:
    """The datasets the caller may read, each a namespace."""
    _require_enabled(_config(settings_table, config))
    names = [dataset.name for dataset in iter_datasets(directory_table) if _readable(principal, dataset)]
    return namespaces_response(names, parent=parent or None)


def catalog_namespace_view(
    namespace: str,
    control_plane_table: TableProtocol | None = None,
    *,
    directory_table: TableProtocol | None = None,
    principal: Principal | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> dict[str, Any]:
    _require_enabled(_config(settings_table, config))
    dataset = _require_namespace(directory_table, principal, namespace)
    return namespace_response(dataset.name, properties={"description": dataset.description} if dataset.description else {})


def catalog_tables_view(
    namespace: str,
    control_plane_table: TableProtocol,
    *,
    directory_table: TableProtocol | None = None,
    principal: Principal | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> dict[str, Any]:
    _require_enabled(_config(settings_table, config))
    dataset = _require_namespace(directory_table, principal, namespace)
    records = _catalog_tables(control_plane_table, directory_table, dataset)
    return tables_response(dataset.name, [record.name for record in records])


async def catalog_load_table_view(
    namespace: str,
    name: str,
    control_plane_table: TableProtocol,
    *,
    directory_table: TableProtocol | None = None,
    principal: Principal | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
    client: Any = None,
) -> dict[str, Any]:
    """`LoadTableResult` for one table: its metadata location and metadata."""
    cfg = _require_enabled(_config(settings_table, config))
    _, record = _find_table(control_plane_table, directory_table, principal, namespace, name)
    key = (record.dataset_id, record.name, record.table_uuid)
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
    if cached is not None and time.monotonic() - cached.fetched_at < cfg.catalog_metadata_cache_seconds:
        return cached.payload
    catalog = _catalog(record, client=client, writer="catalog")

    def load() -> tuple[str, dict[str, Any]]:
        pointer, _ = catalog.load_pointer(record.identifier)
        if pointer is None:
            raise no_such_table(namespace, name)
        if cached is not None and cached.metadata_location == pointer.metadata_location:
            return pointer.metadata_location, cached.payload
        table = catalog.load_table(record.identifier)
        return table.metadata_location, load_table_result(table)

    metadata_location, payload = await _blocking(load, what=f"loading table {namespace}.{name}")
    with _CACHE_LOCK:
        _CACHE[key] = _Cached(fetched_at=time.monotonic(), metadata_location=metadata_location, payload=payload)
    return payload


async def catalog_commit_view(
    namespace: str,
    name: str,
    body: Any,
    control_plane_table: TableProtocol,
    *,
    committed_by: str,
    directory_table: TableProtocol | None = None,
    principal: Principal | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
    client: Any = None,
) -> dict[str, Any]:
    """Commit an engine's changes to a table; `409` when another commit landed first.

    Needs `catalog.write` on the table's dataset.
    """
    _require_enabled(_config(settings_table, config))
    dataset, record = _find_table(control_plane_table, directory_table, principal, namespace, name)
    if principal is not None:
        with catalog_errors():
            require(principal, "catalog.write", dataset=dataset)
    catalog = _catalog(record, client=client, writer=f"catalog:{committed_by}")
    response = await _blocking(lambda: commit(catalog, record.identifier, body), what=f"committing to {namespace}.{name}")
    with _CACHE_LOCK:
        _CACHE.pop((record.dataset_id, record.name, record.table_uuid), None)
    return response


def catalog_metrics_view(
    namespace: str,
    name: str,
    control_plane_table: TableProtocol,
    *,
    directory_table: TableProtocol | None = None,
    principal: Principal | None = None,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> None:
    """Engines report scan metrics here; they are accepted and dropped."""
    _require_enabled(_config(settings_table, config))
    _find_table(control_plane_table, directory_table, principal, namespace, name)
    return None


def catalog_not_supported_view(what: str) -> None:
    messages = {
        "create_table": "Tables are created by choosing a materialization's topics and the datasets they go in.",
        "drop_table": "Tables are dropped by removing their topics from their materialization, or deleting it.",
        "transactions": "Multi-table transactions are not supported.",
    }
    raise not_supported(messages.get(what, f"{what} is not supported."))


def _config(settings_table: TableProtocol | None, config: ControlPlaneConfig | None) -> ControlPlaneConfig:
    return resolve_config(settings_table) if config is None else config


def _require_enabled(cfg: ControlPlaneConfig) -> ControlPlaneConfig:
    if not cfg.catalog_api_enabled:
        raise CatalogError(503, "ServiceUnavailableException", "The Iceberg REST catalog is turned off in Settings.")
    return cfg


def _readable(principal: Principal | None, dataset: Dataset) -> bool:
    return principal is None or principal.can("catalog.read", dataset.dataset_id)


def _require_namespace(directory_table: TableProtocol | None, principal: Principal | None, namespace: str) -> Dataset:
    """The dataset a namespace names, compared without regard to case as engines do; one the caller
    cannot read answers as though it did not exist."""
    dataset = find_dataset_by_name(directory_table, namespace) if isinstance(namespace, str) else None
    if dataset is None or not _readable(principal, dataset):
        raise no_such_namespace(namespace)
    return dataset


def _catalog_tables(
    control_plane_table: TableProtocol,
    directory_table: TableProtocol | None,
    dataset: Dataset,
) -> list[MaterializationTable]:
    """The tables a dataset lists: written and kept ones, whatever materialization has each. A table
    whose name is claimed but which is not created yet, or one being dropped, is not there."""
    records = []
    for claim in iter_table_claims(directory_table, dataset.dataset_id):
        record = get_materialization_table(control_plane_table, claim.materialization_id, claim.table)
        if record is not None and record.state in CATALOG_TABLE_STATES and record.dataset_id == dataset.dataset_id:
            records.append(record)
    return records


def _find_table(
    control_plane_table: TableProtocol,
    directory_table: TableProtocol | None,
    principal: Principal | None,
    namespace: str,
    name: str,
) -> tuple[Dataset, MaterializationTable]:
    """A table by its SQL name: the dataset, the claim of its name, then its record."""
    dataset = _require_namespace(directory_table, principal, namespace)
    claim = get_table_claim(directory_table, dataset.dataset_id, name) if isinstance(name, str) else None
    record = None if claim is None else get_materialization_table(control_plane_table, claim.materialization_id, claim.table)
    if record is None or record.state not in CATALOG_TABLE_STATES or record.dataset_id != dataset.dataset_id:
        raise no_such_table(namespace, name)
    return dataset, record


def _catalog(record: MaterializationTable, *, client: Any, writer: str) -> GcsPointerCatalog:
    entry = TableEntry(identifier=record.identifier, location=record.location, materialization_id=record.materialization_id)
    return GcsPointerCatalog(directory=StaticTableDirectory([entry]), client=client, writer=writer)


async def _blocking(function, *, what: str):
    try:
        return await asyncio.wait_for(asyncio.to_thread(function), timeout=CATALOG_DEADLINE_SECONDS)
    except TimeoutError as error:
        raise CatalogError(503, "ServiceUnavailableException", f"{what} took longer than {CATALOG_DEADLINE_SECONDS:.0f}s") from error
