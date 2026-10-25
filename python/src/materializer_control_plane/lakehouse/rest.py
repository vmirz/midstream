"""The Iceberg REST catalog protocol, as the control plane serves it (spec §5.6).

Pure protocol: the shapes of responses and errors, and committing a `CommitTableRequest` through
the GCS-pointer catalog. `web/catalog.py` serves it. What the facade offers is deliberately small:
every dataset is a namespace whose tables can be listed, loaded and - with `catalog.write` -
committed to; tables are created and dropped by choosing and removing materializations' topics, so
those endpoints, and creating namespaces and multi-table transactions, answer 501.
"""

from __future__ import annotations

import json
from typing import Any

from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException, NoSuchTableError
from pyiceberg.table import CommitTableRequest, Table, TableProperties
from pyiceberg.table.update import SetLocationUpdate, SetPropertiesUpdate

from materializer_control_plane.lakehouse.catalog import ForeignTableError, GcsPointerCatalog

#: Every endpoint the facade answers, as `GET /v1/config` lists them. Clients use the list to tell
#: what they may call; anything else they do not try.
ENDPOINTS = (
    "GET /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
)


#: Properties that would put a table's files somewhere other than under its location. The location
#: is under the materialization's: the runtime, the console and the drop all work under it.
LOCATION_PROPERTIES = frozenset(
    {TableProperties.WRITE_DATA_PATH, TableProperties.WRITE_METADATA_PATH, TableProperties.WRITE_PY_LOCATION_PROVIDER_IMPL}
)


class CatalogError(Exception):
    """An error answered the way the Iceberg REST specification describes, which clients parse."""

    def __init__(self, status_code: int, error_type: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = status_code
        self.error_type = error_type
        self.message = message

    def to_response(self) -> dict[str, Any]:
        return {"error": {"message": self.message, "type": self.error_type, "code": self.status_code}}


def no_such_namespace(namespace: str) -> CatalogError:
    return CatalogError(404, "NoSuchNamespaceException", f"Namespace does not exist: {namespace}")


def no_such_table(namespace: str, name: str) -> CatalogError:
    return CatalogError(404, "NoSuchTableException", f"Table does not exist: {namespace}.{name}")


def not_supported(message: str) -> CatalogError:
    return CatalogError(501, "NotImplementedException", message)


def config_response() -> dict[str, Any]:
    return {"defaults": {}, "overrides": {}, "endpoints": list(ENDPOINTS)}


def namespaces_response(namespaces: list[str], *, parent: str | None = None) -> dict[str, Any]:
    # One level: a dataset has no children.
    return {"namespaces": [] if parent else [[namespace] for namespace in sorted(namespaces, key=str.lower)], "next-page-token": None}


def namespace_response(namespace: str, *, properties: dict[str, str] | None = None) -> dict[str, Any]:
    return {"namespace": [namespace], "properties": dict(properties or {})}


def tables_response(namespace: str, names: list[str]) -> dict[str, Any]:
    return {
        "identifiers": [{"namespace": [namespace], "name": name} for name in sorted(names)],
        "next-page-token": None,
    }


def metadata_json(table: Table) -> dict[str, Any]:
    """A table's metadata as Iceberg serializes it (field names as in `metadata.json`)."""
    return json.loads(table.metadata.model_dump_json(by_alias=True, exclude_none=True))


def load_table_result(table: Table) -> dict[str, Any]:
    return {"metadata-location": table.metadata_location, "metadata": metadata_json(table), "config": {}}


def commit(catalog: GcsPointerCatalog, identifier: tuple[str, str], body: Any) -> dict[str, Any]:
    """Apply a `CommitTableRequest` with the pointer's compare-and-swap; 409 when it lost a race.

    The runtime's committer refreshes before every commit, so an external commit is simply the
    table's newest state the next time it commits.
    """
    if not isinstance(body, dict):
        raise CatalogError(400, "BadRequestException", "The request body must be a CommitTableRequest")
    try:
        # The path names the table; an identifier in the body is not trusted over it.
        request = CommitTableRequest.model_validate({**body, "identifier": {"namespace": list(identifier[:-1]), "name": identifier[-1]}})
    except Exception as error:  # noqa: BLE001 - pydantic's own error says what is wrong
        raise CatalogError(400, "BadRequestException", f"Not a valid commit: {error}") from error
    _refuse_moving_files(request.updates)
    try:
        table = catalog.load_table(identifier)
        response = catalog.commit_table(table, request.requirements, request.updates)
    except NoSuchTableError as error:
        raise no_such_table(identifier[0], identifier[-1]) from error
    except ForeignTableError as error:
        raise CatalogError(409, "CommitFailedException", str(error)) from error
    except CommitFailedException as error:
        raise CatalogError(409, "CommitFailedException", str(error)) from error
    except CommitStateUnknownException as error:
        raise CatalogError(503, "CommitStateUnknownException", str(error)) from error
    return {"metadata-location": response.metadata_location, "metadata": json.loads(response.metadata.model_dump_json(by_alias=True, exclude_none=True))}


def _refuse_moving_files(updates) -> None:
    """A commit may change what a table holds, never where its files are."""
    for update in updates:
        if isinstance(update, SetLocationUpdate):
            raise CatalogError(400, "BadRequestException", "A materialization's table cannot be moved: its location is fixed")
        if isinstance(update, SetPropertiesUpdate):
            moved = sorted(LOCATION_PROPERTIES & set(update.updates))
            if moved:
                raise CatalogError(
                    400,
                    "BadRequestException",
                    f"{moved[0]} cannot be set: a materialization's table keeps its files under its location",
                )
