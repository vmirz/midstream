"""Protocols for the KasprTable-like storage adapter."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol


class TableProtocol(Protocol):
    def get(self, key: str) -> dict[str, Any] | None:
        ...

    def __setitem__(self, key: str, value: dict[str, Any]) -> None:
        ...

    def __delitem__(self, key: str) -> None:
        ...

    def prefix_scan(self, prefix: str) -> Iterable[tuple[str, dict[str, Any]]]:
        ...