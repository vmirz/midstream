"""The keys a provider signs ID tokens with, fetched from its key set and kept.

A token naming a key not seen yet makes the set be read again - a provider rotates its keys that way
- but at most once a minute, so a stream of tokens naming made-up keys cannot make this process
hammer the provider.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import time

import jwt

from midstream.identity.providers.http import ProviderHttp, ProviderUnreachable

#: The shortest time between two reads of one key set.
REFRESH_INTERVAL_SECONDS = 60
#: How long a key set read is trusted before it is read again even for a known key.
MAX_AGE_SECONDS = 6 * 3600


@dataclass
class KeySet:
    """One provider's keys, by key id."""

    jwks_uri: str
    keys: dict[str, jwt.PyJWK] = field(default_factory=dict)
    fetched_at: float | None = None

    async def key(self, kid: str, http: ProviderHttp, *, now: float | None = None) -> jwt.PyJWK | None:
        """The key with id `kid`, reading the set again when it is unknown and a minute has passed."""
        current = time.time() if now is None else now
        stale = self.fetched_at is None or current - self.fetched_at > MAX_AGE_SECONDS
        if kid in self.keys and not stale:
            return self.keys[kid]
        if self.fetched_at is None or current - self.fetched_at >= REFRESH_INTERVAL_SECONDS or stale:
            await self._refresh(http, now=current)
        return self.keys.get(kid)

    async def _refresh(self, http: ProviderHttp, *, now: float) -> None:
        answer = await http.request("GET", self.jwks_uri)
        if answer.status != 200 or not isinstance(answer.body, dict):
            raise ProviderUnreachable(f"{self.jwks_uri} answered {answer.status}")
        keys: dict[str, jwt.PyJWK] = {}
        for item in answer.body.get("keys") or []:
            if not isinstance(item, dict) or not isinstance(item.get("kid"), str):
                continue
            if item.get("use") not in (None, "sig"):
                continue
            try:
                keys[item["kid"]] = jwt.PyJWK(item)
            except (jwt.PyJWKError, jwt.InvalidKeyError, ValueError, TypeError):
                # A key of a kind this library cannot use signs nothing it is asked to check.
                continue
        self.keys = keys
        self.fetched_at = now


_KEY_SETS: dict[str, KeySet] = {}


def key_set(jwks_uri: str) -> KeySet:
    """The process's key set for one URI."""
    found = _KEY_SETS.get(jwks_uri)
    if found is None:
        found = _KEY_SETS[jwks_uri] = KeySet(jwks_uri)
    return found


def forget_key_sets() -> None:
    """Drop every key set read, as a test does between cases."""
    _KEY_SETS.clear()
