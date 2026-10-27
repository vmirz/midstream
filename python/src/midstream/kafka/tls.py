"""The TLS context of Midstream's own Kafka clients: the one Kaspr's clients use.

A KasprApp's `tls` turns TLS on for the listener: the operator sets `K_KAFKA_SECURITY_PROTOCOL` to
`SSL`, or to `SASL_SSL` with authentication, and Kaspr hands its clients
`ssl.create_default_context()` for either (`kaspr/types/settings.py`). aiokafka refuses to make a
client for those protocols without a context, so the topic reads and cleanup, schema discovery, the
lag refresh and the tail's consumers take the same one, and trust exactly the brokers Kaspr's
clients trust.
"""

from __future__ import annotations

from functools import lru_cache
import ssl

#: The security protocols that carry TLS.
TLS_PROTOCOLS = frozenset({"SSL", "SASL_SSL"})


def kafka_ssl_context(security_protocol: str | None) -> ssl.SSLContext | None:
    """The TLS context aiokafka needs for `security_protocol`, or None for a plaintext one."""
    if (security_protocol or "").strip().upper() not in TLS_PROTOCOLS:
        return None
    return _default_context()


@lru_cache(maxsize=1)
def _default_context() -> ssl.SSLContext:
    # One per process: making it reads the whole trust store, and a context serves any number of
    # connections, on any loop.
    return ssl.create_default_context()
