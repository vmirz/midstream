"""Schema discovery: sampling a source topic to build a table's first schema."""

from midstream.schema.discovery import KafkaTopicDiscoveryService, StubDiscoveryService

__all__ = [
	"KafkaTopicDiscoveryService",
	"StubDiscoveryService",
]
