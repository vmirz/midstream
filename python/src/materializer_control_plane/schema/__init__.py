"""Schema discovery: sampling a source topic to build a table's first schema."""

from materializer_control_plane.schema.discovery import KafkaTopicDiscoveryService, StubDiscoveryService

__all__ = [
	"KafkaTopicDiscoveryService",
	"StubDiscoveryService",
]
