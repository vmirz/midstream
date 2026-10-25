"""Shared helpers for the lakehouse tests: a table on the fake GCS, rows written as data files."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import os
from pathlib import Path
import sys
import tempfile
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))

from gcs_fakes import FakeGcs  # noqa: E402

from midstream.lakehouse.catalog import GcsPointerCatalog, StaticTableDirectory, TableEntry  # noqa: E402
from midstream.lakehouse.commits import AnnouncedFile, PendingFiles, commit_append, plan_append  # noqa: E402
from midstream.lakehouse.files import data_file_uri, partition_text, partition_values  # noqa: E402
from midstream.lakehouse.offsets import committed_offsets  # noqa: E402
from midstream.lakehouse.parquet import table_from_rows, write_table  # noqa: E402
from midstream.lakehouse.rows import build_row  # noqa: E402
from midstream.lakehouse.schema.conform import Conformer  # noqa: E402
from midstream.lakehouse.schema.forkmap import ForkMap  # noqa: E402
from midstream.lakehouse.schema.infer import infer  # noqa: E402
from midstream.lakehouse.schema.policy import evolve  # noqa: E402
from midstream.lakehouse.tables import (  # noqa: E402
    build_schema,
    new_table_metadata,
    table_properties,
)

MATERIALIZATION_ID = "mat-a1b2c3d4e5f6"
#: The dataset of the materialization's tables: their Iceberg namespace.
NAMESPACE = "shop"
#: Where the materialization's tables live; each is a directory below.
MATERIALIZATION_LOCATION = "gs://lake/materializer/mat-a1b2c3d4e5f6"
LOCATION = f"{MATERIALIZATION_LOCATION}/orders"
IDENTIFIER = (NAMESPACE, "orders")
BASE_TIME = datetime(2026, 10, 18, 10, 0, tzinfo=UTC)


class LakeFixture:
    """A fake bucket holding a materializer table, and helpers to add data files to it.

    Several fixtures can share one bucket (`store`): a materialization's tables, one per topic.
    """

    def __init__(
        self,
        *records,
        granularity: str = "day",
        location: str = LOCATION,
        identifier: tuple[str, str] = IDENTIFIER,
        source_topic: str = "orders",
        store: FakeGcs | None = None,
        materialization_id: str = MATERIALIZATION_ID,
    ) -> None:
        self.store = FakeGcs() if store is None else store
        self.client = self.store.client()
        self.location = location
        self.identifier = identifier
        self.materialization_id = materialization_id
        self.entry = TableEntry(identifier=identifier, location=location, materialization_id=materialization_id)
        self.directory = StaticTableDirectory([self.entry])
        self.catalog = GcsPointerCatalog(directory=self.directory, client=self.client, writer="test")
        self.granularity = granularity
        evolution = evolve(build_schema(), ForkMap(), [((), infer(r)) for r in records], schema_id=0)
        properties = table_properties(
            materialization_id=materialization_id,
            source_topic=source_topic,
            partition_granularity=granularity,
            created_by="tester@example.com",
            created_at="2026-10-18T00:00:00Z",
            package_version="1.0.0",
            snapshot_retention_hours=72,
            min_snapshots_to_keep=20,
            fork_map_json=evolution.fork_map.to_json(),
        )
        metadata = new_table_metadata(location=location, schema=evolution.schema, granularity=granularity, properties=properties)
        self.table = self.catalog.create_table_from_metadata(
            identifier, location=location, materialization_id=materialization_id, metadata=metadata
        )
        self._tmp = tempfile.TemporaryDirectory()

    def close(self) -> None:
        self._tmp.cleanup()

    def load(self):
        return self.catalog.load_table(self.identifier)

    def commit(self, *announced_lists: list[AnnouncedFile]):
        """Commit seals as the committer would (one append), and return the result."""
        pending = PendingFiles()
        for announced in announced_lists:
            for item in announced:
                pending.add(item)
        table = self.load()
        plan = plan_append(pending.seals(), committed_offsets(table.metadata), max_files=10_000, max_bytes=1 << 50)
        return commit_append(table, plan, runtime="test-runtime", client=self.client)

    def write_seal(
        self,
        *,
        partition: int,
        after_offset: int,
        offsets: list[int],
        values: list[dict] | None = None,
        member: str = "member-0",
        generation: int | None = 1,
        epoch: str | None = None,
        times: list[datetime] | None = None,
        table=None,
    ) -> list[AnnouncedFile]:
        """Write a seal of rows at `offsets` (split by partition day) and return its announcements."""
        table = table or self.load()
        schema = table.schema()
        fork_map = ForkMap.from_json(table.metadata.properties.get("materializer.fork-map"))
        conformer = Conformer(schema, fork_map)
        values = values or [{"order_id": f"O-{offset}"} for offset in offsets]
        times = times or [BASE_TIME + timedelta(seconds=offset) for offset in offsets]
        groups: dict[str | None, list[dict]] = {}
        for offset, value, moment in zip(offsets, values, times, strict=True):
            # The row the seal would write for the line the writer would stage (`lakehouse/rows.py`).
            staged = {
                "m": {"p": partition, "o": offset, "t": _iso(moment), "k": f"K{offset}", "h": [["event-type", "test"]], "a": _iso(BASE_TIME)},
                "v": value,
            }
            row = build_row(staged, conformer=conformer, default_partition=partition).row
            groups.setdefault(partition_text(self.granularity, moment), []).append(row)
        seal_id = uuid.uuid4().hex
        batch_id = f"batch-{member}-p{partition}-o{offsets[0]}"
        announced = []
        for text, rows in groups.items():
            arrow = table_from_rows(rows, schema)
            path = os.path.join(self._tmp.name, f"{uuid.uuid4().hex}.parquet")
            size = write_table(arrow, path)
            uri = data_file_uri(
                self.location,
                granularity=self.granularity,
                partition_value_text=text,
                kafka_partition=partition,
                offset_start=offsets[0],
                offset_end=offsets[-1],
                member_id=member,
                unique=uuid.uuid4().hex[:12],
            )
            self.store.put(uri, Path(path).read_bytes())
            stamps = sorted(row["_kafka_timestamp"] for row in rows)
            announced.append(
                AnnouncedFile(
                    file_path=uri,
                    materialization_id=self.materialization_id,
                    table=self.identifier[1],
                    member_id=member,
                    batch_id=batch_id,
                    seal_id=seal_id,
                    file_count=len(groups),
                    partition=partition,
                    after_offset=after_offset,
                    offset_start=offsets[0],
                    offset_end=offsets[-1],
                    file_size=size,
                    record_count=len(rows),
                    partition_value=partition_values(self.granularity, text),
                    event_time_min=_iso(stamps[0]),
                    event_time_max=_iso(stamps[-1]),
                    schema_id=schema.schema_id,
                    session_generation=generation,
                    session_epoch=epoch,
                )
            )
        return announced


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
