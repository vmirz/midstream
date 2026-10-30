"""Shared helpers for the lakehouse tests: a table in an object store, rows written as data files.

`LakeFixture` keeps its table in a `MemoryStore` unless it is given another `ObjectStore` - such as
`GcsStore(client=FakeGcs().client())`, to run the same test through the GCS store and the client
double behind it. It writes only through the store's contract, so any store will do.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import os
from pathlib import Path
import sys
import tempfile
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))

from midstream.lakehouse.catalog import PointerCatalog, StaticTableDirectory, TableEntry  # noqa: E402
from midstream.lakehouse.commits import AnnouncedFile, PendingFiles, commit_append, plan_append  # noqa: E402
from midstream.lakehouse.files import data_file_uri, partition_text, partition_values, protection_label_suffix  # noqa: E402
from midstream.lakehouse.offsets import committed_offsets  # noqa: E402
from midstream.lakehouse.parquet import table_from_rows, write_table  # noqa: E402
from midstream.lakehouse.rows import build_row  # noqa: E402
from midstream.lakehouse.schema.conform import Conformer  # noqa: E402
from midstream.lakehouse.schema.forkmap import ForkMap  # noqa: E402
from midstream.lakehouse.schema.infer import infer  # noqa: E402
from midstream.lakehouse.schema.policy import evolve  # noqa: E402
from midstream.lakehouse.schema.index import build_index  # noqa: E402
from midstream.lakehouse.store import MemoryStore, ObjectStore, scheme_of, set_store  # noqa: E402
from midstream.lakehouse.tables import (  # noqa: E402
    build_schema,
    new_table_metadata,
    table_properties,
)
from midstream.protection.plan import SHAPE_ONLY_SECRET, ProtectionPlan, initial_protection  # noqa: E402

MATERIALIZATION_ID = "mat-a1b2c3d4e5f6"
#: The dataset of the materialization's tables: their Iceberg namespace.
NAMESPACE = "shop"
#: Where the materialization's tables live; each is a directory below.
MATERIALIZATION_LOCATION = "gs://lake/materializer/mat-a1b2c3d4e5f6"
LOCATION = f"{MATERIALIZATION_LOCATION}/orders"
IDENTIFIER = (NAMESPACE, "orders")
BASE_TIME = datetime(2026, 10, 18, 10, 0, tzinfo=UTC)


class LakeFixture:
    """An object store holding a materializer table, and helpers to add data files to it.

    `store` is the fixture's `ObjectStore`: a new `MemoryStore` unless one is given. Several
    fixtures can share one store: a materialization's tables, one per topic. The catalog reads and
    writes through it, never through the process's stores; `install()` makes it the process's
    store for the location's scheme too, for code that finds its store by URI (`store_for`).
    """

    def __init__(
        self,
        *records,
        granularity: str = "day",
        location: str = LOCATION,
        identifier: tuple[str, str] = IDENTIFIER,
        source_topic: str = "orders",
        store: ObjectStore | None = None,
        materialization_id: str = MATERIALIZATION_ID,
        protection_policy=None,
        protection_revision: int = 1,
    ) -> None:
        """With `protection_policy`, the table is created as a materialization of this release creates
        it: schema 0 from masked samples, and its `materializer.protection` property (design §5.5)."""
        self.store: ObjectStore = MemoryStore() if store is None else store
        self.location = location
        self.identifier = identifier
        self.materialization_id = materialization_id
        self.entry = TableEntry(identifier=identifier, location=location, materialization_id=materialization_id)
        self.directory = StaticTableDirectory([self.entry])
        self.catalog = PointerCatalog(directory=self.directory, store=self.store, writer="test")
        self.granularity = granularity
        if protection_policy is not None:
            plan = ProtectionPlan.for_policy(protection_policy, secret=SHAPE_ONLY_SECRET)
            records = tuple(plan.protect(record) for record in records)
        evolution = evolve(build_schema(), ForkMap(), [((), infer(r)) for r in records], schema_id=0)
        protection_json = None
        if protection_policy is not None:
            index = build_index(evolution.schema, evolution.fork_map, last_column_id=evolution.last_column_id)
            protection_json = initial_protection(
                index, protection_policy, revision=protection_revision, at="2026-10-18T00:00:00Z", by="tester@example.com"
            ).to_json()
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
            protection_json=protection_json,
        )
        metadata = new_table_metadata(location=location, schema=evolution.schema, granularity=granularity, properties=properties)
        self.table = self.catalog.create_table_from_metadata(
            identifier, location=location, materialization_id=materialization_id, metadata=metadata
        )
        self._tmp = tempfile.TemporaryDirectory()
        self._installed: str | None = None

    def install(self) -> None:
        """Make the fixture's store the process's store for its location's scheme (`set_store`), for
        code that finds its store by URI - a committer, the compaction worker, the tail's seed.
        `close()` removes it again."""
        self._installed = scheme_of(self.location)
        set_store(self._installed, self.store)

    def close(self) -> None:
        if self._installed is not None:
            set_store(self._installed, None)
            self._installed = None
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
        return commit_append(table, plan, runtime="test-runtime", store=self.store)

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
        protection_revision: int = 0,
        protection_label: tuple[int, ...] | None = None,
    ) -> list[AnnouncedFile]:
        """Write a seal of rows at `offsets` (split by partition day) and return its announcements;
        its files' names carry `protection_revision`, as a writer's do - or `protection_label`, as a
        file `reprotect` rewrote does."""
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
                protection_revision=protection_revision,
            )
            if protection_label is not None:
                uri = uri[: -len(".parquet")] + protection_label_suffix(protection_label) + ".parquet"
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
