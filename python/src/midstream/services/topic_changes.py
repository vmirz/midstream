"""Changing a materialization's topics: the tables it writes, keeps and drops afterwards.

An update names the topics to read from now on, in order; for each topic new to the
materialization, the table it gets - its dataset and name, chosen with the request; and for each
table whose topic it removes whether to `drop` it, data and all, or `keep` it, read-only.
`plan_topic_change` turns that into the materialization's tables, against the materialization and
its table records as they are:

- a topic it already reads keeps its table, in its dataset and under its name;
- the topic of a kept table writes that table again: it resumes where it stopped, in its dataset
  and under its name;
- any other topic gets the table the request chose for it, whose name must differ from every name
  the materialization holds - the written tables', the kept ones', those still being dropped and
  those dropped with their files kept, whatever their datasets, since each is a directory of its
  own under the materialization's location;
- a table whose topic is removed is kept or dropped as the request says, and a kept table can be
  dropped too.

The planner is pure. The update view runs it to answer the request at once, and request intake runs
it again when it applies the request, against the records as they are then, so two concurrent
updates never apply a plan made for a materialization that has changed since. A plan that adds
tables waits for their names to be claimed in their datasets before it applies
(`PendingTopicChange`).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace

from midstream.models import (
    Materialization,
    MaterializationTable,
    TableSpec,
    TopicChangePlan,
    TopicChangeRequest,
)
from midstream.storage import list_materialization_tables, save_materialization_table
from midstream.storage.table import TableProtocol
from midstream.table_names import (
    InvalidSourceTopicsError,
    normalize_source_topics,
    table_name_problem,
)

__all__ = ["TopicChangeError", "TopicChangePlan", "apply_topic_change", "describe_topic_change", "plan_topic_change"]


class TopicChangeError(ValueError):
    """Raised when an update's topics, or what it says becomes of a table, cannot be applied."""

    def __init__(self, message: str, *, field_name: str = "tables") -> None:
        super().__init__(message)
        self.field_name = field_name


def plan_topic_change(
    materialization: Materialization,
    records: Iterable[MaterializationTable],
    request: TopicChangeRequest,
    *,
    maximum: int,
) -> TopicChangePlan:
    """The materialization's tables once `request` is applied; raises `TopicChangeError` if it cannot be."""
    try:
        topics = normalize_source_topics(list(request.source_topics), maximum=maximum)
    except InvalidSourceTopicsError as error:
        raise TopicChangeError(str(error)) from error
    records = list(records)
    written = {spec.source_topic: spec for spec in materialization.tables}
    kept = {spec.source_topic: spec for spec in materialization.retired_tables}
    by_name = {spec.name.lower(): spec for spec in materialization.all_tables}
    # A request applied again - redelivered - finds the tables it dropped being dropped, or dropped.
    dropping = {record.name.lower() for record in records if record.state in {"dropping", "dropped"}}
    actions: dict[str, str] = {}
    for name, action in request.table_actions.items():
        spec = by_name.get(name.lower())
        if spec is None and name.lower() in dropping:
            continue
        if spec is None:
            known = ", ".join(spec.name for spec in materialization.all_tables)
            raise TopicChangeError(f"there is no table {name}; the tables are {known}", field_name="removed_tables")
        if spec.source_topic in written and spec.source_topic in topics:
            raise TopicChangeError(
                f"table {spec.name} still reads topic {spec.source_topic}: only a table whose topic is "
                "removed, or a kept table, can be dropped or kept",
                field_name="removed_tables",
            )
        if spec.source_topic in kept and spec.source_topic in topics and action == "drop":
            raise TopicChangeError(
                f"adding topic {spec.source_topic} back writes its kept table {spec.name} again, so it "
                "cannot be dropped in the same change; drop it in a change of its own first",
                field_name="removed_tables",
            )
        actions[spec.name] = action

    for spec in request.new_tables:
        existing = written.get(spec.source_topic) or kept.get(spec.source_topic)
        if existing is not None and (existing.dataset_id != spec.dataset_id or existing.name.lower() != spec.name.lower()):
            raise TopicChangeError(
                f"topic {spec.source_topic} has table {existing.qualified_name} already, and a table's "
                "dataset and name never change; to put the topic in another dataset, drop its table first"
            )

    taken = {spec.name.lower() for spec in materialization.all_tables} | {record.name.lower() for record in records}
    tables: list[TableSpec] = []
    added: list[TableSpec] = []
    resumed: list[TableSpec] = []
    for topic in topics:
        if topic in written:
            tables.append(written[topic])
        elif topic in kept:
            tables.append(kept[topic])
            resumed.append(kept[topic])
        else:
            spec = request.new_table(topic)
            if spec is None:
                raise TopicChangeError(f"choose the dataset and name of topic {topic}'s table")
            problem = table_name_problem(spec.name)
            if problem is not None:
                raise TopicChangeError(f"topic {topic}: {problem}")
            if spec.name.lower() in taken:
                raise TopicChangeError(
                    f"this materialization already has a table named {spec.name}; choose another name for "
                    f"topic {topic}'s table"
                )
            taken.add(spec.name.lower())
            tables.append(spec)
            added.append(spec)

    removed: list[TableSpec] = []
    retired: list[TableSpec] = []
    dropped: list[TableSpec] = []
    for spec in materialization.tables:
        if spec.source_topic in topics:
            continue
        action = actions.get(spec.name)
        if action is None:
            raise TopicChangeError(
                f"say whether to drop or keep table {spec.name}: its topic {spec.source_topic} is removed",
                field_name="removed_tables",
            )
        removed.append(spec)
        (retired if action == "keep" else dropped).append(spec)
    retired_tables: list[TableSpec] = []
    for spec in materialization.retired_tables:
        if spec.source_topic in topics:
            continue
        if actions.get(spec.name) == "drop":
            dropped.append(spec)
        else:
            retired_tables.append(spec)
    retired_tables.extend(retired)
    return TopicChangePlan(
        tables=tuple(tables),
        retired_tables=tuple(retired_tables),
        added=tuple(added),
        resumed=tuple(resumed),
        removed=tuple(removed),
        retired=tuple(retired),
        dropped=tuple(dropped),
    )


def apply_topic_change(
    table: TableProtocol,
    materialization: Materialization,
    plan: TopicChangePlan,
    *,
    now: str,
    spec_version_before: int,
) -> Materialization:
    """The materialization with `plan` applied, and each of its table records in its new state.

    The materialization takes the plan's written and kept tables; each existing table record takes
    its state: written again, kept, or being dropped - which takes it out of the catalog. Nothing
    is saved but the table records: the caller saves the materialization. Applying a plan applied
    already changes nothing, so a redelivered request does no harm.
    """
    states = {spec.name: "active" for spec in plan.tables}
    states.update({spec.name: "retired" for spec in plan.retired_tables})
    states.update({spec.name: "dropping" for spec in plan.dropped})
    for record in list_materialization_tables(table, materialization.materialization_id):
        state = states.get(record.name)
        if state is not None and state != record.state:
            save_materialization_table(table, replace(record, state=state, updated_at=now))
    if plan.tables == materialization.tables and plan.retired_tables == materialization.retired_tables:
        return materialization
    # One request is one version of the spec, whether it changes the replicas, the topics or both.
    return replace(
        materialization,
        tables=plan.tables,
        retired_tables=plan.retired_tables,
        spec_version=spec_version_before + 1,
        updated_at=now,
    )


def describe_topic_change(plan: TopicChangePlan) -> str:
    """The status message a change of topics shows until the runtime is applied again."""
    parts = []
    if plan.added:
        parts.append("adding " + ", ".join(f"{spec.source_topic} as {spec.qualified_name}" for spec in plan.added))
    if plan.resumed:
        parts.append("writing kept " + ", ".join(spec.qualified_name for spec in plan.resumed) + " again")
    if plan.retired:
        parts.append("keeping " + ", ".join(spec.qualified_name for spec in plan.retired) + " read-only")
    if plan.dropped:
        parts.append("dropping " + ", ".join(spec.qualified_name for spec in plan.dropped))
    return "Changing topics: " + "; ".join(parts)
