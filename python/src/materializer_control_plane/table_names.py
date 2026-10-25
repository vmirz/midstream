"""Dataset and table names, the names SQL reads tables by, and materialization labels.

A table's SQL name is `<dataset>.<table>`: engines on the REST catalog read
`lake.<dataset>.<table>`, and the console reads `<dataset>.<table>`. Both parts follow one rule, so
a person can type them unquoted in any engine: a lowercase letter first, then lowercase letters,
digits and underscores, at most 63 characters, and no word SQL reserves. Names are lowercase only
because Trino folds unquoted names to lowercase and DuckDB and Spark compare them without regard to
case; with one spelling per name, every engine and the directory's keys agree.

- A **dataset's name** is chosen when the dataset is created and never changes. A dataset is the
  tables' Iceberg namespace, so it also cannot be one of DuckDB's own catalogs or schemas.
- A **table's name** is chosen for each topic when the topic is picked; it defaults to a name
  derived from the topic (`table_name_for_topic`), `orders.v2` → `orders_v2`. It is unique in its
  dataset - the directory enforces that - and in its materialization, and never changes.
- A **materialization's name** is a label: free text that defaults to its id, not unique, and of
  no meaning in SQL or storage. It can be changed at any time.
"""

from __future__ import annotations

from collections.abc import Iterable
import re
from typing import Any

#: The longest dataset name.
DATASET_NAME_MAX_LENGTH = 63
#: The longest table name; a longer derivation from a topic is cut, keeping it unique.
TABLE_NAME_MAX_LENGTH = 63
#: The longest materialization label.
MATERIALIZATION_LABEL_MAX_LENGTH = 128
#: Kafka's own rule for a topic name: letters, digits, `.`, `_` and `-`, at most 249 characters,
#: and neither `.` nor `..`.
_TOPIC_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,249}$")
#: What a derived table name keeps of a topic name; everything else becomes `_`.
_TABLE_NAME_UNSAFE = re.compile(r"[^a-z0-9]+")

#: A lowercase letter first, then lowercase letters, digits and underscores. A leading digit does
#: not parse as a table name; `$`, which DuckDB also accepts, is left out because SQL front ends read
#: it as the start of a dollar-quoted string.
_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
#: Characters a label cannot hold.
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")

#: DuckDB's `reserved` and `type_function` keywords (`duckdb_keywords()`, DuckDB 1.5). An unquoted
#: table name cannot be one of these: `SELECT * FROM order` is a syntax error.
DUCKDB_RESERVED_KEYWORDS = frozenset(
    {
        "all", "analyse", "analyze", "and", "anti", "any", "array", "as", "asc", "asof",
        "asymmetric", "at", "authorization", "binary", "both", "by", "case", "cast", "check",
        "collate", "collation", "column", "columns", "concurrently", "constraint", "create",
        "cross", "default", "deferrable", "desc", "describe", "distinct", "do", "else", "end",
        "except", "false", "fetch", "for", "foreign", "freeze", "from", "full", "generated",
        "glob", "group", "having", "ilike", "in", "initially", "inner", "intersect", "into", "is",
        "isnull", "join", "lambda", "lateral", "leading", "left", "like", "limit", "map",
        "natural", "not", "notnull", "null", "offset", "on", "only", "or", "order", "outer",
        "overlaps", "pivot", "pivot_longer", "pivot_wider", "placing", "positional", "primary",
        "qualify", "references", "returning", "right", "select", "semi", "show", "similar",
        "some", "struct", "summarize", "symmetric", "table", "tablesample", "then", "to",
        "trailing", "true", "try_cast", "union", "unique", "unpack", "unpivot", "using",
        "variadic", "verbose", "when", "where", "window", "with",
    }
)

#: Statement verbs and file-reading functions. DuckDB accepts these as table names, but read-only
#: SQL front ends — the Materializer Console among them — refuse a query that mentions one, so a
#: table named after one could never be queried there.
STATEMENT_KEYWORDS = frozenset(
    {
        "alter", "analyze", "attach", "avro_scan", "begin", "call", "checkpoint", "commit",
        "copy", "create", "delete", "detach", "drop", "export", "glob", "grant", "iceberg_metadata",
        "iceberg_scan", "iceberg_snapshots", "iceberg_table_properties", "import", "insert",
        "install", "load", "pragma", "read_avro", "read_blob", "read_csv", "read_json",
        "read_parquet", "read_text", "reset", "revoke", "rollback", "set", "shell", "sniff_csv",
        "system", "truncate", "update", "vacuum",
    }
)

#: Names the console's SQL surface gives a meaning of its own: `iceberg` is the schema holding each
#: native table (`FROM iceberg.orders`), and `catalog` and `lake` are what the console's Connect
#: snippets attach the REST catalog as. A table by one of these names would be shadowed.
ICEBERG_KEYWORDS = frozenset({"iceberg", "catalog", "lake"})

#: What a table cannot be named: it must stand unquoted in a statement.
RESERVED_TABLE_NAMES = DUCKDB_RESERVED_KEYWORDS | STATEMENT_KEYWORDS | ICEBERG_KEYWORDS

#: The catalogs and schemas DuckDB names itself. A dataset by one of these would be ambiguous in
#: `<dataset>.<table>` (`memory`, `temp`), or would shadow DuckDB's own (`information_schema`).
DUCKDB_CATALOG_NAMES = frozenset(
    {"main", "memory", "temp", "temporary", "system", "information_schema", "pg_catalog"}
)

#: What a dataset cannot be named: a table's reservations, and DuckDB's own namespaces.
RESERVED_DATASET_NAMES = RESERVED_TABLE_NAMES | DUCKDB_CATALOG_NAMES

NAME_RULES = "start with a lowercase letter and use only lowercase letters, digits and underscores"


class InvalidNameError(ValueError):
    """Raised when a requested dataset or table name, or a label, cannot be taken."""

    def __init__(self, message: str, *, field_name: str) -> None:
        super().__init__(message)
        self.field_name = field_name


class InvalidSourceTopicsError(ValueError):
    """Raised when a materialization's requested source topics cannot be materialized."""

    field_name = "source_topics"


def dataset_name_problem(value: Any) -> str | None:
    """Why `value` cannot name a dataset, or None when it can."""
    return _name_problem(value, what="dataset", maximum=DATASET_NAME_MAX_LENGTH, reserved=RESERVED_DATASET_NAMES)


def table_name_problem(value: Any) -> str | None:
    """Why `value` cannot name a table, or None when it can."""
    return _name_problem(value, what="table", maximum=TABLE_NAME_MAX_LENGTH, reserved=RESERVED_TABLE_NAMES)


def normalize_dataset_name(value: Any, *, field_name: str = "name") -> str:
    """A requested dataset name, without surrounding space; raises `InvalidNameError`."""
    problem = dataset_name_problem(value)
    if problem is not None:
        raise InvalidNameError(problem, field_name=field_name)
    return str(value).strip()


def normalize_table_name(value: Any, *, field_name: str = "name") -> str:
    """A requested table name, without surrounding space; raises `InvalidNameError`."""
    problem = table_name_problem(value)
    if problem is not None:
        raise InvalidNameError(problem, field_name=field_name)
    return str(value).strip()


def normalize_materialization_label(value: Any, *, field_name: str = "name") -> str | None:
    """A materialization's label: trimmed, runs of space as one, 1 to 128 characters, no control
    characters. None when none is given - the materialization is then labelled with its id."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if not isinstance(value, str):
        raise InvalidNameError("name must be text", field_name=field_name)
    if _CONTROL.search(value):
        raise InvalidNameError("name contains characters that are not allowed", field_name=field_name)
    label = " ".join(value.split())
    if len(label) > MATERIALIZATION_LABEL_MAX_LENGTH:
        raise InvalidNameError(
            f"name must be at most {MATERIALIZATION_LABEL_MAX_LENGTH} characters", field_name=field_name
        )
    return label


def _name_problem(value: Any, *, what: str, maximum: int, reserved: frozenset[str]) -> str | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return f"a {what} name is required"
    if not isinstance(value, str):
        return f"a {what} name must be text"
    name = value.strip()
    if len(name) > maximum:
        return f"a {what} name is at most {maximum} characters"
    if _NAME_PATTERN.match(name) is None:
        if name.lower() != name and _NAME_PATTERN.match(name.lower()) is not None:
            return f'{what} names are lowercase only: "{name.lower()}", not "{name}"'
        return f'"{name}" cannot name a {what}: {NAME_RULES}'
    if name in reserved:
        if name in DUCKDB_CATALOG_NAMES:
            return f'"{name}" is a name DuckDB gives its own catalogs and schemas; choose another'
        return f'"{name}" is a reserved SQL word and cannot name a {what}; choose another'
    return None


def normalize_source_topics(value: Any, *, maximum: int) -> tuple[str, ...]:
    """Validate requested source topics: a list of names, or one comma-separated string.

    Surrounding space and repeats are dropped; the order given is kept, since it orders the
    materialization's tables and decides which one takes a suffix when two derive the same name.
    """
    if isinstance(value, str):
        candidates: list[Any] = value.split(",")
    elif isinstance(value, (list, tuple)):
        candidates = list(value)
    elif value is None:
        raise InvalidSourceTopicsError("source_topics is required: name at least one topic")
    else:
        raise InvalidSourceTopicsError("source_topics must be a list of topic names")
    topics: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, str):
            raise InvalidSourceTopicsError("source_topics must be a list of topic names")
        topic = candidate.strip()
        if not topic:
            continue
        if topic in (".", "..") or _TOPIC_PATTERN.match(topic) is None:
            raise InvalidSourceTopicsError(
                f'"{topic}" is not a Kafka topic name: use letters, digits, ".", "_" and "-", at most 249 characters'
            )
        if topic not in topics:
            topics.append(topic)
    if not topics:
        raise InvalidSourceTopicsError("source_topics is required: name at least one topic")
    if len(topics) > maximum:
        raise InvalidSourceTopicsError(
            f"a materialization reads at most {maximum} topics; {len(topics)} were given"
        )
    return tuple(topics)


def table_name_for_topic(topic: str) -> str:
    """The SQL table name a topic's rows get: lower case, `_` for anything but letters and digits.

    `Orders.V2` → `orders_v2`; a name that would start with a digit takes `t_`; one that would be a
    reserved word takes `_topic`, since `SELECT * FROM shop.order` does not parse.
    """
    name = _TABLE_NAME_UNSAFE.sub("_", topic.lower()).strip("_") or "topic"
    if name[0].isdigit():
        name = f"t_{name}"
    name = name[:TABLE_NAME_MAX_LENGTH].rstrip("_")
    if name in RESERVED_TABLE_NAMES:
        name = f"{name}_topic"
    return name


def table_names_for_topics(topics: Iterable[str], *, taken: Iterable[str] = ()) -> tuple[str, ...]:
    """Each topic's table name, in order, unique without regard to case.

    A topic whose name is already taken - by an earlier one, or by a name in `taken`, such as the
    tables a materialization already has when an update adds topics - gets the lowest free `_2`,
    `_3`, …, shortened to fit, so the names depend only on the topics, their order and what is taken.
    """
    used = {name.lower() for name in taken}
    names: list[str] = []
    for topic in topics:
        base = table_name_for_topic(topic)
        name = base
        suffix = 2
        while name in used:
            ending = f"_{suffix}"
            name = f"{base[: TABLE_NAME_MAX_LENGTH - len(ending)].rstrip('_')}{ending}"
            suffix += 1
        used.add(name)
        names.append(name)
    return tuple(names)


def same_name(left: str | None, right: str | None) -> bool:
    """Whether two names refer to the same SQL table, which DuckDB decides without regard to case."""
    if left is None or right is None:
        return False
    return left.strip().lower() == right.strip().lower()
