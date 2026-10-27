/**
 * Dataset and table names, the names SQL reads tables by, and materialization labels.
 *
 * A table's SQL name is `<dataset>.<table>`: the console reads `sales.orders`, and an engine on the
 * REST catalog `lake.sales.orders`. Both parts follow one rule, so a person can type them unquoted
 * in any engine: a lowercase letter first, then lowercase letters, digits and underscores, at most
 * 63 characters, and no word SQL reserves. Names are lowercase only: Trino folds unquoted names to
 * lowercase, and DuckDB and Spark compare them without regard to case.
 *
 * - A **dataset's name** is chosen when the dataset is created and never changes. A dataset is its
 *   tables' Iceberg namespace, so it cannot be one of DuckDB's own catalogs or schemas either.
 * - A **table's name** is chosen for each topic when the topic is picked. It starts as one derived
 *   from the topic — `orders-v2` → `orders_v2` — and is unique in its dataset and in its
 *   materialization. It never changes once the table exists.
 * - A **materialization's name** is a label: free text, not unique, of no meaning in SQL. Left
 *   empty, the materialization goes by its id.
 *
 * These rules mirror the control plane's (`table_names.py`), which decides; the console checks them
 * only to say what is wrong while a person types. Dependency-free: imported by the server and the
 * browser alike.
 */

/** The longest dataset name. */
export const DATASET_NAME_MAX_LENGTH = 63;
/** The longest table name; a longer derivation from a topic is cut, keeping it unique. */
export const TABLE_NAME_MAX_LENGTH = 63;
/** The longest materialization label. */
export const LABEL_MAX_LENGTH = 128;

/** A lowercase letter first, then lowercase letters, digits and underscores. */
const NAME_PATTERN = /^[a-z][a-z0-9_]*$/;
/** Characters a label cannot hold. */
const CONTROL = /[\u0000-\u001f\u007f]/;

/**
 * DuckDB's `reserved` and `type_function` keywords (`duckdb_keywords()`, DuckDB 1.5). An unquoted
 * table name cannot be one of these. `server/data/tableNames.test.ts` checks the list against the
 * DuckDB the console runs.
 */
const DUCKDB_RESERVED_KEYWORDS = [
  'all', 'analyse', 'analyze', 'and', 'anti', 'any', 'array', 'as', 'asc', 'asof', 'asymmetric',
  'at', 'authorization', 'binary', 'both', 'by', 'case', 'cast', 'check', 'collate', 'collation',
  'column', 'columns', 'concurrently', 'constraint', 'create', 'cross', 'default', 'deferrable',
  'desc', 'describe', 'distinct', 'do', 'else', 'end', 'except', 'false', 'fetch', 'for',
  'foreign', 'freeze', 'from', 'full', 'generated', 'glob', 'group', 'having', 'ilike', 'in',
  'initially', 'inner', 'intersect', 'into', 'is', 'isnull', 'join', 'lambda', 'lateral',
  'leading', 'left', 'like', 'limit', 'map', 'natural', 'not', 'notnull', 'null', 'offset', 'on',
  'only', 'or', 'order', 'outer', 'overlaps', 'pivot', 'pivot_longer', 'pivot_wider', 'placing',
  'positional', 'primary', 'qualify', 'references', 'returning', 'right', 'select', 'semi', 'show',
  'similar', 'some', 'struct', 'summarize', 'symmetric', 'table', 'tablesample', 'then', 'to',
  'trailing', 'true', 'try_cast', 'union', 'unique', 'unpack', 'unpivot', 'using', 'variadic',
  'verbose', 'when', 'where', 'window', 'with',
] as const;

/**
 * Statement verbs and file-reading functions. DuckDB accepts these as table names, but the
 * console's read-only guard refuses a statement that mentions one, so a table named after one
 * could never be queried here.
 */
const STATEMENT_KEYWORDS = [
  'alter', 'analyze', 'attach', 'avro_scan', 'begin', 'call', 'checkpoint', 'commit', 'copy',
  'create', 'delete', 'detach', 'drop', 'export', 'glob', 'grant', 'iceberg_metadata',
  'iceberg_scan', 'iceberg_snapshots', 'iceberg_table_properties', 'import', 'insert', 'install',
  'load', 'pragma', 'read_avro', 'read_blob', 'read_csv', 'read_json', 'read_parquet', 'read_text',
  'reset', 'revoke', 'rollback', 'set', 'shell', 'sniff_csv', 'system', 'truncate', 'update',
  'vacuum',
] as const;

/**
 * Names the SQL surface gives a meaning of its own: `iceberg` holds each table's native columns
 * (`FROM iceberg.sales.orders`), and `catalog` and `lake` are what the Connect snippets attach the
 * REST catalog as. A name of one of these would be shadowed.
 */
const ICEBERG_KEYWORDS = ['iceberg', 'catalog', 'lake'] as const;

/**
 * The catalogs and schemas DuckDB names itself. A dataset is the namespace of its tables in SQL,
 * `<dataset>.<table>`, and one of these would be ambiguous there or shadow DuckDB's own.
 */
export const NAMESPACE_KEYWORDS = ['main', 'memory', 'temp', 'temporary', 'system', 'information_schema', 'pg_catalog'] as const;

export const RESERVED_TABLE_NAMES: ReadonlySet<string> = new Set([
  ...DUCKDB_RESERVED_KEYWORDS,
  ...STATEMENT_KEYWORDS,
  ...ICEBERG_KEYWORDS,
]);

/** What a dataset cannot be named: what a table cannot, and DuckDB's own namespaces. */
export const RESERVED_DATASET_NAMES: ReadonlySet<string> = new Set([...RESERVED_TABLE_NAMES, ...NAMESPACE_KEYWORDS]);

export interface NameRules {
  maxLength: number;
  reserved: ReadonlySet<string>;
}

export const DEFAULT_DATASET_NAME_RULES: NameRules = {
  maxLength: DATASET_NAME_MAX_LENGTH,
  reserved: RESERVED_DATASET_NAMES,
};

export const DEFAULT_TABLE_NAME_RULES: NameRules = {
  maxLength: TABLE_NAME_MAX_LENGTH,
  reserved: RESERVED_TABLE_NAMES,
};

/** Why `name` cannot name a dataset, or null when it can. */
export function datasetNameProblem(name: string, rules: NameRules = DEFAULT_DATASET_NAME_RULES): string | null {
  return nameProblem(name, rules);
}

/** Why `name` cannot name a table, or null when it can. */
export function tableNameProblem(name: string, rules: NameRules = DEFAULT_TABLE_NAME_RULES): string | null {
  return nameProblem(name, rules);
}

function nameProblem(name: string, rules: NameRules): string | null {
  const trimmed = name.trim();
  if (!trimmed) return 'A name is required';
  if (trimmed.length > rules.maxLength) return `At most ${rules.maxLength} characters`;
  if (!NAME_PATTERN.test(trimmed)) {
    const lower = trimmed.toLowerCase();
    if (lower !== trimmed && NAME_PATTERN.test(lower)) return `Lowercase only: "${lower}"`;
    if (!/^[a-z]/.test(lower)) return 'Start with a lowercase letter';
    return 'Use only lowercase letters, digits and underscores';
  }
  if (rules.reserved.has(trimmed)) {
    return (NAMESPACE_KEYWORDS as readonly string[]).includes(trimmed)
      ? `"${trimmed}" is a name DuckDB gives its own catalogs and schemas`
      : `"${trimmed}" is a reserved SQL word`;
  }
  return null;
}

/** The longest domain id (handbook 20). */
export const DOMAIN_ID_MAX_LENGTH = 32;

/**
 * What a domain id cannot be besides a dataset's reservations: the console's own URL segments
 * (`/api`, `/d`, `/catalog`) and the catalog SQL names native tables in. The control plane's
 * `RESERVED_DOMAIN_IDS` (`table_names.py`) is the same set.
 */
export const RESERVED_DOMAIN_IDS: ReadonlySet<string> = new Set([...RESERVED_DATASET_NAMES, 'api', 'd', 'catalog', 'iceberg']);

/** A domain's native tables are `<domain>_iceberg.<dataset>.<table>` in SQL: no domain may end so. */
export const NATIVE_CATALOG_SUFFIX = '_iceberg';

/**
 * Why `id` cannot be a domain's id, or null when it can. A domain id is a URL segment of the console
 * (`/d/<id>/`), a catalog in its SQL (`<id>.<dataset>.<table>`) and a level of the federated
 * catalog's namespaces, so it follows a dataset name's rule, at most 32 characters, and cannot end
 * `_iceberg`.
 */
export function domainIdProblem(id: string): string | null {
  if (!id) return 'A domain id is required';
  if (id.length > DOMAIN_ID_MAX_LENGTH) return `A domain id is at most ${DOMAIN_ID_MAX_LENGTH} characters`;
  if (!NAME_PATTERN.test(id)) return `"${id}" cannot be a domain id: start with a lowercase letter and use only lowercase letters, digits and underscores`;
  if (RESERVED_DOMAIN_IDS.has(id) || id.endsWith(NATIVE_CATALOG_SUFFIX)) return `"${id}" cannot be a domain id: the console's URLs or SQL use it`;
  return null;
}

/** Why `label` cannot label a materialization, or null when it can; an empty label is the id. */
export function labelProblem(label: string, maxLength: number = LABEL_MAX_LENGTH): string | null {
  if (CONTROL.test(label)) return 'Use no control characters';
  if (normalizeLabel(label).length > maxLength) return `At most ${maxLength} characters`;
  return null;
}

/** A label as the control plane keeps it: trimmed, runs of space as one. */
export function normalizeLabel(label: string): string {
  return label.trim().split(/\s+/).filter(Boolean).join(' ');
}

/** `<dataset>.<table>`: how SQL in the console names a table, and an engine on the catalog does after `lake.`. */
export function qualifiedTableName(dataset: string, table: string): string {
  return `${dataset}.${table}`;
}

/** Whether two names are the same SQL table, which DuckDB decides without regard to case. */
export function sameTableName(left: string | null | undefined, right: string | null | undefined): boolean {
  if (!left || !right) return false;
  return left.trim().toLowerCase() === right.trim().toLowerCase();
}

/**
 * The SQL table name a topic's rows get unless another is chosen: lower case, `_` for anything but
 * letters and digits.
 *
 * `Orders.V2` → `orders_v2`; a name that would start with a digit takes `t_`, one that would be a
 * reserved word takes `_topic`, since `SELECT * FROM sales.order` does not parse.
 */
export function tableNameForTopic(topic: string, rules: NameRules = DEFAULT_TABLE_NAME_RULES): string {
  let name =
    topic
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '_')
      .replace(/^_+/, '')
      .replace(/_+$/, '') || 'topic';
  if (/^[0-9]/.test(name)) name = `t_${name}`;
  name = name.slice(0, rules.maxLength).replace(/_+$/, '');
  if (rules.reserved.has(name)) name = `${name}_topic`;
  return name;
}

/**
 * Each topic's table name, in order, unique without regard to case: what a topic starts with
 * before anyone edits it.
 *
 * A topic whose name is already taken by an earlier one gets the lowest free `_2`, `_3`, …,
 * shortened to fit, so the names depend only on the topics and their order. An edit passes what
 * the materialization has: `fixed`, the table each topic it already reads (or kept a table of) has,
 * which stays; `taken`, the names nothing may take again - its other tables, kept, being dropped, or
 * dropped with their files kept. The control plane names them alike when a request names none.
 */
export function tableNamesForTopics(
  topics: readonly string[],
  rules: NameRules = DEFAULT_TABLE_NAME_RULES,
  existing: { fixed?: ReadonlyMap<string, string>; taken?: Iterable<string> } = {},
): string[] {
  const fixed = existing.fixed ?? new Map<string, string>();
  const used = new Set<string>([...fixed.values(), ...(existing.taken ?? [])].map((name) => name.toLowerCase()));
  return topics.map((topic) => {
    const kept = fixed.get(topic);
    if (kept !== undefined) return kept;
    const base = tableNameForTopic(topic, rules);
    let name = base;
    for (let suffix = 2; used.has(name); suffix += 1) {
      const ending = `_${suffix}`;
      name = `${base.slice(0, rules.maxLength - ending.length).replace(/_+$/, '')}${ending}`;
    }
    used.add(name);
    return name;
  });
}
