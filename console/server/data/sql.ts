import { badRequest } from '../errors.ts';

/** Quote a SQL identifier for DuckDB, escaping embedded double quotes. */
export function quoteIdentifier(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

/** Quote a SQL string literal, escaping embedded single quotes. */
export function quoteLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

const STATEMENT_ALLOW_LIST = /^(select|with|describe|summarize|pivot|unpivot|from|table|values)\b/i;

/** Words a console statement may not contain; no materialization can be named after one. */
export const FORBIDDEN_TOKENS = [
  'attach',
  'detach',
  'copy',
  'export',
  'import',
  'install',
  'load',
  'create',
  'drop',
  'alter',
  'insert',
  'update',
  'delete',
  'truncate',
  'call',
  'pragma',
  'set',
  'reset',
  'checkpoint',
  'vacuum',
  'analyze',
  'begin',
  'commit',
  'rollback',
  'grant',
  'revoke',
  'read_text',
  'read_blob',
  'read_csv',
  'read_json',
  'read_parquet',
  'read_avro',
  'avro_scan',
  'iceberg_scan',
  'iceberg_metadata',
  'iceberg_snapshots',
  'iceberg_table_properties',
  'glob',
  'sniff_csv',
  'shell',
  'system',
] as const;

/**
 * Functions a console statement may not call, however the call is spelled — quoted or not, in any
 * case, qualified with a catalog or schema or not.
 *
 * `duckdb_secrets()` and `which_secret()` list the secrets of the instance a statement runs in: every
 * storage profile's name, scope, endpoint, region and key id (the secret values are redacted). The
 * statement's sandbox cannot refuse them, because they open no file, so the guard must. `query()` and
 * `json_execute_serialized_sql()` run SQL handed to them as a string, which no keyword check sees.
 * (`duckdb_settings()` and `current_setting()` were checked and name no profile: secrets are not
 * settings.) Matched as calls only — the name followed by `(` — so a table, column or alias of the
 * same name stays queryable, and no table name needs reserving for them.
 */
export const FORBIDDEN_FUNCTIONS = ['duckdb_secrets', 'which_secret', 'query', 'json_execute_serialized_sql'] as const;

const IDENTIFIER_CHAR = /[a-z0-9_$]/;
/** Whitespace and invisible formatting characters, which separate tokens as whitespace does. */
const SEPARATOR = /[\s\p{Cf}]/u;

/**
 * The forbidden function `sql` calls, if any. Deliberately blind to string literals and comments
 * around the name: a name inside a string that is followed by `(` is refused too, which costs nothing
 * real and leaves no question of where a string ends.
 */
export function forbiddenCall(sql: string): string | null {
  const lowered = sql.toLowerCase();
  for (const name of FORBIDDEN_FUNCTIONS) {
    for (let at = lowered.indexOf(name); at !== -1; at = lowered.indexOf(name, at + 1)) {
      if (at > 0 && IDENTIFIER_CHAR.test(lowered[at - 1] as string)) continue;
      let cursor = at + name.length;
      if (IDENTIFIER_CHAR.test(lowered[cursor] ?? '')) continue;
      if (lowered[cursor] === '"') cursor += 1;
      if (lowered[skipSeparators(lowered, cursor)] === '(') return name;
    }
  }
  return null;
}

/** Past whitespace and comments — a `--` line, or a block comment, which DuckDB lets nest. */
function skipSeparators(text: string, start: number): number {
  let cursor = start;
  while (cursor < text.length) {
    const char = text[cursor] as string;
    if (SEPARATOR.test(char)) {
      cursor += 1;
    } else if (text.startsWith('--', cursor)) {
      // DuckDB ends a `--` comment at the first carriage return OR line feed, not at a line feed
      // alone: `name --x\r(` is a call, so the comment must stop at the `\r` for the `(` to be seen.
      const offset = text.slice(cursor).search(/[\r\n]/);
      cursor = offset === -1 ? text.length : cursor + offset;
    } else if (text.startsWith('/*', cursor)) {
      let depth = 0;
      while (cursor < text.length) {
        if (text.startsWith('/*', cursor)) {
          depth += 1;
          cursor += 2;
        } else if (text.startsWith('*/', cursor)) {
          depth -= 1;
          cursor += 2;
          if (depth === 0) break;
        } else {
          cursor += 1;
        }
      }
    } else {
      break;
    }
  }
  return cursor;
}

/**
 * Reject anything that is not a single read-only statement.
 *
 * The query console is intentionally powerful, so the guard is deliberately conservative:
 * one statement, must start with a projection keyword, and may not reference statement kinds
 * or file-access functions that would let a query escape the prepared view of the
 * materialization's table. No materialization can be named after one of these tokens (see
 * `shared/tableNames.ts`), so the guard never refuses a statement for naming its own table.
 */
export function assertReadOnlyStatement(sql: string): string {
  const trimmed = sql.trim().replace(/;+\s*$/, '');
  if (!trimmed) throw badRequest('sql is required', { field: 'sql' });
  if (trimmed.length > 20_000) throw badRequest('sql exceeds 20000 characters', { field: 'sql' });

  const stripped = stripStringsAndComments(trimmed);
  if (stripped.includes(';')) {
    throw badRequest('only a single statement can be executed', { field: 'sql' });
  }
  if (!STATEMENT_ALLOW_LIST.test(stripped.trimStart())) {
    throw badRequest('only read-only SELECT-style statements are allowed', { field: 'sql' });
  }

  const lowered = stripped.toLowerCase();
  for (const token of FORBIDDEN_TOKENS) {
    if (new RegExp(`(^|[^\\w.])${token}\\s*[( ]`, 'i').test(lowered)) {
      throw badRequest(`"${token}" is not allowed in console queries`, { field: 'sql', token });
    }
  }
  const call = forbiddenCall(trimmed);
  if (call) throw badRequest(`"${call}" is not allowed in console queries`, { field: 'sql', token: call });
  return trimmed;
}

/**
 * Blank out string literals and comments so keyword scanning cannot be fooled by data.
 *
 * By default quoted identifiers are blanked too and the text shrinks. `keepLength` blanks string
 * literals and comments with spaces instead and keeps quoted identifiers, so a position in the
 * result is the same position in the statement.
 */
export function stripStringsAndComments(sql: string, options: { keepLength?: boolean } = {}): string {
  const keep = options.keepLength ?? false;
  let result = '';
  let index = 0;
  const blank = (text: string): string => text.replace(/[^\n]/g, ' ');

  while (index < sql.length) {
    const char = sql[index] as string;
    const next = sql[index + 1];

    if (char === '-' && next === '-') {
      // A `--` comment runs to the first carriage return or line feed, as DuckDB ends it (so a
      // forbidden token hidden after `--x\r` is still scanned); the terminator itself is kept.
      const offset = sql.slice(index).search(/[\r\n]/);
      const end = offset === -1 ? sql.length : index + offset;
      if (keep) result += blank(sql.slice(index, end));
      index = end;
      continue;
    }
    if (char === '/' && next === '*') {
      const found = sql.indexOf('*/', index + 2);
      const end = found === -1 ? sql.length : found + 2;
      if (keep) result += blank(sql.slice(index, end));
      index = end;
      continue;
    }
    if (char === "'" || char === '"' || char === '$') {
      const quote = char;
      let cursor = index + 1;
      while (cursor < sql.length) {
        if (sql[cursor] === quote) {
          if (sql[cursor + 1] === quote) {
            cursor += 2;
            continue;
          }
          break;
        }
        cursor += 1;
      }
      const end = Math.min(cursor + 1, sql.length);
      if (keep) result += quote === '"' ? sql.slice(index, end) : blank(sql.slice(index, end));
      // Preserve identifier boundaries so `"a"."b"` does not collapse into one token.
      else result += quote === '"' ? ' x ' : ' ';
      index = cursor + 1;
      continue;
    }
    result += char;
    index += 1;
  }
  return result;
}
