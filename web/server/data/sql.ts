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
      const found = sql.indexOf('\n', index);
      const end = found === -1 ? sql.length : found;
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
