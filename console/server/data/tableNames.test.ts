/**
 * The name rules in `shared/tableNames.ts` hold against the DuckDB the console actually runs and the
 * guard it actually applies: every name the rules accept can be created, queried unquoted, and gets
 * past the guard; every word the rules refuse is one of those that could not.
 */

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { DuckDBInstance } from '@duckdb/node-api';

import { FORBIDDEN_TOKENS, assertReadOnlyStatement } from './sql.ts';
import { RESERVED_DATASET_NAMES, RESERVED_TABLE_NAMES, datasetNameProblem, tableNameProblem } from '../../shared/tableNames.ts';

async function keywords(): Promise<Map<string, string>> {
  const instance = await DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  try {
    const reader = await connection.runAndReadAll('SELECT keyword_name, keyword_category FROM duckdb_keywords()');
    return new Map(reader.getRowObjects().map((row) => [String(row.keyword_name).toLowerCase(), String(row.keyword_category)]));
  } finally {
    connection.closeSync();
  }
}

describe('dataset and table name rules', () => {
  it('refuse every keyword DuckDB reserves from table names', async () => {
    const missing = [...(await keywords())]
      .filter(([, category]) => category === 'reserved' || category === 'type_function')
      .map(([keyword]) => keyword)
      .filter((keyword) => !RESERVED_TABLE_NAMES.has(keyword));
    assert.deepEqual(missing, [], 'a DuckDB upgrade reserved new keywords; add them to both name lists');
  });

  it('refuse every word the statement guard refuses, so a valid table is always queryable', () => {
    const missing = FORBIDDEN_TOKENS.filter((token) => !RESERVED_TABLE_NAMES.has(token));
    assert.deepEqual(missing, []);
  });

  it('accept names that DuckDB creates, queries unquoted in any case, and the guard lets through', async () => {
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    try {
      const unreserved = [...(await keywords())]
        .filter(([, category]) => category === 'unreserved' || category === 'column_name')
        .map(([keyword]) => keyword)
        .filter((keyword) => tableNameProblem(keyword) === null);
      for (const name of ['orders', 'orders_2026', 'materialized', ...unreserved]) {
        await connection.run(`CREATE OR REPLACE TEMP VIEW "${name}" AS SELECT 1 AS x`);
        for (const sql of [`SELECT x FROM ${name} WHERE x = 1`, `SELECT count(*) FROM ${name.toUpperCase()} AS t`]) {
          assert.equal(assertReadOnlyStatement(sql), sql, name);
          await connection.run(sql);
        }
      }
    } finally {
      connection.closeSync();
    }
  });

  it('accept dataset names DuckDB takes as a schema, so `<dataset>.<table>` reads unquoted', async () => {
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    try {
      const unreserved = [...(await keywords())]
        .filter(([, category]) => category === 'unreserved' || category === 'column_name')
        .map(([keyword]) => keyword)
        .filter((keyword) => datasetNameProblem(keyword) === null);
      for (const name of ['sales', 'finance_eu', ...unreserved]) {
        await connection.run(`CREATE SCHEMA IF NOT EXISTS "${name}"`);
        await connection.run(`CREATE OR REPLACE VIEW "${name}".orders AS SELECT 1 AS x`);
        const sql = `SELECT x FROM ${name}.orders WHERE x = 1`;
        assert.equal(assertReadOnlyStatement(sql), sql, name);
        await connection.run(sql);
      }
      for (const name of ['main', 'temp', 'memory', 'information_schema']) {
        assert.ok(RESERVED_DATASET_NAMES.has(name), `${name} is DuckDB's own`);
      }
    } finally {
      connection.closeSync();
    }
  });
});
