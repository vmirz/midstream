/**
 * The statement guard's forbidden functions: what lists the sandbox's secrets, and what runs SQL from a
 * string, is refused however the call is written; a table or column of the same name is not.
 */

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { FORBIDDEN_FUNCTIONS, assertReadOnlyStatement, forbiddenCall } from './sql.ts';

const refused = (sql: string, name: string): void => {
  assert.throws(
    () => assertReadOnlyStatement(sql),
    (error: unknown) => error instanceof Error && error.message === `"${name}" is not allowed in console queries`,
    sql,
  );
};

describe('the statement guard', () => {
  it('refuses the functions that list the sandbox’s secrets', () => {
    refused('SELECT * FROM duckdb_secrets()', 'duckdb_secrets');
    refused("SELECT name FROM which_secret('s3://example-lake/sales/orders/data/a.parquet', 's3')", 'which_secret');
    refused('FROM duckdb_secrets()', 'duckdb_secrets');
    refused('SELECT name, scope FROM DUCKDB_SECRETS() WHERE type = \'s3\'', 'duckdb_secrets');
    refused('SELECT count(*) FROM (SELECT * FROM duckdb_secrets()) AS s', 'duckdb_secrets');
  });

  it('refuses them quoted, qualified, or with a comment before the parenthesis', () => {
    refused('SELECT * FROM "duckdb_secrets"()', 'duckdb_secrets');
    refused('SELECT * FROM main.duckdb_secrets()', 'duckdb_secrets');
    refused('SELECT * FROM system.main."which_secret" (\'gs://lake/x\', \'gcs\')', 'which_secret');
    refused('SELECT * FROM duckdb_secrets /* nothing */ ()', 'duckdb_secrets');
    refused('SELECT * FROM duckdb_secrets -- nothing\n()', 'duckdb_secrets');
    // DuckDB ends a `--` comment at a carriage return too, so the parenthesis is still a call.
    refused('SELECT name, scope, secret_string FROM duckdb_secrets --x\r()', 'duckdb_secrets');
    refused("SELECT name FROM which_secret --x\r('s3://example-lake/x', 's3')", 'which_secret');
    refused('SELECT * FROM duckdb_secrets --x\r\n()', 'duckdb_secrets');
  });

  it('refuses the functions that run SQL given as a string', () => {
    refused("SELECT * FROM query('SELECT 1')", 'query');
    refused("SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT 1'))", 'json_execute_serialized_sql');
  });

  it('lets a table, column or value of the same name through', () => {
    for (const sql of [
      'SELECT query, count(*) FROM domain_a.sales.searches GROUP BY query',
      'SELECT "query" FROM domain_a.sales.searches',
      'SELECT * FROM domain_a.sales.which_secret',
      'SELECT s.query FROM domain_a.sales.searches AS s',
      "SELECT * FROM domain_a.sales.orders WHERE note = 'duckdb_secrets'",
      'SELECT my_query(1)',
      'SELECT querystring FROM domain_a.sales.searches',
    ]) {
      assert.equal(assertReadOnlyStatement(sql), sql, sql);
    }
  });

  it('names each function it refuses', () => {
    assert.deepEqual(FORBIDDEN_FUNCTIONS, ['duckdb_secrets', 'which_secret', 'query', 'json_execute_serialized_sql']);
    assert.equal(forbiddenCall('SELECT 1'), null);
    assert.equal(forbiddenCall('SELECT * FROM which_secret(\'s3://x/\', \'s3\')'), 'which_secret');
  });
});
