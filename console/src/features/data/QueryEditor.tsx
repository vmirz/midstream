import type { CompletionContext, CompletionResult } from '@codemirror/autocomplete';
import { sql, PostgreSQL } from '@codemirror/lang-sql';
import { lintGutter, setDiagnostics, type Diagnostic } from '@codemirror/lint';
import type { EditorView } from '@codemirror/view';
import CodeMirror from '@uiw/react-codemirror';
import { useEffect, useMemo, useRef, type ReactNode } from 'react';

import { isMetadataColumn } from '@shared/fieldPaths';
import type { StatementPosition } from '@shared/types';

const RUN_HINT = navigator.platform.toLowerCase().includes('mac') ? '⌘↵' : 'Ctrl+↵';

/**
 * SQL editor bound to the prepared views of the tables a statement may read.
 *
 * Tables are completed by their full names, `<domain>.<dataset>.<table>`, and their native ones,
 * `<domain>_iceberg.<dataset>.<table>` (handbook 20): the materialization's own, with their columns,
 * and any other the person may name, in any domain. Column names are fed in from the run's columns
 * and the tables' schemas, so completion offers exactly the names the views have: field paths
 * without the schema's `payload.` root, and the Kafka metadata.
 */
export function QueryEditor({
  value,
  tableName,
  domain,
  tables,
  otherTables = [],
  tableColumns = {},
  columns,
  onChange,
  onRun,
  error = null,
}: {
  value: string;
  /** The table on screen; completion offers its columns without a table name. */
  tableName: string;
  /** The domain the materialization is in. */
  domain: string;
  /** The materialization's tables a statement may read, each in its dataset. */
  tables: ReadonlyArray<{ dataset: string; name: string }>;
  /** Other tables a statement may name, in any domain: completed by name, their columns unknown here. */
  otherTables?: ReadonlyArray<{ domain: string; dataset: string; name: string }>;
  /** Each table's columns, for completion after its name. */
  tableColumns?: Record<string, string[]>;
  columns: string[];
  onChange: (value: string) => void;
  /** Bound to the keyboard shortcut only; the tab's Run control is the visible affordance. */
  onRun: () => void;
  /**
   * The last failure, with the place the engine pointed at.
   *
   * Marked in the text rather than only described below it: an error that names a line is far
   * easier to act on when the line is also underlined, and the message reads as a note about
   * something visible instead of a claim about text the reader now has to go and count.
   */
  error?: { message: string; position?: StatementPosition | null } | null;
}): ReactNode {
  const viewRef = useRef<EditorView | null>(null);
  const extensions = useMemo(() => {
    // Catalog, schema, table: `domain_a.sales.orders`, and `domain_a_iceberg.sales.orders` for the
    // same tables with their native columns, offered by name.
    const schema: Record<string, Record<string, Record<string, string[]>>> = {};
    const add = (catalog: string, dataset: string, name: string, names: string[]): void => {
      const tablesOf = ((schema[catalog] ??= {})[dataset] ??= {});
      if (!(name in tablesOf) || names.length > 0) tablesOf[name] = names;
    };
    for (const table of otherTables) {
      add(table.domain, table.dataset, table.name, []);
      add(`${table.domain}_iceberg`, table.dataset, table.name, []);
    }
    for (const table of tables) {
      add(domain, table.dataset, table.name, table.name === tableName ? columns : (tableColumns[table.name] ?? []));
      add(`${domain}_iceberg`, table.dataset, table.name, []);
    }
    return [
      lintGutter(),
      sql({ dialect: PostgreSQL, upperCaseKeywords: true, schema }),
      // The table on screen's columns, unqualified, wherever a name can stand; `defaultTable` cannot
      // name a table inside a namespace.
      PostgreSQL.language.data.of({ autocomplete: unqualifiedColumns(columns) }),
    ];
  }, [domain, tableName, tables, otherTables, tableColumns, columns]);
  const current = tables.find((table) => table.name === tableName);

  const dark = document.documentElement.classList.contains('dark');
  const example = useMemo(() => exampleColumn(columns), [columns]);

  /*
   * Diagnostics are pushed through a transaction rather than rebuilt as an extension, because
   * swapping extensions re-creates the editor state and would throw away the cursor and undo
   * history every time a query failed.
   */
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;

    const diagnostics: Diagnostic[] = [];
    if (error) {
      const { from, to } = markedRange(view.state.doc.toString(), error.position ?? null);
      diagnostics.push({ from, to, severity: 'error', message: error.message });
    }
    view.dispatch(setDiagnostics(view.state, diagnostics));
  }, [error]);

  return (
    <div className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-[var(--radius-panel)] border border-[var(--color-line)]">
      <div
        className="min-h-0 flex-1 overflow-auto"
        onKeyDown={(event) => {
          if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
            event.preventDefault();
            onRun();
          }
        }}
      >
        <CodeMirror
          value={value}
          height="100%"
          theme={dark ? 'dark' : 'light'}
          extensions={extensions}
          onChange={onChange}
          onCreateEditor={(view) => {
            viewRef.current = view;
          }}
          basicSetup={{
            lineNumbers: true,
            foldGutter: false,
            highlightActiveLine: false,
            highlightActiveLineGutter: false,
            autocompletion: true,
          }}
        />
      </div>
      <div className="flex shrink-0 flex-wrap items-center justify-between gap-x-3 gap-y-1 border-t border-[var(--color-line)] bg-[var(--color-raised)] px-3 py-2">
        <p className="text-[11px] text-[var(--color-ink-muted)]">
          Read-only. Query{' '}
          {tables.length > 1 ? (
            <>
              any of <code className="numeric">{tables.map((table) => `${domain}.${table.dataset}.${table.name}`).join(', ')}</code> and join them
            </>
          ) : (
            <code className="numeric">
              {domain}.{current?.dataset ?? 'dataset'}.{tableName}
            </code>
          )}
          {otherTables.length > 0 ? ', or any table you may read in any domain, by its full name' : ''}; columns are named by their field paths, so quote
          them: <code className="numeric">"{example}"</code>.
        </p>
        {/*
          No Run button here. The tab has one Run control, above the grid, and it already reports
          whether there is anything to run and whether something is running. A second button in the
          editor was the same action in a different place, which only raises the question of
          whether the two do the same thing.
        */}
        <p className="text-[11px] text-[var(--color-ink-faint)]">
          <span className="numeric">{RUN_HINT}</span> to run
        </p>
      </div>
    </div>
  );
}

/** A column worth quoting in the hint: a nested field if there is one, since those need quotes most. */
function exampleColumn(columns: string[]): string {
  const fields = columns.filter((name) => !isMetadataColumn(name));
  return fields.find((name) => name.includes('.')) ?? fields[0] ?? 'customer.email';
}

/**
 * The document range to underline for a failure.
 *
 * DuckDB points at a single character, which underlines to nothing, so the marker is widened to
 * the word beginning there — the token the parser actually objected to. Without a position the
 * whole last line is marked: `syntax error at end of input` has no coordinates, and the end of the
 * statement is exactly where the reader should look.
 */
function markedRange(doc: string, position: StatementPosition | null): { from: number; to: number } {
  const lines = doc.split('\n');

  const lineNumber = position
    ? Math.min(Math.max(position.line, 1), lines.length)
    : lines.length;
  const lineText = lines[lineNumber - 1] ?? '';
  const lineStart = lines.slice(0, lineNumber - 1).reduce((sum, line) => sum + line.length + 1, 0);

  if (!position) {
    return { from: lineStart + (lineText.length - lineText.trimStart().length), to: lineStart + lineText.length };
  }

  const column = Math.min(Math.max(position.column, 1), Math.max(lineText.length, 1));
  const from = lineStart + column - 1;
  const token = /^[A-Za-z0-9_."']+/.exec(lineText.slice(column - 1));
  return { from, to: from + Math.max(token?.[0]?.length ?? 1, 1) };
}

/** Completion of the on-screen table's columns where no qualifier precedes the cursor. */
function unqualifiedColumns(columns: readonly string[]): (context: CompletionContext) => CompletionResult | null {
  const options = columns.map((name) => ({
    label: name,
    type: 'property',
    // A field path holds dots, so it is quoted as SQL reads it: `"customer.email"`.
    apply: /^[a-z_][a-z0-9_]*$/.test(name) ? name : `"${name.replace(/"/g, '""')}"`,
  }));
  return (context) => {
    const word = context.matchBefore(/[\w$]*/);
    if (!word || (word.from === word.to && !context.explicit)) return null;
    // After `domain_a.` or an alias's `o.`, the schema completion knows better.
    if (context.state.sliceDoc(word.from - 1, word.from) === '.') return null;
    return { from: word.from, options, validFor: /^[\w$]*$/ };
  };
}
