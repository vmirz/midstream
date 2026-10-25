import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, it } from 'node:test';

const HERE = dirname(fileURLToPath(import.meta.url));

/**
 * A hook called after a conditional early return is only reached on some renders.
 *
 * React counts hooks per render, so that throws "Rendered more hooks than during the previous
 * render" the moment the component gets past the return — a blank page with nothing wrong at
 * compile time. It happened here: a `useMemo` added below `if (!dataset?.available) return …`
 * typechecked, built, and broke the tab as soon as data loaded.
 *
 * The repository deliberately runs no linter, so there is no `react-hooks/rules-of-hooks` to catch
 * it. This is the narrow substitute: within a component body, no hook call may follow the first
 * early return. It only inspects lines at component-body indentation, so hooks inside nested
 * callbacks and helper functions are ignored.
 *
 * The rule is per function body, so the scan resets at every top-level declaration. Without that
 * an early return in a plain helper leaked into the component below it and flagged every hook in
 * the file — which is exactly what happened when a helper above `JobPanel` grew a multi-line
 * guard clause, and a false positive here is a test nobody can act on.
 */
export function hooksAfterEarlyReturn(source: string): string[] {
  const lines = source.split('\n');
  const offenders: string[] = [];
  let sawEarlyReturn = false;

  for (const [index, line] of lines.entries()) {
    // A declaration in column zero ends the previous body and starts a fresh one.
    if (/^(export )?(default )?(async )?(function|const|class) /.test(line)) {
      sawEarlyReturn = false;
    }
    // Two spaces exactly: the body of a top-level component, not a nested closure.
    if (/^ {2}if \(/.test(line)) {
      // An early return is an `if` whose block returns. Look ahead a few lines for it.
      const block = lines.slice(index, index + 6).join('\n');
      if (/\n {4}return[ (]/.test(block)) sawEarlyReturn = true;
      continue;
    }
    if (!sawEarlyReturn) continue;
    if (/^ {2}(const .* = )?use[A-Z]\w*\(/.test(line)) {
      offenders.push(`line ${index + 1}: ${line.trim()}`);
    }
  }
  return offenders;
}

describe('the rules-of-hooks detector', () => {
  it('flags a hook after an early return inside a component', () => {
    const source = [
      'export function Thing(): ReactNode {',
      '  if (!ready) {',
      '    return null;',
      '  }',
      '  const value = useMemo(() => 1, []);',
      '  return value;',
      '}',
    ].join('\n');
    assert.deepEqual(hooksAfterEarlyReturn(source), ['line 5: const value = useMemo(() => 1, []);']);
  });

  it('does not let a helper’s guard clause leak into the next function', () => {
    const source = [
      'function helper(x: number): number | null {',
      '  if (x < 0) {',
      '    return null;',
      '  }',
      '  return x;',
      '}',
      '',
      'export function Thing(): ReactNode {',
      '  const value = useMemo(() => 1, []);',
      '  return value;',
      '}',
    ].join('\n');
    assert.deepEqual(hooksAfterEarlyReturn(source), []);
  });
});

describe('rules of hooks', () => {
  const files = readdirSync(HERE).filter((name) => name.endsWith('.tsx'));

  it('finds components to check', () => {
    assert.ok(files.length > 0);
  });

  for (const file of files) {
    it(`${file} calls no hook after an early return`, () => {
      const offenders = hooksAfterEarlyReturn(readFileSync(join(HERE, file), 'utf8'));
      assert.deepEqual(
        offenders,
        [],
        `hooks after an early return in ${file}:\n  ${offenders.join('\n  ')}`,
      );
    });
  }
});
