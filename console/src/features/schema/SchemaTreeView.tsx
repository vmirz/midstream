import { ProtectedMark } from '../protection/ProtectionSummary';
import { ChevronRight } from 'lucide-react';
import { useMemo, useState, type ReactNode } from 'react';

import { FieldState, TypeChip } from './SchemaFieldTable';
import { cn } from '@/lib/cn';
import type { CpSchemaField } from '@shared/types';

interface TreeNode {
  key: string;
  name: string;
  children: TreeNode[];
  /** The schema field at this path; null for a container the filter left without its own entry. */
  field: CpSchemaField | null;
  /** Number of leaf fields at or below this node. */
  leafCount: number;
}

const INDENT_PX = 16;
const ELEMENT = '[]';

/**
 * The schema as the table stores it: structs hold their fields, and a list of structs holds its
 * element's fields, so the tree is the Iceberg schema itself — forks and `__json` fields as the
 * siblings they are. Order is the schema's own.
 */
function buildTree(fields: CpSchemaField[]): TreeNode[] {
  const roots: TreeNode[] = [];
  const index = new Map<string, TreeNode>();

  const ensureNode = (key: string, name: string, parent: TreeNode | null): TreeNode => {
    const existing = index.get(key);
    if (existing) return existing;
    const node: TreeNode = { key, name, children: [], field: null, leafCount: 0 };
    index.set(key, node);
    if (parent) parent.children.push(node);
    else roots.push(node);
    return node;
  };

  for (const field of fields) {
    // A list element is not a node of its own: `items` holds `sku` directly.
    const segments = field.path.filter((segment) => segment !== ELEMENT);
    let parent: TreeNode | null = null;
    let key = '';
    segments.forEach((segment, depth) => {
      key = depth === 0 ? segment : `${key}/${segment}`;
      const node = ensureNode(key, segment, parent);
      if (depth === segments.length - 1) node.field = field;
      parent = node;
    });
  }

  const countLeaves = (node: TreeNode): number => {
    node.leafCount = node.children.length === 0 ? 1 : node.children.reduce((sum, child) => sum + countLeaves(child), 0);
    return node.leafCount;
  };
  roots.forEach(countLeaves);
  return roots;
}

function collectBranchKeys(nodes: TreeNode[], into: Set<string> = new Set()): Set<string> {
  for (const node of nodes) {
    if (node.children.length > 0) {
      into.add(node.key);
      collectBranchKeys(node.children, into);
    }
  }
  return into;
}

export function SchemaTreeView({ fields }: { fields: CpSchemaField[] }): ReactNode {
  const roots = useMemo(() => buildTree(fields), [fields]);
  const branchKeys = useMemo(() => collectBranchKeys(roots), [roots]);
  const rootBranchKeys = useMemo(
    () => roots.filter((node) => node.children.length > 0).map((node) => node.key),
    [roots],
  );

  const [expanded, setExpanded] = useState<Set<string>>(() => new Set(rootBranchKeys));
  const [seed, setSeed] = useState(() => rootBranchKeys.join('|'));

  // Re-seed the open set whenever the underlying field list changes (version or filter).
  const nextSeed = rootBranchKeys.join('|');
  if (nextSeed !== seed) {
    setSeed(nextSeed);
    setExpanded(new Set(rootBranchKeys));
  }

  const toggle = (key: string): void => {
    setExpanded((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const expandAll = (): void => setExpanded(new Set(branchKeys));
  const collapseAll = (): void => setExpanded(new Set());

  const renderNodes = (nodes: TreeNode[], depth: number): ReactNode =>
    nodes.map((node) => {
      const hasChildren = node.children.length > 0;
      const open = hasChildren && expanded.has(node.key);
      const list = node.field?.path.at(-1) !== ELEMENT && (node.field?.type.startsWith('list<') ?? false);
      return (
        <li key={node.key}>
          <div
            className={cn(
              'flex items-center gap-3 px-5 py-2 transition-colors hover:bg-[var(--color-raised)]',
              node.field?.state === 'superseded' && 'opacity-55',
            )}
            style={{ paddingLeft: 20 + depth * INDENT_PX }}
          >
            {hasChildren ? (
              <button
                type="button"
                onClick={() => toggle(node.key)}
                aria-expanded={open}
                aria-label={`${open ? 'Collapse' : 'Expand'} ${node.name}`}
                className="flex size-4 shrink-0 items-center justify-center text-[var(--color-ink-muted)] transition-colors hover:text-[var(--color-ink)]"
              >
                <ChevronRight className={cn('size-3.5 transition-transform', open && 'rotate-90')} />
              </button>
            ) : (
              <span className="size-4 shrink-0" />
            )}

            <span className="numeric flex min-w-0 flex-1 items-center gap-1.5 text-[12px] text-[var(--color-ink)]">
              <span className="truncate">
                {node.name}
                {list ? <span className="text-[var(--color-ink-faint)]">[]</span> : null}
              </span>
              <ProtectedMark protection={node.field?.protection} />
            </span>

            {hasChildren ? (
              <span className="shrink-0 text-[11px] text-[var(--color-ink-faint)]">
                {node.leafCount} {node.leafCount === 1 ? 'field' : 'fields'}
              </span>
            ) : null}

            {node.field ? (
              <>
                <span className="w-[130px] shrink-0">
                  <TypeChip type={node.field.type} json={node.field.kind === 'json'} />
                </span>
                <span className="w-[64px] shrink-0 text-[12px] text-[var(--color-ink-muted)]">
                  {node.field.required ? 'required' : 'optional'}
                </span>
                <span className="flex w-[96px] shrink-0 justify-end">
                  <FieldState field={node.field} />
                </span>
              </>
            ) : (
              <span className="w-[314px] shrink-0" />
            )}
          </div>
          {open ? <ul>{renderNodes(node.children, depth + 1)}</ul> : null}
        </li>
      );
    });

  return (
    <div>
      <div
        className={cn(
          'flex items-center justify-between gap-4 border-b border-[var(--color-line)] bg-[var(--color-raised)] px-5 py-2',
          'text-[11px] font-medium tracking-wide text-[var(--color-ink-muted)] uppercase',
        )}
      >
        <span>Structure</span>
        <span className="flex items-center gap-2 normal-case">
          <button type="button" onClick={expandAll} className="text-[11px] transition-colors hover:text-[var(--color-ink)]">
            Expand all
          </button>
          <span className="text-[var(--color-line-strong)]">·</span>
          <button type="button" onClick={collapseAll} className="text-[11px] transition-colors hover:text-[var(--color-ink)]">
            Collapse all
          </button>
        </span>
      </div>
      <ul className="divide-y divide-[var(--color-line)] overflow-x-auto">{renderNodes(roots, 0)}</ul>
    </div>
  );
}
