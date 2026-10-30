import { Shield } from 'lucide-react';
import type { ReactNode } from 'react';

import { Badge } from '@/components/ui/primitives';
import type { CpProtectionCatalog, CpProtectionSummary, ProtectionPolicy } from '@shared/types';

import { choiceOf, kindLabel, maskName } from './protection';

/** What a policy masks, as chips: `Email addresses · hashed`. */
export function ProtectionChips({
  policy,
  catalog,
}: {
  policy: ProtectionPolicy | null | undefined;
  catalog: CpProtectionCatalog | null;
}): ReactNode {
  const choice = choiceOf(policy ?? null, catalog);
  const entries = Object.entries(choice);
  if (entries.length === 0) return <span className="text-[var(--color-ink-muted)]">None: values are written as they arrive</span>;
  return (
    <span className="flex flex-wrap gap-1.5">
      {entries.map(([kind, mask]) => (
        <Badge key={kind} tone="accent" title={catalog ? undefined : kind}>
          <Shield className="size-3" aria-hidden />
          {kindLabel(catalog, kind)} · {maskName(mask, catalog).toLowerCase()}
        </Badge>
      ))}
    </span>
  );
}

/** How far a policy has reached, in a word, for the overview and the Protection tab. */
export function ProtectionStateBadge({ summary }: { summary: CpProtectionSummary }): ReactNode {
  if (summary.revision < 1) return null;
  switch (summary.state) {
    case 'applying':
      return (
        <Badge tone="pending" title={applyingTitle(summary)}>
          Applying revision {summary.revision}
        </Badge>
      );
    case 'stopped':
      return <Badge tone="neutral">Applies on resume</Badge>;
    case 'not_reporting':
      return <Badge tone="neutral">No member reporting</Badge>;
    default:
      return <Badge tone="running">In force · revision {summary.revision}</Badge>;
  }
}

function applyingTitle(summary: CpProtectionSummary): string {
  const parts: string[] = [];
  if (summary.tables_behind.length) parts.push(`tables not yet recording it: ${summary.tables_behind.join(', ')}`);
  if (summary.members_behind.length) parts.push(`members not yet masking with it: ${summary.members_behind.length}`);
  return parts.join('; ') || 'Reaching every member and table';
}

/** A shield beside a protected field or column, saying how its values are masked. */
export function ProtectedMark({
  protection,
  catalog = null,
}: {
  protection: { kind: string; mask: string; since_schema_id?: number } | null | undefined;
  catalog?: CpProtectionCatalog | null;
}): ReactNode {
  if (!protection) return null;
  const title = `Masked · ${kindLabel(catalog, protection.kind)} · ${maskName(protection.mask, catalog).toLowerCase()}${
    protection.since_schema_id !== undefined ? ` since schema ${protection.since_schema_id}` : ''
  }`;
  return (
    <span className="inline-flex shrink-0 items-center text-[var(--color-accent)]" title={title} aria-label={title} role="img">
      <Shield className="size-3.5" aria-hidden />
    </span>
  );
}
