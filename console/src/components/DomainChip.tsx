import type { ReactNode } from 'react';

import { Badge } from '@/components/ui/primitives';

/** Which domain something is in, on a page that shows several (handbook 20). */
export function DomainChip({ name, id, className }: { name: string; id?: string; className?: string }): ReactNode {
  return (
    <Badge className={className} title={id ? `Domain ${name} (${id})` : `Domain ${name}`}>
      {name}
    </Badge>
  );
}
