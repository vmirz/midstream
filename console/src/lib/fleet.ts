import { useMemo } from 'react';

import { mergeFleet, type Fleet } from './fleetMerge';
import { useFleetStream, type LiveStatus } from './live';

export type { DomainMaterialization, Fleet } from './fleetMerge';

/** Every domain's fleet merged, or `domain`'s alone, live. */
export function useFleet(domain: string | null = null): { data: Fleet | null; status: LiveStatus } {
  const { data, status } = useFleetStream();
  const merged = useMemo(() => (data ? mergeFleet(data, domain) : null), [data, domain]);
  return { data: merged, status };
}
