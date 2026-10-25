import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback } from 'react';

import { api } from '@/lib/api';
import type { CpDatasetList } from '@shared/types';

export const DATASETS_KEY = ['datasets'] as const;

/**
 * Every dataset, as the control plane describes it to the person asking: locked ones included, each
 * with what they may do with it. Shared by the pickers and the Datasets page.
 */
export function useDatasets(enabled = true) {
  return useQuery<CpDatasetList>({ queryKey: DATASETS_KEY, queryFn: () => api.datasets(), staleTime: 15_000, enabled });
}

/** Read datasets again: after one is created, changed or deleted, or its access changed. */
export function useRefreshDatasets(): () => Promise<void> {
  const queryClient = useQueryClient();
  return useCallback(() => queryClient.invalidateQueries({ queryKey: DATASETS_KEY }), [queryClient]);
}
