import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';

import { api } from '@/lib/api';
import { DEFAULT_DATASET_NAME_RULES, DEFAULT_TABLE_NAME_RULES, LABEL_MAX_LENGTH, type NameRules } from '@shared/tableNames';
import type { ConsoleAutoscaling, MaterializationDefaults } from '@shared/types';

/** The most topics a materialization reads when the control plane has not said. */
export const FALLBACK_MAX_TOPICS = 20;

/**
 * The control plane's defaults for a new materialization, and the rules dataset and table names
 * meet.
 *
 * Read through the console config, which the shell already holds. A dialog asks again when that
 * copy is more than a minute old, so a control plane that was unreachable when the page loaded is
 * not remembered as having no defaults. Until the defaults arrive — or for someone the console does
 * not give them to — the rules are the console's own copy of the same lists.
 */
export function useMaterializationDefaults(): {
  defaults: MaterializationDefaults | null;
  datasetRules: NameRules;
  tableRules: NameRules;
  labelMaxLength: number;
  maxTopics: number;
  /** How the control plane scales runtimes far behind; null when it cannot say. */
  autoscaling: ConsoleAutoscaling | null;
  loading: boolean;
} {
  const config = useConsoleConfig();
  const defaults = config.data?.materializationDefaults ?? null;
  const datasetRules = useMemo<NameRules>(
    () =>
      defaults
        ? {
            maxLength: defaults.datasetNameMaxLength,
            reserved: new Set([...defaults.reservedNames, ...defaults.reservedNamespaceNames]),
          }
        : DEFAULT_DATASET_NAME_RULES,
    [defaults],
  );
  const tableRules = useMemo<NameRules>(
    () =>
      defaults
        ? { maxLength: defaults.tableNameMaxLength, reserved: new Set(defaults.reservedNames) }
        : DEFAULT_TABLE_NAME_RULES,
    [defaults],
  );
  return {
    defaults,
    datasetRules,
    tableRules,
    labelMaxLength: defaults?.labelMaxLength ?? LABEL_MAX_LENGTH,
    maxTopics: defaults?.maxTopics ?? FALLBACK_MAX_TOPICS,
    autoscaling: config.data?.autoscaling ?? null,
    loading: config.isLoading,
  };
}

/** How the control plane scales runtimes far behind their topics, for anyone signed in; null when it cannot say. */
export function useAutoscaling(): ConsoleAutoscaling | null {
  return useConsoleConfig().data?.autoscaling ?? null;
}

function useConsoleConfig() {
  return useQuery({
    queryKey: ['config'],
    queryFn: () => api.config(),
    staleTime: 60_000,
    retry: 0,
  });
}
