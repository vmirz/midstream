/**
 * Request shapes shared by the data routes.
 *
 * Fork projections used to travel with every request — the client sent every fork of every field
 * so the server could fold them — which made bodies grow with schema history until a limit had to
 * be invented for them. The server now derives projections from the schema it already holds, so
 * nothing here grows with history.
 */

import { z } from 'zod';

export const MAX_FILTERS = 20;

export const filterSchema = z.object({
  // Logical column names are paths such as `payload.items[].sku`, validated against the run's
  // columns before they reach SQL.
  column: z.string().min(1).max(1_024),
  operator: z.enum([
    'eq',
    'neq',
    'contains',
    'starts_with',
    'gt',
    'gte',
    'lt',
    'lte',
    'is_null',
    'is_not_null',
  ]),
  value: z.string().max(4096).optional(),
});

export const filtersSchema = z.array(filterSchema).max(MAX_FILTERS).optional();
