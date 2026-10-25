/**
 * The table's catalog entry: `<location>/metadata/catalog-pointer.json`.
 *
 * The runtime's committer replaces it on every commit, by compare-and-swap, to name the table's new
 * metadata file; that is the whole catalog. Reading it is how the console learns there is newer
 * data, and the metadata location it names is what a run pins to.
 */

import { ObjectMissingError, joinLocation, readObject } from './io.ts';
import { parseJson } from './json.ts';

export const POINTER_PATH = 'metadata/catalog-pointer.json';

export interface TablePointer {
  tableUuid: string;
  identifier: string[];
  materializationId: string;
  location: string;
  metadataLocation: string;
  /** Bumped by every commit; never moves backwards. */
  sequence: number;
  updatedAt: string | null;
  updatedBy: string | null;
}

/** The table's pointer, or null when there is none (the table is not created, or was dropped). */
export async function readPointer(location: string): Promise<TablePointer | null> {
  let raw: Buffer;
  try {
    raw = await readObject(joinLocation(location, POINTER_PATH));
  } catch (cause) {
    if (cause instanceof ObjectMissingError) return null;
    throw cause;
  }
  return parsePointer(raw.toString('utf8'));
}

export function parsePointer(text: string): TablePointer {
  const value = parseJson(text) as Record<string, unknown>;
  const metadataLocation = value.metadata_location;
  if (typeof metadataLocation !== 'string' || !metadataLocation) {
    throw new Error('the table pointer names no metadata file');
  }
  return {
    tableUuid: String(value.table_uuid ?? ''),
    identifier: Array.isArray(value.identifier) ? value.identifier.map(String) : [],
    materializationId: String(value.materialization_id ?? ''),
    location: String(value.location ?? ''),
    metadataLocation,
    sequence: Number(value.sequence ?? 0),
    updatedAt: typeof value.updated_at === 'string' ? value.updated_at : null,
    updatedBy: typeof value.updated_by === 'string' ? value.updated_by : null,
  };
}
