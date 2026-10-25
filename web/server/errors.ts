import type { ApiErrorBody } from '../shared/types.ts';

/** Transport-agnostic API failure carrying the status code the console should return. */
export class ConsoleError extends Error {
  readonly code: string;
  readonly statusCode: number;
  readonly details: Record<string, unknown>;

  constructor(code: string, message: string, statusCode = 400, details: Record<string, unknown> = {}) {
    super(message);
    this.name = 'ConsoleError';
    this.code = code;
    this.statusCode = statusCode;
    this.details = details;
  }

  toBody(): ApiErrorBody {
    return { error: { code: this.code, message: this.message, details: this.details } };
  }
}

export const badRequest = (message: string, details: Record<string, unknown> = {}): ConsoleError =>
  new ConsoleError('invalid_request', message, 400, details);

export const notFound = (message: string, details: Record<string, unknown> = {}): ConsoleError =>
  new ConsoleError('not_found', message, 404, details);

export const unavailable = (message: string, details: Record<string, unknown> = {}): ConsoleError =>
  new ConsoleError('unavailable', message, 503, details);

export const upstreamError = (
  message: string,
  statusCode: number,
  details: Record<string, unknown> = {},
): ConsoleError => new ConsoleError('control_plane_error', message, statusCode, details);

/** Materialization ids are used in filesystem paths and SQL literals; keep them opaque and safe. */
const IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

export function assertSafeIdentifier(value: string, field = 'materialization_id'): string {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw badRequest(`${field} contains unsupported characters`, { field });
  }
  return value;
}
