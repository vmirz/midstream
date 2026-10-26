import type { FastifyInstance, FastifyRequest } from 'fastify';
import { z } from 'zod';

import type { Caller } from '../controlPlane/client.ts';
import { authOf, callerOf, domainOf, permit } from '../auth/guard.ts';
import { refusal, waitForOutcome } from '../auth/outcomes.ts';
import { config } from '../config.ts';
import { assertSafeIdentifier, badRequest } from '../errors.ts';
import type { Domain } from '../domains/domain.ts';
import { visibleDetail } from '../live/store.ts';
import { datasetNameProblem, labelProblem, tableNameProblem } from '../../shared/tableNames.ts';
import { PARTITION_GRANULARITIES, type CpAccepted } from '../../shared/types.ts';

const labelMap = z.record(z.string().min(1).max(253), z.string().max(1024));

/** A sanity bound only: the control plane enforces its own, configurable maximum. */
const replicaCount = z.number().int().min(1).max(1000);

/** A label: free text, which the control plane trims; empty, the materialization goes by its id. */
const label = z.string().superRefine((value, context) => {
  const problem = labelProblem(value);
  if (problem) context.addIssue({ code: 'custom', message: problem });
});

/**
 * A dataset's or a table's name: what SQL reads the table by, `<dataset>.<table>`, so it is checked
 * here as well as by the control plane: a request the console would accept but engines cannot
 * address never leaves.
 */
const datasetName = z.string().trim().superRefine((value, context) => {
  const problem = datasetNameProblem(value);
  if (problem) context.addIssue({ code: 'custom', message: problem });
});
const tableName = z.string().trim().superRefine((value, context) => {
  const problem = tableNameProblem(value);
  if (problem) context.addIssue({ code: 'custom', message: problem });
});

/** A Kafka topic name, as Kafka itself allows it. */
const topicName = z
  .string()
  .trim()
  .regex(/^[A-Za-z0-9._-]{1,249}$/, 'must be a Kafka topic name: letters, digits, ".", "_" and "-"')
  .refine((value) => value !== '.' && value !== '..', 'must be a Kafka topic name');

/**
 * A topic and its table: the dataset it goes in, and its name, derived from the topic when absent. A
 * topic the materialization already has a table of keeps it, so an update may leave both out.
 */
const tableRequest = z.strictObject({
  source_topic: topicName,
  dataset: datasetName.optional(),
  name: tableName.optional(),
});

/** One table each, in this order; the control plane bounds how many (Settings › Materializations). */
function tableRequests(needsDataset: boolean) {
  return z
    .array(tableRequest)
    .min(1, 'choose at least one topic')
    .max(100)
    .superRefine((tables, context) => {
      const topics = new Set<string>();
      const names = new Set<string>();
      tables.forEach((table, index) => {
        if (topics.has(table.source_topic)) {
          context.addIssue({ code: 'custom', message: 'each topic can be chosen once', path: [index, 'source_topic'] });
        }
        topics.add(table.source_topic);
        if (needsDataset && !table.dataset) {
          context.addIssue({ code: 'custom', message: 'choose the dataset its table goes in', path: [index, 'dataset'] });
        }
        if (table.name) {
          if (names.has(table.name)) {
            context.addIssue({ code: 'custom', message: 'two tables cannot share a name', path: [index, 'name'] });
          }
          names.add(table.name);
        }
      });
    });
}

/**
 * What a create may choose. The destination, the runtime's namespace and name, and its storage are
 * the deployment's, not a materialization's: a body naming one is refused, not quietly overruled.
 */
export const createMaterializationSchema = z.strictObject({
  tables: tableRequests(true),
  /** Its label; omitted or empty, it goes by its id. */
  name: label.optional(),
  runtime_mode: z.enum(['dedicated', 'shared']).optional(),
  resource_profile: z.string().min(1).max(63).nullish(),
  replicas: replicaCount.optional(),
  /** Omitted, on: the control plane adds members while its runtime is far behind its topics. */
  autoscaling: z.boolean().optional(),
  skip_tombstones: z.boolean().optional(),
  /** Omitted, the control plane's default (Settings › Materializations). */
  partition_granularity: z.enum(PARTITION_GRANULARITIES).optional(),
  labels: labelMap.optional(),
  annotations: labelMap.optional(),
});

/**
 * What can change after creation: the label, the runtime's scale and whether it is autoscaled, and
 * the topics it reads - a new one with the dataset and name of its table, and, for each table whose
 * topic is removed, whether to drop it or keep it read-only. Anything else is refused, not ignored.
 */
export const updateMaterializationSchema = z
  .strictObject({
    name: label.optional(),
    replicas: replicaCount.optional(),
    autoscaling: z.boolean().optional(),
    tables: tableRequests(false).optional(),
    removed_tables: z.record(z.string().trim().min(1).max(255), z.enum(['drop', 'keep'])).optional(),
  })
  .refine(
    (body) =>
      body.name !== undefined ||
      body.replicas !== undefined ||
      body.autoscaling !== undefined ||
      body.tables !== undefined ||
      body.removed_tables !== undefined,
    { message: 'say what to change: name, replicas, autoscaling, tables or removed_tables', path: ['replicas'] },
  );

export function materializationId(request: FastifyRequest): string {
  const params = request.params as { materializationId?: string };
  if (!params.materializationId) throw badRequest('materialization_id is required');
  return assertSafeIdentifier(params.materializationId);
}

/**
 * Wait for the directory to claim the names of a request's new tables, and say so on the answer: a
 * refused claim - a name taken meanwhile, a dataset deleted, write access taken away - is the
 * request's error, naming the table it was about, so a dialog can show it on the right row.
 */
export async function awaitClaim(domain: Domain, caller: Caller, accepted: CpAccepted): Promise<CpAccepted> {
  if (!accepted.claim_request_id) return accepted;
  const outcome = await waitForOutcome(domain, caller, accepted.claim_request_id);
  if (outcome.status === 'rejected') {
    const error = refusal(outcome);
    error.details.materialization_id = accepted.materialization_id;
    throw error;
  }
  return { ...accepted, claim_status: outcome.status === 'applied' ? 'applied' : 'pending' };
}

export function parseBody<T extends z.ZodTypeAny>(schema: T, body: unknown): z.infer<T> {
  const parsed = schema.safeParse(body ?? {});
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    throw badRequest(issue ? `${issue.path.join('.') || 'body'}: ${issue.message}` : 'invalid request body', {
      field: issue?.path.join('.'),
    });
  }
  return parsed.data;
}

/** A domain's materializations, under `/api/d/<domain>/` (handbook 20). */
export async function registerMaterializationRoutes(app: FastifyInstance): Promise<void> {
  app.get('/materializations', { preHandler: permit('materializations.view') }, async (request) => {
    const { live } = domainOf(request);
    await live.ensureFresh(config.listPollIntervalMs);
    return live.fleetSnapshot();
  });

  /** Answers once the new tables' names are claimed: a name taken meanwhile is the request's error. */
  app.post('/materializations', { preHandler: permit('materializations.create') }, async (request, reply) => {
    const domain = domainOf(request);
    const input = parseBody(createMaterializationSchema, request.body);
    const caller = callerOf(request);
    const accepted = await domain.controlPlane.create(caller, input);
    domain.live.wake();
    try {
      return reply.code(202).send(await awaitClaim(domain, caller, accepted));
    } finally {
      domain.live.wake();
    }
  });

  app.get('/materializations/:materializationId', { preHandler: permit('materializations.view_details') }, async (request) => {
    const { live } = domainOf(request);
    const id = materializationId(request);
    const detail = await live.getDetail(id);
    return visibleDetail({ ...live.detailSnapshot(id), detail }, authOf(request).state.principal);
  });

  /** Scale the runtime to zero; resume scales it back to its replica count. */
  app.post('/materializations/:materializationId/stop', { preHandler: permit('materializations.stop') }, async (request, reply) => {
    const domain = domainOf(request);
    const accepted = await domain.controlPlane.stop(callerOf(request), materializationId(request));
    domain.live.wake();
    return reply.code(202).send(accepted);
  });

  /**
   * Change a materialization's mutable settings: its label, its replica count, and the topics it
   * reads. A change adding topics answers once their tables' names are claimed.
   */
  app.patch('/materializations/:materializationId', { preHandler: permit('materializations.edit') }, async (request, reply) => {
    const domain = domainOf(request);
    const id = materializationId(request);
    const input = parseBody(updateMaterializationSchema, request.body);
    const caller = callerOf(request);
    const accepted = await domain.controlPlane.update(caller, id, input);
    domain.live.wake();
    try {
      return reply.code(202).send(await awaitClaim(domain, caller, accepted));
    } finally {
      domain.live.wake();
    }
  });

  app.post('/materializations/:materializationId/resume', { preHandler: permit('materializations.resume') }, async (request, reply) => {
    const domain = domainOf(request);
    const accepted = await domain.controlPlane.resume(callerOf(request), materializationId(request));
    domain.live.wake();
    return reply.code(202).send(accepted);
  });

  app.delete('/materializations/:materializationId', { preHandler: permit('materializations.delete') }, async (request, reply) => {
    const domain = domainOf(request);
    const accepted = await domain.controlPlane.remove(callerOf(request), materializationId(request));
    domain.live.wake();
    return reply.code(202).send(accepted);
  });
}
