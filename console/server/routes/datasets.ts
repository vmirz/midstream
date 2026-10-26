import type { FastifyInstance, FastifyRequest } from 'fastify';
import { z } from 'zod';

import { assertSafeIdentifier, badRequest } from '../errors.ts';
import { callerOf, domainOf, permit, permitAnywhere } from '../auth/guard.ts';
import { settle } from '../auth/outcomes.ts';
import { parseBody } from './materializations.ts';
import { datasetNameProblem } from '../../shared/tableNames.ts';
import type { DatasetChangeResult } from '../../shared/types.ts';

/** A dataset's name, as SQL reads it: checked here so a request the control plane would refuse never leaves. */
const datasetName = z.string().trim().superRefine((value, context) => {
  const problem = datasetNameProblem(value);
  if (problem) context.addIssue({ code: 'custom', message: problem });
});
const description = z.string().max(500);

const createDatasetSchema = z.strictObject({ name: datasetName, description: description.optional() });
const updateDatasetSchema = z.strictObject({ description, base_revision: z.number().int().min(1).optional() });
const grantSchema = z.strictObject({
  role_ids: z.array(z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/, 'is not a valid id')).max(50),
  principal_type: z.enum(['user', 'group']).optional(),
});
const listQuery = z.object({ q: z.string().max(200).optional() });

/** The dataset a request names, checked to be one a path can carry. */
function datasetOf(request: FastifyRequest): string {
  const name = (request.params as { dataset?: string }).dataset ?? '';
  if (!name) throw badRequest('dataset is required', { field: 'dataset' });
  return assertSafeIdentifier(name, 'dataset');
}

function principalIdOf(request: FastifyRequest): string {
  const id = (request.params as { principalId?: string }).principalId ?? '';
  if (!id) throw badRequest('principal_id is required', { field: 'principal_id' });
  return assertSafeIdentifier(id, 'principal_id');
}

/**
 * Datasets: named groups of tables, and their Iceberg namespaces.
 *
 * Reads pass the control plane's answer through, asked as the person, so what it says about each
 * dataset - `locked`, the person's own permissions on it - is about them. A change is sent with the
 * person's session, so the control plane authorizes it on the dataset itself; the guards here only
 * turn away someone who holds the permission on no dataset at all. A change answers once the
 * directory has applied it: `200` (`201` for a new dataset), `202` when it is still pending after
 * 10 seconds, or the refusal.
 */
export async function registerDatasetRoutes(app: FastifyInstance): Promise<void> {
  /** Every dataset, locked ones included; `?q=` finds them by name, description or table name. */
  app.get('/datasets', { preHandler: permit('datasets.view') }, async (request) => {
    const { q } = parseBody(listQuery, request.query ?? {});
    return domainOf(request).controlPlane.datasets(callerOf(request), q ?? '');
  });

  app.post('/datasets', { preHandler: permit('datasets.create') }, async (request, reply) => {
    const caller = callerOf(request);
    const body = parseBody(createDatasetSchema, request.body);
    const accepted = await domainOf(request).controlPlane.createDataset(caller, body);
    const result: DatasetChangeResult = { ...(await settle(domainOf(request), caller, accepted)), name: accepted.name };
    return reply.code(result.status === 'applied' ? 201 : 202).send(result);
  });

  /** One dataset, and every table in it with the materialization that writes it. */
  app.get('/datasets/:dataset', { preHandler: permit('datasets.view') }, async (request) =>
    domainOf(request).controlPlane.dataset(callerOf(request), datasetOf(request)),
  );

  app.patch('/datasets/:dataset', { preHandler: permitAnywhere('dataset.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const body = parseBody(updateDatasetSchema, request.body);
    const result = await settle(domainOf(request), caller, await domainOf(request).controlPlane.updateDataset(caller, datasetOf(request), body));
    return reply.code(result.status === 'applied' ? 200 : 202).send(result);
  });

  /** Only an empty dataset can be deleted: none of its tables written, kept, or still being dropped. */
  app.delete('/datasets/:dataset', { preHandler: permitAnywhere('dataset.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const result = await settle(domainOf(request), caller, await domainOf(request).controlPlane.deleteDataset(caller, datasetOf(request)));
    return reply.code(result.status === 'applied' ? 200 : 202).send(result);
  });

  /** Who may use a dataset, and with which roles; and the roles the person asking can grant there. */
  app.get('/datasets/:dataset/grants', { preHandler: permitAnywhere('dataset.manage') }, async (request) =>
    domainOf(request).controlPlane.datasetGrants(callerOf(request), datasetOf(request)),
  );

  /** Set the roles a person or a group holds on a dataset; none removes their grant. */
  app.put('/datasets/:dataset/grants/:principalId', { preHandler: permitAnywhere('dataset.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const body = parseBody(grantSchema, request.body);
    const result = await settle(domainOf(request), caller, await domainOf(request).controlPlane.setDatasetGrant(caller, datasetOf(request), principalIdOf(request), body));
    // Who may read which tables changed: open detail pages show it on their next refresh.
    domainOf(request).live.wake();
    return reply.code(result.status === 'applied' ? 200 : 202).send(result);
  });
}
