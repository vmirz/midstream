import type { FastifyInstance, FastifyRequest } from 'fastify';
import { z } from 'zod';

import { controlPlane } from '../controlPlane/client.ts';
import { assertSafeIdentifier, badRequest } from '../errors.ts';
import { callerOf, permit, signedIn } from '../auth/guard.ts';
import { settle } from '../auth/outcomes.ts';
import { parseBody } from './materializations.ts';
import { USER_FILTERS, USER_SORTS } from '../../shared/types.ts';

const id = z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/, 'is not a valid id');
const ids = z.array(id).max(500);
const name = z.string().trim().min(1).max(100);
const label = z.string().trim().min(1).max(80);
const description = z.string().max(500);
const password = z.string().min(1).max(256);
const baseRevision = z.number().int().min(1);

const createUserSchema = z.strictObject({
  first_name: name,
  last_name: name,
  email: z.string().trim().min(1).max(254),
  password,
  group_ids: ids.optional(),
});

const updateUserSchema = z.strictObject({
  first_name: name.optional(),
  last_name: name.optional(),
  email: z.string().trim().min(1).max(254).optional(),
  group_ids: ids.optional(),
  status: z.enum(['active', 'deactivated']).optional(),
  base_revision: baseRevision.optional(),
});

const createGroupSchema = z.strictObject({
  name: label,
  description: description.optional(),
  role_ids: ids.optional(),
  member_ids: ids.optional(),
});

const updateGroupSchema = z.strictObject({
  name: label.optional(),
  description: description.optional(),
  role_ids: ids.optional(),
  add_member_ids: ids.optional(),
  remove_member_ids: ids.optional(),
  base_revision: baseRevision.optional(),
});

const permissions = z.array(z.string().min(1).max(64)).max(200);
const createRoleSchema = z.strictObject({ name: label, description: description.optional(), permissions });
const updateRoleSchema = z.strictObject({
  name: label.optional(),
  description: description.optional(),
  permissions: permissions.optional(),
  base_revision: baseRevision.optional(),
});

const userQuery = z.object({
  status: z.enum(USER_FILTERS).optional(),
  sort: z.enum(USER_SORTS).optional(),
  q: z.string().max(200).optional(),
});

const auditQuery = z.object({
  limit: z.coerce.number().int().min(1).max(200).optional(),
  before: z.string().max(200).optional(),
  action: z.string().max(80).optional(),
  outcome: z.enum(['success', 'failure']).optional(),
  q: z.string().max(200).optional(),
});

function param(request: FastifyRequest, key: string): string {
  const value = (request.params as Record<string, string | undefined>)[key];
  if (!value) throw badRequest(`${key} is required`, { field: key });
  return assertSafeIdentifier(value, key);
}

function query<T extends z.ZodTypeAny>(schema: T, request: FastifyRequest): Record<string, string> {
  const parsed = schema.safeParse(request.query ?? {});
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    throw badRequest(`${issue?.path.join('.') || 'query'}: ${issue?.message ?? 'invalid'}`, { field: issue?.path.join('.') });
  }
  return Object.fromEntries(
    Object.entries(parsed.data as Record<string, unknown>)
      .filter(([, value]) => value !== undefined && value !== '')
      .map(([key, value]) => [key, String(value)]),
  );
}

function changes<T extends z.ZodTypeAny>(schema: T, body: unknown): z.infer<T> {
  const parsed = parseBody(schema, body);
  if (Object.keys(parsed as object).filter((key) => key !== 'base_revision').length === 0) {
    throw badRequest('nothing to change');
  }
  return parsed;
}

/**
 * Users, groups, roles, the permission catalog and the audit log.
 *
 * Reads pass the control plane's answer through. A change is sent with the signed-in person's own
 * session, so the control plane authorizes it, and the route answers once it has been applied:
 * `200` when it was, `202` when it is still pending after 10 seconds, or the refusal.
 */
export async function registerIdentityRoutes(app: FastifyInstance): Promise<void> {
  app.get('/api/users', { preHandler: permit('users.view') }, async (request) =>
    controlPlane.users(callerOf(request), query(userQuery, request)),
  );

  app.get('/api/users/:userId', { preHandler: permit('users.view_details') }, async (request) =>
    controlPlane.user(callerOf(request), param(request, 'userId')),
  );

  app.post('/api/users', { preHandler: permit('users.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const result = await settle(caller, await controlPlane.createUser(caller, parseBody(createUserSchema, request.body)));
    return reply.code(result.status === 'applied' ? 201 : 202).send(result);
  });

  app.patch('/api/users/:userId', { preHandler: permit('users.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const body = changes(updateUserSchema, request.body);
    const result = await settle(caller, await controlPlane.updateUser(caller, param(request, 'userId'), body));
    return reply.code(result.status === 'applied' ? 200 : 202).send(result);
  });

  app.delete('/api/users/:userId', { preHandler: permit('users.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const result = await settle(caller, await controlPlane.deleteUser(caller, param(request, 'userId')));
    return reply.code(result.status === 'applied' ? 200 : 202).send(result);
  });

  app.post('/api/users/:userId/password', { preHandler: permit('users.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const body = parseBody(z.strictObject({ password }), request.body);
    const result = await settle(caller, await controlPlane.setUserPassword(caller, param(request, 'userId'), body));
    return reply.code(result.status === 'applied' ? 200 : 202).send(result);
  });

  app.post('/api/users/:userId/sign-out', { preHandler: permit('users.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const body = parseBody(z.strictObject({ session_id: id.optional() }), request.body ?? {});
    const result = await settle(caller, await controlPlane.signOutUser(caller, param(request, 'userId'), body));
    return reply.code(result.status === 'applied' ? 200 : 202).send(result);
  });

  app.post('/api/users/:userId/unlock', { preHandler: permit('users.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const result = await settle(caller, await controlPlane.unlockUser(caller, param(request, 'userId')));
    return reply.code(result.status === 'applied' ? 200 : 202).send(result);
  });

  app.get('/api/groups', { preHandler: permit('groups.view') }, async (request) => controlPlane.groups(callerOf(request)));

  app.get('/api/groups/:groupId', { preHandler: permit('groups.view') }, async (request) =>
    controlPlane.group(callerOf(request), param(request, 'groupId')),
  );

  app.post('/api/groups', { preHandler: permit('groups.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const result = await settle(caller, await controlPlane.createGroup(caller, parseBody(createGroupSchema, request.body)));
    return reply.code(result.status === 'applied' ? 201 : 202).send(result);
  });

  app.patch('/api/groups/:groupId', { preHandler: permit('groups.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const body = changes(updateGroupSchema, request.body);
    const result = await settle(caller, await controlPlane.updateGroup(caller, param(request, 'groupId'), body));
    return reply.code(result.status === 'applied' ? 200 : 202).send(result);
  });

  app.delete('/api/groups/:groupId', { preHandler: permit('groups.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const result = await settle(caller, await controlPlane.deleteGroup(caller, param(request, 'groupId')));
    return reply.code(result.status === 'applied' ? 200 : 202).send(result);
  });

  app.get('/api/roles', { preHandler: permit('roles.view') }, async (request) => controlPlane.roles(callerOf(request)));

  app.get('/api/roles/:roleId', { preHandler: permit('roles.view') }, async (request) =>
    controlPlane.role(callerOf(request), param(request, 'roleId')),
  );

  app.post('/api/roles', { preHandler: permit('roles.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const result = await settle(caller, await controlPlane.createRole(caller, parseBody(createRoleSchema, request.body)));
    return reply.code(result.status === 'applied' ? 201 : 202).send(result);
  });

  app.patch('/api/roles/:roleId', { preHandler: permit('roles.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const body = changes(updateRoleSchema, request.body);
    const result = await settle(caller, await controlPlane.updateRole(caller, param(request, 'roleId'), body));
    return reply.code(result.status === 'applied' ? 200 : 202).send(result);
  });

  app.delete('/api/roles/:roleId', { preHandler: permit('roles.manage') }, async (request, reply) => {
    const caller = callerOf(request);
    const result = await settle(caller, await controlPlane.deleteRole(caller, param(request, 'roleId')));
    return reply.code(result.status === 'applied' ? 200 : 202).send(result);
  });

  app.get('/api/permissions', { preHandler: signedIn() }, async (request) =>
    controlPlane.permissions(callerOf(request)),
  );

  app.get('/api/audit', { preHandler: permit('audit.view') }, async (request) =>
    controlPlane.audit(callerOf(request), query(auditQuery, request)),
  );
}
