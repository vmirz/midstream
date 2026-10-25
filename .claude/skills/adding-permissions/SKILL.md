---
name: adding-permissions
description: Add or change a permission, a built-in role or a built-in group in the Kafka materializer, and gate endpoints and console UI on it. Use whenever a task adds a control-plane WebView or console route, a new action someone may or may not be allowed to take, a new page or button that only some people should see, or changes what the default roles and groups (Guest, All Users, Viewer, Editor, Admin) may do.
---

# Adding permissions and roles

People hold **groups**, groups hold **roles**, roles hold **permissions**. Permissions are a fixed
catalog in code, each with a **scope**: `deployment` — held everywhere or not at all — or `dataset`
— about a dataset's tables, held on one dataset at a time through a **dataset grant** (a person or
group given dataset roles on one dataset), or everywhere through a group. Every control-plane
WebView and every console `/api` route names the one it needs. The control plane is the authority (`authorize(...)` in each WebView); the console checks
the same permission on its own routes and hides UI with `useCan`. Background:
`docs/handbook/18-identity-and-access.md` (§18.3 catalog and rules, §18.7 authorization).

## 0. Do you need a new permission at all?

Reuse an existing one when the new thing is the same kind of access: a new read about a
materialization is `materializations.view_details`; a new way to change one is
`materializations.edit`; another export format is `data.export`. Add a permission only when an
administrator would plausibly want to grant it separately. Every permission is one more checkbox
in the role editor.

## 1. Define it — `python/src/materializer_control_plane/identity/permissions.py`

The examples below use a hypothetical **restart** action; nothing by that name exists.

```python
PermissionDefinition(
    "materializations.restart",          # <area>.<verb>; area = category id; never reuse a key
    "materializations",                  # a PERMISSION_CATEGORIES id (add a category if needed)
    "Restart materializations",          # label: shown as written in the role editor
    "Restart a materialization's runtime pods. Rows staged on them and not yet written are lost.",
    implies=("materializations.view_details",),   # what it cannot be used without
    danger=True,                         # can remove data or lock people out
    # scope=DATASET_SCOPE for anything about a dataset's tables (their schemas, rows, catalog
    # entries, adding or dropping them); the default is DEPLOYMENT_SCOPE
),
```

- **`implies`**: an action implies the read it acts on (create/edit/stop → `view_details`;
  `data.export` → `data.query`). Implications are transitive and resolved on every request, and
  the role editor shows them as included. Never imply something *more* powerful, and **stay within
  the scope**: a dataset permission implies only dataset permissions, or a grant on one dataset
  would hand out something across the deployment (`test_identity` checks it).
- **A dataset permission** also goes in `web/shared/access.ts::DATASET_PERMISSIONS` (the console's
  `can(principal, key, datasetId)` needs to know it is held per dataset), and in the dataset roles
  (Dataset Reader / Writer / Owner) it belongs to.
- **Order** in `PERMISSIONS` is the order the role editor and the user page list them; keep each
  category together.
- **Keys are forever.** Custom roles store keys. A removed or renamed key silently grants nothing
  (`expand_permissions` drops unknown keys) and `clean_permissions` refuses it on the next save of
  any role holding it. To rename, add the new key and leave the old one implying it.

## 2. Decide who gets it

| Lever | Effect | Use for |
|---|---|---|
| Built-in **role** (`BUILT_IN_ROLES`) | Defined in code, never stored: a change applies to every deployment on upgrade | The normal way: put the permission in the role(s) whose description it fits (Operator, Viewer, User Manager, Dataset Reader / Writer / Owner, …). Update the role's description if it no longer reads true |
| Administrator (`*`) | Holds every permission automatically, including new ones | Nothing to do |
| Built-in **group** defaults (`BUILT_IN_GROUPS`) | Only seed a group the first time `identity-bootstrap` stores it; **an already-stored group keeps its own roles** | New deployments only; never rely on it to change existing ones |
| `SERVICE_PERMISSIONS` | The console's read-only service credential (live store polling) | Only a `.view`-style read the live store itself performs — and the console must then cut what it reads down for each person (`live/store.ts::visibleDetail`). Never anything that changes state |
| Guest | Whatever the Guest group's roles grant (Status Viewer by default) | Almost never; guests are anyone who can reach the console |

A new built-in **role**: add a `BuiltInRole(role_id, name, description, permissions)`; it appears
everywhere at once and administrators can add it to groups. A new built-in **group**: add a
`BuiltInGroup` (id, name, description, `membership="assigned"`, `role_ids`, `locked_role_ids`); the
next start's bootstrap stores it. Built-in ids are permanent — a custom group or role may already
use a generated `grp-…`/`rol-…` id, never a readable one, so readable ids are safe for built-ins.

## 3. Gate the control-plane endpoint

The first line of the WebView operation's Python, with the directory table bound:

```yaml
    operations:
      - name: prepare-request
        tables:
          - name: directory
            paramName: directory_table
          - name: state
            paramName: control_plane_table
        map:
          entrypoint: prepare_request
          python: |
            from materializer_control_plane.identity.authorization import authorize
            from materializer_control_plane.web.materializations import restart_materialization

            def prepare_request(request, materialization_id, directory_table, control_plane_table):
                principal = authorize(request, directory_table, "materializations.restart")
                return restart_materialization(
                    control_plane_table,
                    materialization_id,
                    submitted_by=principal.attribution,
                    submitted_by_id=principal.user_id,
                ).to_value()
```

- `authorize(request, directory_table, None)` asks only that the caller be known (any signed-in
  person); add `allow_password_change=True` only for what a person must reach before replacing a
  chosen password (their session, the password change itself).
- **A dataset permission** is checked on a dataset: `authorize(..., "dataset.manage",
  dataset=dataset)` when the path names it (an unknown one answers `404`), or `any_dataset=True`
  when the view then checks each dataset it touches. When the dataset comes from records — a
  table's — authorize the deployment permission the view needs (`materializations.view_details`)
  and call `require(principal, "<key>", dataset=…)` once it knows the dataset. `authorize` raises
  for `dataset=` on a deployment permission.
- A view function that takes the `Principal` should also `require(principal, "<key>")`, so it
  cannot be called unchecked from elsewhere.
- Changes record `principal.attribution` (the email) and `principal.user_id`, never a header.
- Patch the directory table binding in `manifests/dev/kustomization.yaml` (`develop-directory`),
  like every other view's. `OverlayTopicWiringTests` renders every overlay directory, including local
  untracked ones, and fails until they carry it.
- `python/tests/test_agent_manifests.py::AuthorizationWiringTests` fails when a view does not call
  `authorize` with a permission the catalog knows. Only `system-version` is public
  (`PUBLIC_WEBVIEWS`); views that authenticate themselves are listed in
  `SELF_AUTHENTICATING_ENTRYPOINTS` — do not add to either lightly.

## 4. Gate the console

**Route** (`web/server/routes/*.ts`), and call the control plane as the person:

```ts
app.post('/api/materializations/:materializationId/restart', { preHandler: permit('materializations.restart') }, async (request, reply) => {
  const accepted = await controlPlane.restart(callerOf(request), materializationId(request));
  return reply.code(202).send(accepted);
});
```

- `permit(key)` → `401 sign_in_required` for a guest, `403 forbidden` (with `permission`) for a
  person without it, `403 password_change_required` until a chosen password is replaced.
  `signedIn()` asks only for a session.
- A dataset permission: `permitAnywhere(key)` (held on at least one dataset), then
  `requireOnDatasets(authOf(request), key, [{ id, name }])` on each dataset the request is about —
  every table a run reads, the table a schema is of. `permit(key)` alone would refuse everyone whose
  access comes from grants.
- `callerOf(request)` for anything a person asks or changes. `SERVICE` only for reads the live
  store shares among everyone — and those must be in `SERVICE_PERMISSIONS`.
- A stream: `watchAccess(request, '<key>', onLost)`, then send `event: auth` and end it in
  `onLost` (see `routes/stream.ts`).
- A route that answers anyone (no guard) is a deliberate choice; say why in a comment.

**UI** (`web/src`): never a security boundary, always a courtesy.

| Gate | How |
|---|---|
| A button or control | `const canRestart = useCan('materializations.restart')` and render it only when true; for a dataset permission `useCan('data.query', table.dataset_id)` |
| A whole page | `<RequirePermission permission="…" what="…">` in `App.tsx` (guests go to sign-in, others see Access denied) |
| A nav item | `permission` on the entry in `NAV_ITEMS` (`components/AppShell.tsx`) |
| A detail tab | `permission` on its `TABS` entry (`features/detail/DetailPage.tsx`); a dataset permission there shows the tab when it is held on any of the materialization's tables' datasets |
| A Settings section | `permission` on its `SETTINGS_SECTIONS` entry (`features/settings/sections.ts`) |
| A query | `enabled: canX` so the page does not ask for what will be refused |

The role editor, the user page and the account page read the catalog from `GET /api/permissions`,
so a new permission appears there with no console change.

## 5. Tests

- `python/tests/test_identity.py` — `PermissionCatalogTests` checks keys are unique, implications
  and built-in roles refer to real keys, and pins what the default groups grant
  (`test_default_groups_grant_what_was_agreed`): update its expectations deliberately when a
  built-in role changes. Add a case for any new rule.
- The view's behaviour: the module that owns it, called with a principal from `authorize(...)`
  (see `DirectoryViewTests.principal`), asserting a principal without the permission gets
  `WebError` 401/403.
- Console route: `installTestAuth(server, testPrincipal(['materializations.restart']))` from
  `server/auth/testing.ts`, plus a test that `testPrincipal(['materializations.view'])` gets `403`
  (`server/topics/catalog.test.ts` has the pattern).

## 6. Documentation

- `docs/handbook/18-identity-and-access.md` §18.3: the permission table, and the built-in role
  table if a role changed.
- `docs/handbook/04-http-api-reference.md` §4.1 **Needs** column; `web/docs/08-console-api-reference.md`
  §8.1 **Needs** column.
- `web/docs/11-feature-reference.md` where a button, tab or page now depends on it.

## 7. Verify

```bash
python -m unittest discover -s python/tests -p 'test_identity.py' -v
python -m unittest discover -s python/tests -p 'test_datasets.py' -v
python -m unittest discover -s python/tests -p 'test_*.py'
kubectl kustomize manifests/base >/dev/null && kubectl kustomize manifests/dev >/dev/null
cd web && npm test && npm run typecheck && npm run build
```

Then in a browser, three times: as a **guest** (sent to sign-in), as someone **without** the
permission (no button; the route answers 403 if called anyway), and as someone **with** it. The
Roles page shows the new permission in its category, with what it implies.

## Deploying

No migration: the catalog is code. Built-in role changes apply on upgrade, to everyone in groups
holding those roles. Custom roles get a new permission only when an administrator adds it.
