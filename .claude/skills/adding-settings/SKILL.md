---
name: adding-settings
description: Add, change or retire a setting of the Kafka materializer — the control plane, its runtimes or the web console — whether adjustable from the console's Settings page (an override) or fixed by the deployment (shown read-only). Use whenever a task introduces a new environment variable, configuration field, tunable, limit, timeout or credential, or moves one between adjustable and fixed.
---

# Adding a setting

Every component is configured by its environment. A setting that is safe to change while the
system runs may also be **overridden** from the Settings page; the override is stored with the
control plane (`settings` table) and wins until it is reset. Everything else is **fixed by the
deployment** and only shown, read-only, on the page's **Deployment** section. Background:
`docs/handbook/17-settings.md`, `console/docs/03-configuration-reference.md` §3.10.

## 1. Decide what kind of setting it is

| Question | Answer → kind |
|---|---|
| Does it size the pod, wire it to its environment (topic, table, namespace, URL, port, path), shape the rendered runtime spec, or is it read only once at startup? | **Fixed** (§5) |
| Is it a credential or secret? | **Fixed**, and never a config field — report only *configured / not configured* (§5.3) |
| Can a wrong value lose data or lock people out? | Adjustable, but `danger=True`, and think hard about bounds |
| Otherwise | **Adjustable** — then which component reads it? Control plane (§2), runtime (§3), console (§4) |

An adjustable setting must be read **where it is used, every time**, never captured at import or in
a constructor; otherwise an override silently does nothing. If the code cannot work that way, the
setting is fixed.

Pick the **section** it belongs to from `console/src/features/settings/sections.ts`
(`materializations`, `deletion`, `schema`, `runtimes`, `jobs`, `live`, `data`, `topics`,
`diagnostics`, `security`, …). A section id that is not in `SETTINGS_SECTIONS` is never displayed
and no test catches it — check the id. A new section is §6.

## 2. Adjustable control-plane setting

1. **Config field** — `python/src/midstream/config.py`: a default constant, a
   field on the frozen `ControlPlaneConfig`, and a parse in `get_config()` with the env var
   `CONTROL_PLANE_<KEY_IN_CAPS>` and a `minimum` no looser than the page's bound:

   ```python
   session_refresh_minutes=_parse_int_env(
       os.getenv("CONTROL_PLANE_SESSION_REFRESH_MINUTES"),
       default=SESSION_REFRESH_MINUTES,
       minimum=1,
       name="CONTROL_PLANE_SESSION_REFRESH_MINUTES",
   ),
   ```

2. **Read it per event** from the resolved config. Handlers already do
   `cfg = resolve_config(settings_table) if config is None else config` and pass `config=cfg` down;
   services use `cfg = get_config() if config is None else config` and read `cfg.<field>`.

3. **Catalog entry** — `settings/catalog.py`, `CONTROL_PLANE_SETTINGS`:

   ```python
   _cp(
       "session_refresh_minutes",          # = the ControlPlaneConfig field; env var derived from it
       section="security",
       label="Session renewal interval",   # shown as written, in the reader's terms
       kind="integer",                     # boolean | integer | gcs_uri | quantity
       unit="minutes",
       minimum=1,
       maximum=60,
       advanced=True,                      # optional: folded under Advanced
       description="How often a client renews an active session. …",
       effect="new sign-ins and renewed sessions",   # when not "at once"
   ),
   ```

   Pass `env_var=` only when the variable does not follow the `CONTROL_PLANE_` rule
   (e.g. `PROTOCOL_LOGGING_ENABLED`). `nullable=True` lets a `gcs_uri` be empty.
   A rule spanning settings goes in `validate_scope_values` (it receives every effective value of
   the scope) — raise `SettingValueError(message, key=<the setting to blame>)`. If two valid values
   can still disagree between an override and the deployment, also clamp in
   `settings/effective.py::apply_control_plane_overrides`, as replicas do.

4. **Wiring** — if a handler or view that did not read configuration before now does, bind the
   `settings` table to its operation (`- name: settings / paramName: settings_table`), pass it to
   the handler, add the entry point to `CONFIG_RESOLVING_ENTRYPOINTS` in
   `python/tests/test_agent_manifests.py`, and patch the binding in `charts/midstream/values.yaml`
   (`<release>-settings`). `SettingsWiringTests` fails otherwise.

5. **Never into the runtime spec.** No overridable field may be read while rendering a runtime;
   it would change the spec hash and the sweep would restart every runtime.

6. **Tests** —
   - `python/tests/test_settings.py` checks every catalog key is a config field read from its env
     var (it feeds `7`, or the setting's minimum when that is higher) and that primary settings
     have descriptions. Add a case for any cross-field rule.
   - `python/tests/test_runtime_settings.py` — add the key to the `overrides` dict in the
     spec-hash test; it asserts that dict covers the whole catalog.
   - Test the behaviour the setting changes in the module that owns it.

The console needs **no change**: it renders whatever the catalog describes.

## 3. Adjustable runtime setting

Runtimes cannot read the control plane's table; overrides are delivered to them
(`docs/handbook/17-settings.md` §17.7).

1. `runtime_app/config.py`: the `RuntimeAppConfig` field and its env var, read by
   `get_runtime_app_config()`.
2. `runtime_app/live_settings.py`: add it to `LIVE_SETTING_FIELDS` (`int` or `bool`) and
   `LIVE_SETTING_ENV_VARS`.
3. `settings/catalog.py`: an `_rt("<key>", section="runtimes", …)` entry in `RUNTIME_SETTINGS`
   (its env var comes from `LIVE_SETTING_ENV_VARS`). An unworkable combination goes in
   `validate_scope_values` (scope `runtime`), and the runtime must also tolerate it at its point of
   use — it keeps its environment's values rather than stop.
4. Read it at the point of use with `get_runtime_app_config()`, never once at start.
5. Tests: `test_settings.py` asserts `RUNTIME_SETTINGS` keys equal `LIVE_SETTING_FIELDS`;
   `test_runtime_settings.py` covers delivery and sanitising.

## 4. Adjustable console setting

1. `console/server/config.ts` — add it to `tunableDeployment` with `int` / `bool` / `str` and its env
   var `CONSOLE_<NAME>`. It becomes a getter on `config`.
2. Read `config.<key>` where it is used. Long-lived objects take a getter (`() => config.<key>`),
   as the topic catalog does.
3. `console/server/settings/catalog.ts` — a `CONSOLE_SETTINGS` entry:

   ```ts
   {
     key: 'sessionCheckMs',
     envVar: 'CONSOLE_SESSION_CHECK_MS',
     section: 'security',
     label: 'Session check interval',
     description: 'How long the console trusts a session it checked with the control plane before checking it again. …',
     kind: 'integer',
     unit: 'ms',
     minimum: 0,
     maximum: MINUTE,
     advanced: true,
   },
   ```

4. `server/settings/catalog.test.ts` fails until `config.ts` and the catalog agree. Add a route or
   unit test for the behaviour it changes.
5. Retiring one: remove it from both, and add its key to `RETIRED_SETTINGS` in
   `server/settings/catalog.ts` so a stored override is ignored silently instead of logged as
   unknown. (The control plane needs nothing: it applies only keys its catalog has.)

## 5. Fixed (deployment-only) setting

### 5.1 Control plane or runtime

Read it from the environment as in §2.1 (or `runtime_app/config.py`), with no catalog entry. To
show it on the Deployment section, add a `DeploymentSetting(key, component, label, env_var,
description)` to `DEPLOYMENT_SETTINGS` in `settings/catalog.py`; its value comes from
`web/settings.py::_deployment_value`, which reads `getattr(config, key)` — add a branch there if the
value lives elsewhere.

### 5.2 Console

Read it in `config.ts` outside `tunableDeployment` (with `str` / `int` / `bool` / `optional`), and
add an `entry(...)` to `consoleDeployment()` in `console/server/routes/settings.ts`.

### 5.3 Credentials

Never a `ControlPlaneConfig` field and never a value on any page. Follow
`identity/secrets.py`: read once from the environment into its own frozen object, refuse weak
values with a logged error, and report only *configured / not configured* in `_deployment_value`
(console: `config.x ? 'configured' : 'not configured'`). In the chart, take it from a Secret with
`secretKeyRef` (`optional: true` if the app must start without it), the Secret named by a value.
Never commit a value; never print one.

## 6. A new section

`console/src/features/settings/sections.ts`, in `SETTINGS_SECTIONS` where it should appear:

```ts
{
  id: 'security',
  label: 'Security',
  summary: 'Passwords, sessions, lockouts and how long the audit log is kept.',
  group: 'access',          // account | access | platform | console | system
  icon: KeyRound,
  audience: 'admin',
  kind: 'settings',         // settings sections read and save settings
  permission: 'settings.view',
},
```

Settings sections are read-only without `settings.manage`; keep `permission: 'settings.view'`
unless the section is someone's own (the Account section uses `null`).

## 7. The chart

An adjustable setting needs no chart change — its default is in code. Add a value and set the env var
in `charts/midstream/templates/control-plane/app.yaml` (or the runtime spec helper in `_helpers.tpl`)
only when a deployment needs to choose it. Never put an overridable control-plane setting into the
runtime spec env (it changes the spec hash).

## 8. Documentation

- `docs/handbook/17-settings.md` §17.3 tables (key, env var, default, bounds, effect), and §17.3
  "Never overridable" for fixed ones.
- `docs/handbook/03-configuration-reference.md` §3.2.1 field table and §3.2.5 list.
- Console: `console/docs/03-configuration-reference.md` (the variable, and §3.10 for adjustable ones).
- The doc that owns the behaviour (e.g. `18-identity-and-access.md` §18.11 for security settings).

## 9. Verify

```bash
python -m unittest python.tests.test_settings python.tests.test_runtime_settings python.tests.test_agent_manifests -v
python -m unittest discover -s python/tests -p 'test_*.py'
python -m compileall -q python/src
helm lint charts/midstream -f charts/midstream/ci/test-values.yaml
cd console && npm test && npm run typecheck && npm run build
```

Then open the Settings page: the setting is in its section with its label, unit and bounds; an
out-of-range value is refused under the field; a saved override shows *Changed by …*, takes effect
where it should, and **Reset** returns it to the deployment value.
