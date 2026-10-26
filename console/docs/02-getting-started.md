# 02 — Getting Started

## 2.1 Prerequisites

| Requirement | Why |
|-------------|-----|
| Node ≥ 24 | The server runs TypeScript directly via native type stripping. Node 22 will fail on `.ts` entry points. |
| npm ≥ 10 | Lockfile v3 and `overrides` support. |
| Network access to a control plane | Everything except the shell of the UI requires it. |
| Google ADC credentials (optional) | Only needed for the Data tab. Without them the Data tab reports that object storage is unreachable; every other feature works. |

Check your toolchain:

```bash
node -v   # must be >= 24
npm -v
```

## 2.2 First run

```bash
cd console
npm install
cp .env.example .env
```

With no registry in `.env`, the console serves one domain, `local`, at `http://localhost:6065`. For
the dev cluster, port-forward its control plane there:

```bash
kubectl -n midstream port-forward svc/midstream-control-plane-app-api 6065:6065
```

To serve several domains, give `CONSOLE_DOMAINS` (or `CONSOLE_DOMAINS_FILE`) a registry and each
domain's service token (03 §3.3).

Then:

```bash
npm run dev
```

This runs two processes via `concurrently`:

| Process | Command | Port |
|---------|---------|------|
| `server` | `node --watch --env-file-if-exists=.env server/main.ts` | 8787 |
| `client` | `vite` | 5173 |

Open **http://localhost:5173**. The Vite dev server proxies `/api` to `:8787`, including the SSE
streams — the proxy is explicitly configured not to buffer `text/event-stream` responses
(`vite.config.ts`).

### Signing in

Without a session the console shows what a guest may see — by default the materialization list
and status — with **Sign in** in the sidebar. Signing in with a password needs an account in the
domain chosen; through a provider, the console's registration with it (03 §3.3a). On a fresh control plane, the first administrator is the bootstrap account from its
`<release>-auth` Secret, who chooses a new password at first sign-in and can then add people
under **Settings → Users** ([18 — Identity and access](../../docs/handbook/18-identity-and-access.md)).

For the live views to show details, give the console each domain's service token:

```bash
# console/.env
CONSOLE_DOMAIN_LOCAL_SERVICE_TOKEN=<the control plane's AUTH_SERVICE_TOKEN>
```

Without it the server logs a warning, and the fleet and detail streams read as a guest.

## 2.3 Running only one half

```bash
npm run dev:server   # BFF only, on :8787
npm run dev:client   # Vite only, on :5173 (expects a server on :8787)
```

`CONSOLE_SERVER_URL` overrides the Vite proxy target if the server runs elsewhere.

## 2.4 Production build and start

```bash
npm run build   # vite build → dist/
npm start       # node --env-file-if-exists=.env server/main.ts
```

`npm start` serves both the API and the built client on `:8787`.

> **Important operational detail.** `@fastify/static` is registered with `wildcard: false`, which
> means it registers a route per file discovered **at startup**. If you rebuild the client while
> the server is running, the new asset filenames will 404 until you restart the server. Always
> build first, then start. In a container this is automatic; in a local production-mode loop it
> is a real papercut.

## 2.5 Verification commands

```bash
npm run typecheck   # tsc -p tsconfig.app.json --noEmit && tsc -p tsconfig.server.json --noEmit
npm run build       # production client bundle
```

There is no unit test runner wired up. The verification approach is an end-to-end harness against
a stand-in control plane and a fake GCS endpoint; see
[14 — Testing and verification](./14-testing-and-verification.md) for the full reproducible
procedure.

## 2.6 Expected build output

```
dist/index.html                      ~1 kB
dist/assets/index-<hash>.css        ~31 kB   (gzip ~6.6 kB)
dist/assets/index-<hash>.js        ~375 kB   (gzip ~114 kB)
dist/assets/QueryEditor-<hash>.js  ~442 kB   (gzip ~145 kB)
```

The `QueryEditor` chunk is CodeMirror plus the SQL language mode. It is lazy-loaded and only
fetched when a user switches the Data tab into SQL mode, so it never affects first paint.
If you see it merged back into the main chunk, someone has replaced the `lazy()` import in
`src/features/data/DataTab.tsx` with a static one.

## 2.7 The development loop

| Change | What happens |
|--------|--------------|
| Any file under `src/` | Vite HMR updates the browser instantly |
| Any file under `server/` or `shared/` | `node --watch` restarts the server; browsers reconnect their SSE streams automatically |
| `styles.css` or Tailwind classes | Tailwind 4's Vite plugin recompiles on demand |
| `.env` | Requires a manual restart — env is read once at module load in `config.ts` |

Because `useEventStream` tracks connection state, a server restart during development shows
`Reconnecting…` in the header and then returns to `Live` without a page refresh.

## 2.8 Common first-run problems

| Symptom | Cause | Fix |
|---------|-------|-----|
| A banner says `The control plane of Local cannot be reached` | No control plane, or wrong URL | Start the port-forward; check the domain's `controlPlaneUrl` in `CONSOLE_DOMAINS` (unset, it is `http://localhost:6065`) |
| A banner says the registry sends a domain to a control plane that is another | The URL reaches another domain's control plane, or one whose `DOMAIN_ID` differs | Point the entry at the right control plane, or give the entry the id that control plane reports |
| `Cannot find module ... server/main.ts` | You ran `node server/main.ts` from the repository root | `cd console` first, or use an absolute path |
| Blank page and a MIME-type error in the console | The server was started before the client was built, or the client was rebuilt after the server started | `npm run build` then restart the server |
| Data tab: "Object storage is not reachable" | No ADC credentials, or the destination bucket is not readable | `gcloud auth application-default login`, or set `CONSOLE_DATA_ACCESS_ENABLED=false` to hide the feature |
| `PORT` already in use | A previous server is still running | `lsof -ti tcp:8787 \| xargs kill` |

## 2.9 Where to go next

- Understand what you are looking at → [11 — Feature reference](./11-feature-reference.md)
- Tune cadence, limits, or storage → [03 — Configuration reference](./03-configuration-reference.md)
- Add something → [15 — Extending the console](./15-extending-the-console.md)
