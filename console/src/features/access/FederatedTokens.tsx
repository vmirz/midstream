import { useMutation, useQuery } from '@tanstack/react-query';
import { KeyRound, Plus } from 'lucide-react';
import { useMemo, useState, type FormEvent, type ReactNode } from 'react';

import { TokenReveal } from './AccessTokens';
import { ConfirmDialog } from './ConfirmDialog';
import { RelativeTime } from '@/components/Inline';
import { Dialog } from '@/components/ui/Dialog';
import { useToast } from '@/components/ui/Toast';
import { Badge, Button, EmptyState, Field, Input, Panel, PanelHeader, Select, Skeleton } from '@/components/ui/primitives';
import { ApiError, domainApi } from '@/lib/api';
import { useConsoleAuth } from '@/lib/auth';
import { DomainProvider, useConsoleConfig } from '@/lib/domains';
import { formatTimestamp } from '@/lib/format';
import { rootQueryClient } from '@/lib/queryClient';
import type { ConsoleDomain, CpAccessToken, CpAccessTokenCreated, CpAccessTokenList } from '@shared/types';

const LIFETIMES = [7, 30, 90, 180, 365];
const PERMISSION_BADGES: Record<string, string> = { 'catalog.read': 'Read', 'catalog.write': 'Commit' };
const KEY = ['federated-tokens'] as const;

/** Each signed-in domain's tokens, and what a new token made there may carry. */
interface DomainTokens {
  domain: ConsoleDomain;
  list: CpAccessTokenList | null;
  error: string | null;
}

/**
 * Access tokens, across domains (handbook 20). A token is made in one domain and is good in every
 * domain where its owner has an account, with what they hold in each; each domain lists the tokens
 * made in it. Revoking one reaches every domain.
 */
export function FederatedTokens(): ReactNode {
  const config = useConsoleConfig();
  const auth = useConsoleAuth();
  const toast = useToast();
  const signedIn = (config.data?.domains ?? []).filter((domain) => domain.status !== 'misconfigured' && auth.forDomain(domain.id).signedIn);
  const ids = signedIn.map((domain) => domain.id).join(',');
  const tokens = useQuery(
    {
      queryKey: [...KEY, ids],
      enabled: signedIn.length > 0,
      queryFn: async (): Promise<DomainTokens[]> =>
        Promise.all(
          signedIn.map(async (domain) => {
            try {
              return { domain, list: await domainApi(domain.id).accessTokens(), error: null };
            } catch (cause) {
              return { domain, list: null, error: cause instanceof Error ? cause.message : String(cause) };
            }
          }),
        ),
    },
    rootQueryClient,
  );
  const [creating, setCreating] = useState(false);
  const [created, setCreated] = useState<{ token: CpAccessTokenCreated; domain: ConsoleDomain } | null>(null);
  const [revoking, setRevoking] = useState<{ token: CpAccessToken; domain: ConsoleDomain } | null>(null);
  const refresh = (): void => void rootQueryClient.invalidateQueries({ queryKey: KEY });

  const revoke = useMutation({
    mutationFn: ({ token, domain }: { token: CpAccessToken; domain: ConsoleDomain }) => domainApi(domain.id).revokeAccessToken(token.token_id),
    onSuccess: (revoked, { token }) => {
      const missed = Object.entries(revoked.domains ?? {}).filter(([, result]) => result === 'failed').map(([domain]) => domain);
      toast.success(
        'Token revoked',
        missed.length
          ? `“${token.name}” stops working now where it was made; ${missed.join(', ')} could not be told yet and will be within five minutes.`
          : `“${token.name}” stops working on its next request, in every domain.`,
      );
      setRevoking(null);
      refresh();
    },
    onError: (error) => toast.error('The token could not be revoked', error instanceof Error ? error.message : String(error)),
  });

  const entries = tokens.data ?? [];
  const rows = entries.flatMap((entry) => (entry.list?.tokens ?? []).map((token) => ({ token, domain: entry.domain })));
  rows.sort((left, right) => right.token.created_at.localeCompare(left.token.created_at));
  const mintable = entries.filter((entry) => entry.list?.permissions.some((permission) => permission.key === 'catalog.read' && permission.granted));

  return (
    <Panel>
      <PanelHeader
        title="Access tokens"
        description="For engines that read the Iceberg catalog: DuckDB, Spark, Trino, PyIceberg. A token works in every domain where you have an account, with what you hold there."
        actions={
          <Button size="sm" onClick={() => setCreating(true)} disabled={mintable.length === 0}>
            <Plus className="size-3.5" />
            New token
          </Button>
        }
      />
      {tokens.isLoading ? (
        <div className="p-5">
          <Skeleton className="h-12" />
        </div>
      ) : rows.length === 0 ? (
        <EmptyState icon={<KeyRound className="size-6" />} title="No tokens yet" description="Make one for each tool or machine, so each can be revoked on its own." />
      ) : (
        <ul className="divide-y divide-[var(--color-line)]">
          {rows.map(({ token, domain }) => (
            <li key={`${domain.id}:${token.token_id}`} className="flex flex-wrap items-center gap-3 px-5 py-3">
              <div className="min-w-0 flex-1">
                <p className="flex flex-wrap items-center gap-2 text-[13px] font-medium">
                  {token.name}
                  {token.permissions.map((permission) => (
                    <Badge key={permission} tone={permission === 'catalog.write' ? 'paused' : 'neutral'}>
                      {PERMISSION_BADGES[permission] ?? permission}
                    </Badge>
                  ))}
                  {!token.active ? <Badge tone="failed">{token.revoked_at ? 'revoked' : 'expired'}</Badge> : null}
                </p>
                <p className="mt-0.5 text-[12px] text-[var(--color-ink-muted)]">
                  {token.datasets.length ? (
                    <>
                      Only <span className="numeric">{token.datasets.map((dataset) => dataset.name).join(', ')}</span>
                    </>
                  ) : (
                    'Every dataset you may read, in every domain'
                  )}
                </p>
                <p className="numeric mt-0.5 text-[12px] text-[var(--color-ink-muted)]">
                  made in {domain.name} · {token.token_id} · <RelativeTime value={token.created_at} />
                  {token.revoked_at ? (
                    <>
                      {' '}
                      · revoked <RelativeTime value={token.revoked_at} />
                    </>
                  ) : token.expires_at ? (
                    ` · ${token.active ? 'expires' : 'expired'} ${formatTimestamp(token.expires_at)}`
                  ) : null}
                </p>
              </div>
              {token.active ? (
                <Button size="sm" variant="ghost" onClick={() => setRevoking({ token, domain })}>
                  Revoke
                </Button>
              ) : null}
            </li>
          ))}
        </ul>
      )}
      {entries.filter((entry) => entry.error).map((entry) => (
        <p key={entry.domain.id} className="border-t border-[var(--color-line)] px-5 py-2 text-[12px] text-[var(--color-phase-paused)]">
          {entry.domain.name}&rsquo;s tokens could not be read: {entry.error}
        </p>
      ))}

      {creating ? (
        <NewTokenDialog
          entries={entries}
          onClose={() => setCreating(false)}
          onCreated={(token, domain) => {
            setCreating(false);
            setCreated({ token, domain });
            refresh();
          }}
        />
      ) : null}
      {created ? (
        <DomainProvider domain={created.domain}>
          <TokenReveal token={created.token} onClose={() => setCreated(null)} />
        </DomainProvider>
      ) : null}
      <ConfirmDialog
        open={revoking !== null}
        title={`Revoke “${revoking?.token.name ?? ''}”?`}
        description="Every tool using it is refused from its next request, in every domain. This cannot be undone; make a new token to reconnect."
        confirmLabel="Revoke token"
        danger
        pending={revoke.isPending}
        onConfirm={() => revoking && revoke.mutate(revoking)}
        onClose={() => setRevoking(null)}
      />
    </Panel>
  );
}

function NewTokenDialog({
  entries,
  onClose,
  onCreated,
}: {
  entries: DomainTokens[];
  onClose: () => void;
  onCreated: (token: CpAccessTokenCreated, domain: ConsoleDomain) => void;
}): ReactNode {
  const readable = entries.filter((entry) => entry.list !== null);
  const mintable = readable.filter((entry) => entry.list!.permissions.some((permission) => permission.key === 'catalog.read' && permission.granted));
  const [minter, setMinter] = useState(mintable[0]?.domain.id ?? '');
  const chosen = mintable.find((entry) => entry.domain.id === minter) ?? mintable[0];
  const options = chosen?.list ?? null;
  const [name, setName] = useState('');
  const [permissions, setPermissions] = useState<string[]>(['catalog.read']);
  const [days, setDays] = useState(options?.default_expires_in_days ?? 90);
  const [limited, setLimited] = useState(false);
  const [datasets, setDatasets] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  // Every dataset the person reads through a catalog, in every domain they are signed in to.
  const available = useMemo(
    () => readable.flatMap((entry) => (entry.list?.datasets ?? []).map((dataset) => ({ ...dataset, domainName: entry.domain.name }))),
    [readable],
  );
  const create = useMutation({
    mutationFn: () =>
      domainApi(chosen!.domain.id).createAccessToken({
        name: name.trim(),
        permissions,
        expires_in_days: days,
        ...(limited ? { datasets } : {}),
      }),
    onSuccess: (token) => onCreated(token, chosen!.domain),
    onError: (cause) => setError(cause instanceof ApiError ? cause.message : String(cause)),
  });
  const lifetimes = [...new Set([...LIFETIMES, options?.default_expires_in_days ?? 90])]
    .filter((value) => value <= (options?.max_expires_in_days ?? 365))
    .sort((a, b) => a - b);

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    setError(null);
    if (!name.trim()) {
      setError('Name the token after where it will be used');
      return;
    }
    create.mutate();
  };

  return (
    <Dialog
      open
      onClose={onClose}
      title="New access token"
      description="It acts as you, with only the permissions you give it here, in every domain where you have an account."
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" type="submit" form="create-federated-token" loading={create.isPending} disabled={!chosen || permissions.length === 0 || (limited && datasets.length === 0)}>
            Make token
          </Button>
        </>
      }
    >
      <form id="create-federated-token" onSubmit={submit} className="space-y-4">
        <Field label="Name" required hint="Where it will be used, so you know which to revoke: “DuckDB on my laptop”." error={error}>
          <Input value={name} onChange={(event) => setName(event.target.value)} maxLength={64} autoFocus placeholder="DuckDB on my laptop" />
        </Field>
        <Field label="Made in" hint="The domain that lists it and revokes it. It works in every domain either way.">
          <Select value={chosen?.domain.id ?? ''} onChange={(event) => setMinter(event.target.value)}>
            {mintable.map((entry) => (
              <option key={entry.domain.id} value={entry.domain.id}>
                {entry.domain.name}
              </option>
            ))}
          </Select>
        </Field>
        <fieldset className="space-y-2">
          <legend className="mb-1 text-[12px] font-medium">Permissions</legend>
          {(options?.permissions ?? []).map((permission) => (
            <label key={permission.key} className="flex items-start gap-2.5">
              <input
                type="checkbox"
                className="mt-[3px] size-3.5 shrink-0 accent-[var(--color-accent)]"
                disabled={!permission.granted}
                checked={permissions.includes(permission.key)}
                onChange={(event) =>
                  setPermissions((current) => (event.target.checked ? [...new Set([...current, permission.key])] : current.filter((key) => key !== permission.key)))
                }
              />
              <span>
                <span className="block text-[13px]">{permission.label}</span>
                <span className="block text-[12px] text-[var(--color-ink-muted)]">
                  {permission.key === 'catalog.write' ? 'Lets the engine commit to tables. Leave it off unless the tool must write.' : 'List and load tables through the catalog.'}
                  {permission.granted ? '' : ` You do not have it in ${chosen?.domain.name ?? 'this domain'}.`}
                </span>
              </span>
            </label>
          ))}
        </fieldset>
        <fieldset className="space-y-2">
          <legend className="mb-1 text-[12px] font-medium">Datasets</legend>
          <label className="flex items-start gap-2.5">
            <input type="radio" name="federated-token-datasets" className="mt-[3px] size-3.5 accent-[var(--color-accent)]" checked={!limited} onChange={() => setLimited(false)} />
            <span className="text-[13px]">
              Every dataset you may read, in every domain
              <span className="block text-[12px] text-[var(--color-ink-muted)]">Including ones you are given access to later.</span>
            </span>
          </label>
          <label className="flex items-start gap-2.5">
            <input
              type="radio"
              name="federated-token-datasets"
              className="mt-[3px] size-3.5 accent-[var(--color-accent)]"
              checked={limited}
              disabled={available.length === 0}
              onChange={() => setLimited(true)}
            />
            <span className="text-[13px]">
              Only these datasets
              <span className="block text-[12px] text-[var(--color-ink-muted)]">
                {available.length === 0 ? 'You may read no dataset through the catalog yet.' : `At most ${options?.max_datasets ?? 100}, in any domain.`}
              </span>
            </span>
          </label>
          {limited ? (
            <div className="ml-6 flex max-h-48 flex-wrap gap-1.5 overflow-y-auto">
              {available.map((dataset) => {
                const checked = datasets.includes(dataset.name);
                return (
                  <label
                    key={dataset.name}
                    className={`flex cursor-pointer items-center gap-1.5 rounded-md border px-2 py-1 text-[12px] ${
                      checked ? 'border-[var(--color-accent)] text-[var(--color-ink)]' : 'border-[var(--color-line)] text-[var(--color-ink-muted)]'
                    }`}
                    title={`${dataset.domainName}: ${dataset.permissions.includes('catalog.write') ? 'you may read and commit here' : 'you may read here'}`}
                  >
                    <input
                      type="checkbox"
                      className="size-3 accent-[var(--color-accent)]"
                      checked={checked}
                      onChange={(event) => setDatasets((current) => (event.target.checked ? [...current, dataset.name] : current.filter((item) => item !== dataset.name)))}
                    />
                    <span className="numeric">{dataset.name}</span>
                  </label>
                );
              })}
            </div>
          ) : null}
        </fieldset>
        <Field label="Expires" hint="A token stops working when it expires, and can be revoked any time before.">
          <Select value={days} onChange={(event) => setDays(Number(event.target.value))}>
            {lifetimes.map((value) => (
              <option key={value} value={value}>
                In {value} days
              </option>
            ))}
          </Select>
        </Field>
      </form>
    </Dialog>
  );
}
