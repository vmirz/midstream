import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Check, Copy, KeyRound, Plus, TriangleAlert } from 'lucide-react';
import { useState, type FormEvent, type ReactNode } from 'react';

import { ConfirmDialog } from './ConfirmDialog';
import { RelativeTime } from '@/components/Inline';
import { Dialog } from '@/components/ui/Dialog';
import { useToast } from '@/components/ui/Toast';
import { Badge, Button, EmptyState, Field, Input, Panel, PanelHeader, Select, Skeleton } from '@/components/ui/primitives';
import { ApiError } from '@/lib/api';
import { formatTimestamp } from '@/lib/format';
import { useApi, useManyDomains } from '@/lib/domains';
import type { CpAccessToken, CpAccessTokenCreated, CpAccessTokenList } from '@shared/types';

const LIFETIMES = [7, 30, 90, 180, 365];

/** `catalog.read` → `Read`: what a token's permission means, as short as a badge allows. */
const PERMISSION_BADGES: Record<string, string> = {
  'catalog.read': 'Read',
  'catalog.write': 'Commit',
};

/**
 * Access tokens: how DuckDB, Spark, Trino or PyIceberg sign in to the Iceberg REST catalog.
 *
 * A token stands in for you with at most the catalog permissions you choose, on the datasets you
 * choose — and never more than you hold yourself, on each dataset — so revoking it, or losing access
 * to a dataset, takes effect on its next request. Its text is shown once, when it is made; the
 * control plane keeps only a hash.
 */
export function AccessTokens(): ReactNode {
  const api = useApi();
  const queryClient = useQueryClient();
  const toast = useToast();
  const tokens = useQuery({ queryKey: ['access-tokens'], queryFn: () => api.accessTokens() });
  const [creating, setCreating] = useState(false);
  const [created, setCreated] = useState<CpAccessTokenCreated | null>(null);
  const [revoking, setRevoking] = useState<CpAccessToken | null>(null);

  // A change is applied by the identity agent a moment after the control plane accepts it.
  const refresh = (): void => {
    setTimeout(() => void queryClient.invalidateQueries({ queryKey: ['access-tokens'] }), 600);
  };
  const revoke = useMutation({
    mutationFn: (token: CpAccessToken) => api.revokeAccessToken(token.token_id),
    onSuccess: (revoked, token) => {
      const missed = Object.entries(revoked.domains ?? {}).filter(([, result]) => result === 'failed').map(([domain]) => domain);
      toast.success(
        'Token revoked',
        missed.length
          ? `“${token.name}” stops working on its next request here; ${missed.join(', ')} could not be told yet and will be within five minutes.`
          : `“${token.name}” stops working on its next request, in every domain.`,
      );
      setRevoking(null);
      refresh();
    },
    onError: (error) => toast.error('The token could not be revoked', error instanceof Error ? error.message : String(error)),
  });

  const list = tokens.data?.tokens ?? [];
  const active = list.filter((token) => token.active).length;
  return (
    <Panel>
      <PanelHeader
        title="Access tokens"
        description="For engines that read the Iceberg catalog: DuckDB, Spark, Trino, PyIceberg. A token acts as you, with only the permissions it carries."
        actions={
          <Button size="sm" onClick={() => setCreating(true)} disabled={!tokens.data || active >= (tokens.data?.limit ?? 0)}>
            <Plus className="size-3.5" />
            New token
          </Button>
        }
      />
      {tokens.isLoading ? (
        <div className="p-5">
          <Skeleton className="h-12" />
        </div>
      ) : tokens.isError ? (
        <p className="px-5 py-4 text-[12px] text-[var(--color-phase-failed)]">
          {tokens.error instanceof Error ? tokens.error.message : 'Your tokens could not be read.'}
        </p>
      ) : list.length === 0 ? (
        <EmptyState
          icon={<KeyRound className="size-6" />}
          title="No tokens yet"
          description="Make one for each tool or machine, so each can be revoked on its own."
        />
      ) : (
        <ul className="divide-y divide-[var(--color-line)]">
          {list.map((token) => (
            <li key={token.token_id} className="flex flex-wrap items-center gap-3 px-5 py-3">
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
                  {token.datasets?.length ? (
                    <>
                      Only <span className="numeric">{token.datasets.map((dataset) => dataset.name).join(', ')}</span>
                    </>
                  ) : (
                    'Every dataset you may read through the catalog'
                  )}
                </p>
                <p className="numeric mt-0.5 text-[12px] text-[var(--color-ink-muted)]">
                  {token.token_id} · made <RelativeTime value={token.created_at} />
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
                <Button size="sm" variant="ghost" onClick={() => setRevoking(token)}>
                  Revoke
                </Button>
              ) : null}
            </li>
          ))}
        </ul>
      )}

      {creating && tokens.data ? (
        <CreateTokenDialog
          options={tokens.data}
          onClose={() => setCreating(false)}
          onCreated={(token) => {
            setCreating(false);
            setCreated(token);
            refresh();
          }}
        />
      ) : null}
      {created ? <TokenReveal token={created} onClose={() => setCreated(null)} /> : null}
      <ConfirmDialog
        open={revoking !== null}
        title={`Revoke “${revoking?.name ?? ''}”?`}
        description="Every tool using it is refused from its next request. This cannot be undone; make a new token to reconnect."
        confirmLabel="Revoke token"
        danger
        pending={revoke.isPending}
        onConfirm={() => revoking && revoke.mutate(revoking)}
        onClose={() => setRevoking(null)}
      />
    </Panel>
  );
}

function CreateTokenDialog({
  options,
  onClose,
  onCreated,
}: {
  options: CpAccessTokenList;
  onClose: () => void;
  onCreated: (token: CpAccessTokenCreated) => void;
}): ReactNode {
  const api = useApi();
  const [name, setName] = useState('');
  const [permissions, setPermissions] = useState<string[]>(['catalog.read']);
  const [days, setDays] = useState(options.default_expires_in_days);
  // Empty: every dataset you may read through the catalog, now and later.
  const [datasets, setDatasets] = useState<string[]>([]);
  const [limited, setLimited] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const create = useMutation({
    mutationFn: () =>
      api.createAccessToken({ name: name.trim(), permissions, expires_in_days: days, ...(limited ? { datasets } : {}) }),
    onSuccess: onCreated,
    onError: (cause) => setError(cause instanceof ApiError ? cause.message : String(cause)),
  });
  const available = options.datasets ?? [];
  const lifetimes = [...new Set([...LIFETIMES, options.default_expires_in_days])].filter((value) => value <= options.max_expires_in_days).sort((a, b) => a - b);

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
      description="It acts as you, with only the permissions you give it here."
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button
            variant="primary"
            type="submit"
            form="create-token"
            loading={create.isPending}
            disabled={permissions.length === 0 || (limited && datasets.length === 0)}
          >
            Make token
          </Button>
        </>
      }
    >
      <form id="create-token" onSubmit={submit} className="space-y-4">
        <Field label="Name" required hint="Where it will be used, so you know which to revoke: “DuckDB on my laptop”." error={error}>
          <Input value={name} onChange={(event) => setName(event.target.value)} maxLength={64} autoFocus placeholder="DuckDB on my laptop" />
        </Field>
        <fieldset className="space-y-2">
          <legend className="mb-1 text-[12px] font-medium">Permissions</legend>
          {options.permissions.map((permission) => (
            <label key={permission.key} className="flex items-start gap-2.5">
              <input
                type="checkbox"
                className="mt-[3px] size-3.5 shrink-0 accent-[var(--color-accent)]"
                disabled={!permission.granted}
                checked={permissions.includes(permission.key)}
                onChange={(event) =>
                  setPermissions((current) =>
                    event.target.checked ? [...new Set([...current, permission.key])] : current.filter((key) => key !== permission.key),
                  )
                }
              />
              <span>
                <span className="block text-[13px]">{permission.label}</span>
                <span className="block text-[12px] text-[var(--color-ink-muted)]">
                  {permission.key === 'catalog.write'
                    ? 'Lets the engine commit to tables, not just read them. Leave it off unless the tool must write.'
                    : 'List and load tables through the catalog.'}
                  {permission.granted ? '' : ' You do not have this permission, so a token cannot carry it.'}
                </span>
              </span>
            </label>
          ))}
        </fieldset>
        <fieldset className="space-y-2">
          <legend className="mb-1 text-[12px] font-medium">Datasets</legend>
          <label className="flex items-start gap-2.5">
            <input type="radio" name="token-datasets" className="mt-[3px] size-3.5 accent-[var(--color-accent)]" checked={!limited} onChange={() => setLimited(false)} />
            <span className="text-[13px]">
              Every dataset you may read
              <span className="block text-[12px] text-[var(--color-ink-muted)]">Including ones you are given access to later.</span>
            </span>
          </label>
          <label className="flex items-start gap-2.5">
            <input
              type="radio"
              name="token-datasets"
              className="mt-[3px] size-3.5 accent-[var(--color-accent)]"
              checked={limited}
              disabled={available.length === 0}
              onChange={() => setLimited(true)}
            />
            <span className="text-[13px]">
              Only these datasets
              <span className="block text-[12px] text-[var(--color-ink-muted)]">
                {available.length === 0 ? 'You may read no dataset through the catalog yet.' : `At most ${options.max_datasets ?? 100}.`}
              </span>
            </span>
          </label>
          {limited ? (
            <div className="ml-6 flex max-h-40 flex-wrap gap-1.5 overflow-y-auto">
              {available.map((dataset) => {
                const checked = datasets.includes(dataset.name);
                return (
                  <label
                    key={dataset.dataset_id}
                    className={`flex cursor-pointer items-center gap-1.5 rounded-md border px-2 py-1 text-[12px] ${
                      checked ? 'border-[var(--color-accent)] text-[var(--color-ink)]' : 'border-[var(--color-line)] text-[var(--color-ink-muted)]'
                    }`}
                    title={dataset.permissions.includes('catalog.write') ? 'You may read and commit here' : 'You may read here'}
                  >
                    <input
                      type="checkbox"
                      className="size-3 accent-[var(--color-accent)]"
                      checked={checked}
                      onChange={(event) =>
                        setDatasets((current) => (event.target.checked ? [...current, dataset.name] : current.filter((name) => name !== dataset.name)))
                      }
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

/** A token just made: its text, shown this once, and where engines connect with it. */
export function TokenReveal({ token, onClose }: { token: CpAccessTokenCreated; onClose: () => void }): ReactNode {
  const api = useApi();
  const many = useManyDomains();
  const [copied, setCopied] = useState(false);
  const connection = useQuery({ queryKey: ['catalog-connection'], queryFn: () => api.catalogConnection(), retry: 0 });
  return (
    <Dialog
      open
      onClose={onClose}
      title="Your new access token"
      description={`“${token.name}”. Copy it now: it is not shown again.`}
      footer={
        <Button variant="primary" onClick={onClose}>
          Done
        </Button>
      }
    >
      <div className="space-y-4">
        <div className="flex items-center gap-2 rounded-[var(--radius-control)] border border-[var(--color-line-strong)] bg-[var(--color-raised)] px-3 py-2">
          <code className="numeric min-w-0 flex-1 text-[12px] break-all" data-role="token-text">
            {token.token}
          </code>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => {
              void navigator.clipboard.writeText(token.token);
              setCopied(true);
            }}
          >
            {copied ? <Check className="size-3.5" /> : <Copy className="size-3.5" />}
            {copied ? 'Copied' : 'Copy'}
          </Button>
        </div>
        <p className="flex items-start gap-2 text-[12px] text-[var(--color-ink-muted)]">
          <TriangleAlert className="mt-0.5 size-3.5 shrink-0 text-[var(--color-phase-paused)]" />
          Anyone holding it can read the catalog as you until it expires on {formatTimestamp(token.expires_at)}. Keep it in
          a secret store, not in code.
        </p>
        {connection.data ? (
          <div className="space-y-1">
            <p className="text-[12px] font-medium">{many ? 'Catalog URI, every domain' : 'Catalog URI'}</p>
            <code className="numeric block rounded-[var(--radius-control)] bg-[var(--color-raised)] px-3 py-2 text-[12px] break-all">
              {many ? connection.data.federatedUri : connection.data.uri}
            </code>
            {many ? (
              <p className="text-[12px] text-[var(--color-ink-muted)]">
                Its namespaces are <span className="numeric">&lt;domain&gt;.&lt;dataset&gt;</span>. An engine that reads one level
                of namespaces connects to a domain&rsquo;s own catalog instead, such as{' '}
                <span className="numeric break-all">{connection.data.uri}</span>.
              </p>
            ) : null}
            <p className="text-[12px] text-[var(--color-ink-muted)]">
              Each materialization’s Table tab has snippets for DuckDB, PyIceberg, Spark and Trino.
            </p>
          </div>
        ) : null}
      </div>
    </Dialog>
  );
}
