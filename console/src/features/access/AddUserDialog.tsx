import { Wand2 } from 'lucide-react';
import { useEffect, useState, type ReactNode } from 'react';
import { useNavigate } from 'react-router-dom';

import { usePasswordMinimum } from '../auth/ChangePasswordForm';
import { PasswordInput } from '../auth/PasswordInput';
import { GroupPicker } from './GroupPicker';
import { errorField, generatePassword, useIdentityChange } from './shared';
import { CopyButton } from '@/components/Inline';
import { Dialog } from '@/components/ui/Dialog';
import { Button, Field, Input } from '@/components/ui/primitives';
import { useApi, useDomainPath } from '@/lib/domains';

interface Draft {
  first_name: string;
  last_name: string;
  email: string;
  password: string;
  group_ids: string[];
}

const EMPTY: Draft = { first_name: '', last_name: '', email: '', password: '', group_ids: [] };

/**
 * Add a person. They sign in with the initial password given here, and must choose their own the
 * first time they do — so only they ever know the password they keep using.
 */
export function AddUserDialog({ open, onClose }: { open: boolean; onClose: () => void }): ReactNode {
  const domainPath = useDomainPath();
  const api = useApi();
  const navigate = useNavigate();
  const minimum = usePasswordMinimum();
  const [draft, setDraft] = useState<Draft>(EMPTY);

  const create = useIdentityChange((input: Draft) => api.createUser({ ...input, email: input.email.trim() }), {
    success: (input) => `${input.first_name} ${input.last_name} added`,
    failure: 'The user was not added',
    onApplied: (result) => {
      onClose();
      if (result.status === 'applied') navigate(domainPath(`/settings/users/${result.entity_id}`));
    },
  });
  const { reset } = create;

  useEffect(() => {
    if (!open) return;
    setDraft({ ...EMPTY, password: generatePassword() });
    reset();
  }, [open, reset]);

  const field = errorField(create.error);
  const fieldError = (name: string): string | null => (field === name ? (create.error as Error).message : null);
  const set = <K extends keyof Draft>(key: K, value: Draft[K]): void => setDraft((current) => ({ ...current, [key]: value }));
  const ready =
    draft.first_name.trim() && draft.last_name.trim() && draft.email.trim() && draft.password.length >= minimum;

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Add user"
      description="They sign in with their email and the initial password below, then choose their own."
      size="lg"
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" loading={create.isPending} disabled={!ready} onClick={() => create.mutate(draft)}>
            Add user
          </Button>
        </>
      }
    >
      <form
        className="space-y-4"
        onSubmit={(event) => {
          event.preventDefault();
          if (ready) create.mutate(draft);
        }}
      >
        <div className="grid gap-4 sm:grid-cols-2">
          <Field label="First name" required error={fieldError('first_name')}>
            <Input autoFocus value={draft.first_name} onChange={(event) => set('first_name', event.target.value)} autoComplete="off" />
          </Field>
          <Field label="Last name" required error={fieldError('last_name')}>
            <Input value={draft.last_name} onChange={(event) => set('last_name', event.target.value)} autoComplete="off" />
          </Field>
        </div>
        <Field label="Email" required error={fieldError('email')} hint="What they sign in with. It must be unique.">
          <Input type="email" value={draft.email} onChange={(event) => set('email', event.target.value)} autoComplete="off" placeholder="jane@example.com" />
        </Field>
        <Field
          label="Initial password"
          required
          error={fieldError('password') ?? (draft.password && draft.password.length < minimum ? `At least ${minimum} characters` : null)}
          hint="Share it with them privately. They must replace it when they first sign in."
        >
          <div className="flex items-center gap-2">
            <div className="min-w-0 flex-1">
              <PasswordInput value={draft.password} onChange={(event) => set('password', event.target.value)} autoComplete="new-password" />
            </div>
            <CopyButton value={draft.password} label="Copy password" className="h-9 px-2.5" />
            <Button type="button" size="sm" variant="secondary" onClick={() => set('password', generatePassword())} title="Generate a new password">
              <Wand2 className="size-3.5" />
              Generate
            </Button>
          </div>
        </Field>
        <div className="space-y-1.5">
          <span className="text-[13px] font-medium">Groups</span>
          <GroupPicker value={draft.group_ids} onChange={(ids) => set('group_ids', ids)} />
          {fieldError('group_ids') ? <span className="block text-[12px] text-[var(--color-phase-failed)]">{fieldError('group_ids')}</span> : null}
        </div>
      </form>
    </Dialog>
  );
}
