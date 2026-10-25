import { useState, type FormEvent, type ReactNode } from 'react';

import { PasswordInput } from './PasswordInput';
import { Button, Field } from '@/components/ui/primitives';
import { useToast } from '@/components/ui/Toast';
import { ApiError, api } from '@/lib/api';
import { useAuth } from '@/lib/auth';

/** The password rules, as the control plane states them; 12 characters when it does not say. */
export function usePasswordMinimum(): number {
  return useAuth().state?.passwordPolicy?.min_length ?? 12;
}

/**
 * Change your own password. Every other session of yours ends once it applies; this browser is
 * signed straight back in with the new password.
 */
export function ChangePasswordForm({ onChanged, submitLabel = 'Change password' }: { onChanged?: () => void; submitLabel?: string }): ReactNode {
  const auth = useAuth();
  const toast = useToast();
  const minimum = usePasswordMinimum();
  const [current, setCurrent] = useState('');
  const [next, setNext] = useState('');
  const [confirm, setConfirm] = useState('');
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [submitting, setSubmitting] = useState(false);

  const problems: Record<string, string> = {};
  if (next && next.length < minimum) problems.new_password = `At least ${minimum} characters`;
  if (next && current && next === current) problems.new_password = 'Choose a password different from your current one';
  if (confirm && confirm !== next) problems.confirm = 'Does not match the new password';
  const ready = Boolean(current && next && confirm) && Object.keys(problems).length === 0;

  const submit = async (event: FormEvent): Promise<void> => {
    event.preventDefault();
    if (!ready) return;
    setSubmitting(true);
    setErrors({});
    try {
      const result = await api.changePassword(current, next);
      auth.adopt(result.session);
      setCurrent('');
      setNext('');
      setConfirm('');
      if (result.status === 'pending') {
        toast.success('Password change accepted', 'It applies in a moment; sign in with the new password if you are asked to.');
      } else {
        toast.success('Password changed', 'Your other sessions were signed out.');
      }
      onChanged?.();
    } catch (cause) {
      if (cause instanceof ApiError && typeof cause.details.field === 'string') {
        setErrors({ [cause.details.field]: cause.message });
      } else {
        toast.error('Password was not changed', cause instanceof Error ? cause.message : String(cause));
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <form onSubmit={(event) => void submit(event)} className="space-y-4" noValidate>
      <Field label="Current password" error={errors.current_password}>
        <PasswordInput autoComplete="current-password" value={current} onChange={(event) => setCurrent(event.target.value)} />
      </Field>
      <Field
        label="New password"
        error={errors.new_password ?? problems.new_password}
        hint={`At least ${minimum} characters. A few unrelated words make a strong one.`}
      >
        <PasswordInput autoComplete="new-password" value={next} onChange={(event) => setNext(event.target.value)} />
      </Field>
      <Field label="Confirm new password" error={problems.confirm}>
        <PasswordInput autoComplete="new-password" value={confirm} onChange={(event) => setConfirm(event.target.value)} />
      </Field>
      <Button type="submit" variant="primary" loading={submitting} disabled={!ready}>
        {submitLabel}
      </Button>
    </form>
  );
}
