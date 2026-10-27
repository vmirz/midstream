import { useMutation } from '@tanstack/react-query';
import { useEffect, useState, type FormEvent, type ReactNode } from 'react';
import { useNavigate } from 'react-router-dom';

import { Dialog } from '@/components/ui/Dialog';
import { useToast } from '@/components/ui/Toast';
import { Button, Field, Input } from '@/components/ui/primitives';
import { ApiError } from '@/lib/api';
import { useMaterializationDefaults } from '@/lib/defaults';
import { useApi, useDomainPath } from '@/lib/domains';
import { datasetNameProblem } from '@shared/tableNames';

import { useRefreshDatasets } from './useDatasets';

/**
 * Make a dataset: a name SQL reads its tables in, fixed once made, and a description. Its maker owns
 * it, and everyone signed in can read it until its owners say otherwise.
 */
export function CreateDatasetDialog({ open, onClose }: { open: boolean; onClose: () => void }): ReactNode {
  const domainPath = useDomainPath();
  const api = useApi();
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [error, setError] = useState<string | null>(null);
  const { datasetRules } = useMaterializationDefaults();
  const refresh = useRefreshDatasets();
  const navigate = useNavigate();
  const toast = useToast();

  useEffect(() => {
    if (open) {
      setName('');
      setDescription('');
      setError(null);
    }
  }, [open]);

  const problem = datasetNameProblem(name, datasetRules);
  const create = useMutation({
    mutationFn: () => api.createDataset({ name: name.trim(), ...(description.trim() ? { description: description.trim() } : {}) }),
    onSuccess: async (result) => {
      await refresh();
      const created = result.name ?? name.trim();
      toast.success('Dataset created', result.status === 'applied' ? `${created} is ready for tables.` : `${created} is being created.`);
      onClose();
      navigate(domainPath(`/datasets/${encodeURIComponent(created)}`));
    },
    onError: (failure: unknown) => setError(failure instanceof ApiError ? failure.message : 'The dataset could not be created.'),
  });

  const onSubmit = (event: FormEvent): void => {
    event.preventDefault();
    if (!problem) create.mutate();
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="New dataset"
      description="A named group of tables: SQL reads each as <dataset>.<table>, in the console and through the Iceberg catalog."
      footer={
        <>
          <Button variant="ghost" type="button" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" type="submit" form="create-dataset" disabled={Boolean(problem)} loading={create.isPending}>
            Create
          </Button>
        </>
      }
    >
      <form id="create-dataset" onSubmit={onSubmit} className="space-y-4">
        <Field
          label="Name"
          required
          hint="Lowercase letters, digits and underscores, starting with a letter. It never changes."
          error={name.trim() ? problem : null}
        >
          <Input
            autoFocus
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="sales"
            spellCheck={false}
            autoComplete="off"
            className="numeric"
          />
        </Field>
        <Field label="Description" hint="What its tables hold; it helps people find it. Its owners can change it.">
          <Input value={description} onChange={(event) => setDescription(event.target.value)} maxLength={500} placeholder="Orders, refunds and the rest of sales" />
        </Field>
        <p className="text-[12px] leading-relaxed text-[var(--color-ink-muted)]">
          You become its owner, and everyone signed in can read it. Its owners decide who may read it, add tables to it and
          manage it.
        </p>
        {error ? <p className="text-[12px] text-[var(--color-phase-failed)]">{error}</p> : null}
      </form>
    </Dialog>
  );
}
