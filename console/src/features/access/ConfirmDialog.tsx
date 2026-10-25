import type { ReactNode } from 'react';

import { Dialog } from '@/components/ui/Dialog';
import { Button } from '@/components/ui/primitives';

/** Ask before a change that is hard to take back. */
export function ConfirmDialog({
  open,
  title,
  description,
  confirmLabel,
  danger = false,
  pending = false,
  onConfirm,
  onClose,
  children,
}: {
  open: boolean;
  title: string;
  description?: ReactNode;
  confirmLabel: string;
  danger?: boolean;
  pending?: boolean;
  onConfirm: () => void;
  onClose: () => void;
  children?: ReactNode;
}): ReactNode {
  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={title}
      description={description}
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button variant={danger ? 'danger' : 'primary'} loading={pending} onClick={onConfirm}>
            {confirmLabel}
          </Button>
        </>
      }
    >
      {children ?? null}
    </Dialog>
  );
}
