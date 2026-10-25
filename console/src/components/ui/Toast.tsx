import { CheckCircle2, AlertTriangle, X } from 'lucide-react';
import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from 'react';

import { cn } from '@/lib/cn';

interface Toast {
  id: number;
  tone: 'success' | 'error';
  title: string;
  description?: string;
}

interface ToastApi {
  success: (title: string, description?: string) => void;
  error: (title: string, description?: string) => void;
}

const ToastContext = createContext<ToastApi | null>(null);

export function ToastProvider({ children }: { children: ReactNode }): ReactNode {
  const [toasts, setToasts] = useState<Toast[]>([]);

  const dismiss = useCallback((id: number) => {
    setToasts((current) => current.filter((toast) => toast.id !== id));
  }, []);

  const push = useCallback(
    (tone: Toast['tone'], title: string, description?: string) => {
      const id = Date.now() + Math.random();
      setToasts((current) => [...current.slice(-3), { id, tone, title, description }]);
      setTimeout(() => dismiss(id), tone === 'error' ? 8000 : 4500);
    },
    [dismiss],
  );

  const api = useMemo<ToastApi>(
    () => ({
      success: (title, description) => push('success', title, description),
      error: (title, description) => push('error', title, description),
    }),
    [push],
  );

  return (
    <ToastContext.Provider value={api}>
      {children}
      {/* A page with a sticky bottom bar sets --toast-offset so toasts stack above it. */}
      <div
        className="pointer-events-none fixed right-4 z-[60] flex w-[min(24rem,calc(100vw-2rem))] flex-col gap-2"
        style={{ bottom: 'calc(1rem + var(--toast-offset, 0px))' }}
      >
        {toasts.map((toast) => (
          <div
            key={toast.id}
            role="status"
            className={cn(
              'fade-in pointer-events-auto flex items-start gap-3 rounded-[var(--radius-panel)] border p-3.5',
              'border-[var(--color-line)] bg-[var(--color-surface)] shadow-lg shadow-black/10 dark:shadow-black/40',
            )}
          >
            {toast.tone === 'success' ? (
              <CheckCircle2 className="mt-0.5 size-4 shrink-0 text-[var(--color-phase-running)]" />
            ) : (
              <AlertTriangle className="mt-0.5 size-4 shrink-0 text-[var(--color-phase-failed)]" />
            )}
            <div className="min-w-0 flex-1">
              <p className="text-[13px] font-medium">{toast.title}</p>
              {toast.description ? (
                <p className="mt-0.5 text-[12px] break-words text-[var(--color-ink-muted)]">
                  {toast.description}
                </p>
              ) : null}
            </div>
            <button
              type="button"
              onClick={() => dismiss(toast.id)}
              aria-label="Dismiss"
              className="text-[var(--color-ink-faint)] transition-colors hover:text-[var(--color-ink)]"
            >
              <X className="size-3.5" />
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

export function useToast(): ToastApi {
  const context = useContext(ToastContext);
  if (!context) throw new Error('useToast must be used inside ToastProvider');
  return context;
}
