import {
  forwardRef,
  type ButtonHTMLAttributes,
  type HTMLAttributes,
  type InputHTMLAttributes,
  type ReactNode,
  type SelectHTMLAttributes,
  type TextareaHTMLAttributes,
} from 'react';

import { cn } from '@/lib/cn';

/* ------------------------------------------------------------------ Button */

type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger';
type ButtonSize = 'sm' | 'md';

const BUTTON_VARIANTS: Record<ButtonVariant, string> = {
  primary:
    'bg-[var(--color-ink)] text-[var(--color-canvas)] hover:opacity-90 disabled:opacity-40 shadow-sm',
  secondary:
    'bg-[var(--color-surface)] text-[var(--color-ink)] border border-[var(--color-line-strong)] hover:bg-[var(--color-raised)] disabled:opacity-40',
  ghost:
    'text-[var(--color-ink-muted)] hover:text-[var(--color-ink)] hover:bg-[var(--color-raised)] disabled:opacity-40',
  danger:
    'bg-[var(--color-phase-failed)] text-white hover:opacity-90 disabled:opacity-40 shadow-sm',
};

const BUTTON_SIZES: Record<ButtonSize, string> = {
  sm: 'h-8 px-3 text-[13px] gap-1.5',
  md: 'h-9 px-4 text-sm gap-2',
};

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  loading?: boolean;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  { className, variant = 'secondary', size = 'md', loading = false, disabled, children, ...props },
  ref,
) {
  return (
    <button
      ref={ref}
      disabled={disabled || loading}
      className={cn(
        'inline-flex items-center justify-center rounded-[var(--radius-control)] font-medium',
        'transition-[opacity,background-color,color] duration-150 select-none whitespace-nowrap',
        'disabled:cursor-not-allowed',
        BUTTON_SIZES[size],
        BUTTON_VARIANTS[variant],
        className,
      )}
      {...props}
    >
      {loading ? <Spinner className="size-3.5" /> : null}
      {children}
    </button>
  );
});

/* ------------------------------------------------------------------ Spinner */

export function Spinner({ className }: { className?: string }): ReactNode {
  return (
    <svg className={cn('size-4 animate-spin', className)} viewBox="0 0 24 24" fill="none" aria-hidden>
      <circle cx="12" cy="12" r="9" stroke="currentColor" strokeOpacity="0.2" strokeWidth="3" />
      <path d="M21 12a9 9 0 0 0-9-9" stroke="currentColor" strokeWidth="3" strokeLinecap="round" />
    </svg>
  );
}

/* -------------------------------------------------------------------- Panel */

export function Panel({ className, ...props }: HTMLAttributes<HTMLDivElement>): ReactNode {
  return <section className={cn('panel', className)} {...props} />;
}

export function PanelHeader({
  title,
  description,
  actions,
  className,
}: {
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  className?: string;
}): ReactNode {
  return (
    <header
      className={cn(
        'flex items-start justify-between gap-4 border-b border-[var(--color-line)] px-5 py-3.5',
        className,
      )}
    >
      <div className="min-w-0">
        <h2 className="text-[13px] font-semibold tracking-tight">{title}</h2>
        {description ? (
          <p className="mt-0.5 text-[12px] text-[var(--color-ink-muted)]">{description}</p>
        ) : null}
      </div>
      {actions ? <div className="flex shrink-0 items-center gap-2">{actions}</div> : null}
    </header>
  );
}

/* -------------------------------------------------------------------- Badge */

type BadgeTone = 'neutral' | 'accent' | 'running' | 'pending' | 'paused' | 'failed';

const BADGE_TONES: Record<BadgeTone, string> = {
  neutral: 'text-[var(--color-ink-muted)] bg-[var(--color-raised)] border-[var(--color-line)]',
  accent: 'text-[var(--color-accent)] bg-[var(--color-accent-soft)] border-transparent',
  running: 'text-[var(--color-phase-running)] bg-[color-mix(in_srgb,var(--color-phase-running)_12%,transparent)] border-transparent',
  pending: 'text-[var(--color-phase-pending)] bg-[color-mix(in_srgb,var(--color-phase-pending)_12%,transparent)] border-transparent',
  paused: 'text-[var(--color-phase-paused)] bg-[color-mix(in_srgb,var(--color-phase-paused)_14%,transparent)] border-transparent',
  failed: 'text-[var(--color-phase-failed)] bg-[color-mix(in_srgb,var(--color-phase-failed)_12%,transparent)] border-transparent',
};

export function Badge({
  tone = 'neutral',
  className,
  title,
  children,
}: {
  tone?: BadgeTone;
  className?: string;
  title?: string;
  children: ReactNode;
}): ReactNode {
  return (
    <span
      title={title}
      className={cn(
        'inline-flex items-center gap-1.5 rounded-md border px-2 py-0.5 text-[11px] font-medium',
        BADGE_TONES[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}

/* ------------------------------------------------------------------- Inputs */

/** Shared by every text-like control, including custom triggers that must look like an input. */
export const FIELD_BASE =
  'w-full rounded-[var(--radius-control)] border border-[var(--color-line-strong)] bg-[var(--color-surface)] ' +
  'px-3 text-sm text-[var(--color-ink)] placeholder:text-[var(--color-ink-faint)] ' +
  'transition-colors focus:border-[var(--color-accent)] focus:outline-none disabled:opacity-50';

export const Input = forwardRef<HTMLInputElement, InputHTMLAttributes<HTMLInputElement>>(function Input(
  { className, ...props },
  ref,
) {
  return <input ref={ref} className={cn(FIELD_BASE, 'h-9', className)} {...props} />;
});

export const Textarea = forwardRef<HTMLTextAreaElement, TextareaHTMLAttributes<HTMLTextAreaElement>>(
  function Textarea({ className, ...props }, ref) {
    return <textarea ref={ref} className={cn(FIELD_BASE, 'py-2 font-mono text-[13px]', className)} {...props} />;
  },
);

export const Select = forwardRef<HTMLSelectElement, SelectHTMLAttributes<HTMLSelectElement>>(
  function Select({ className, ...props }, ref) {
    return (
      <select
        ref={ref}
        className={cn(FIELD_BASE, 'h-9 cursor-pointer appearance-none pr-8', className)}
        style={{
          backgroundImage:
            "url(\"data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='16' height='16' fill='none' stroke='%239ca3af' stroke-width='1.75'><path d='M4 6.5 8 10.5 12 6.5'/></svg>\")",
          backgroundRepeat: 'no-repeat',
          backgroundPosition: 'right 8px center',
        }}
        {...props}
      />
    );
  },
);

export function Field({
  label,
  hint,
  error,
  required,
  children,
}: {
  label: string;
  hint?: ReactNode;
  error?: string | null;
  required?: boolean;
  children: ReactNode;
}): ReactNode {
  return (
    <label className="block space-y-1.5">
      <span className="flex items-baseline gap-1.5">
        <span className="text-[13px] font-medium">{label}</span>
        {required ? <span className="text-[11px] text-[var(--color-ink-faint)]">required</span> : null}
      </span>
      {children}
      {error ? (
        <span className="block text-[12px] text-[var(--color-phase-failed)]">{error}</span>
      ) : hint ? (
        <span className="block text-[12px] text-[var(--color-ink-muted)]">{hint}</span>
      ) : null}
    </label>
  );
}

/* -------------------------------------------------------- Segmented control */

export function SegmentedControl<T extends string>({
  value,
  options,
  onChange,
  className,
}: {
  value: T;
  options: { value: T; label: ReactNode }[];
  onChange: (value: T) => void;
  className?: string;
}): ReactNode {
  return (
    <div
      role="tablist"
      className={cn(
        'inline-flex items-center gap-0.5 rounded-[var(--radius-control)] border border-[var(--color-line)] bg-[var(--color-raised)] p-0.5',
        className,
      )}
    >
      {options.map((option) => (
        <button
          key={option.value}
          role="tab"
          type="button"
          aria-selected={option.value === value}
          onClick={() => onChange(option.value)}
          className={cn(
            'rounded-[6px] px-2.5 py-1 text-[12px] font-medium transition-colors',
            option.value === value
              ? 'bg-[var(--color-surface)] text-[var(--color-ink)] shadow-sm'
              : 'text-[var(--color-ink-muted)] hover:text-[var(--color-ink)]',
          )}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}

/* -------------------------------------------------------------- Empty state */

export function EmptyState({
  icon,
  title,
  description,
  action,
  className,
}: {
  icon?: ReactNode;
  title: string;
  description?: ReactNode;
  action?: ReactNode;
  className?: string;
}): ReactNode {
  return (
    <div className={cn('flex flex-col items-center justify-center px-6 py-16 text-center', className)}>
      {icon ? <div className="mb-3 text-[var(--color-ink-faint)]">{icon}</div> : null}
      <p className="text-sm font-medium">{title}</p>
      {description ? (
        <p className="mt-1 max-w-sm text-[13px] leading-relaxed text-[var(--color-ink-muted)]">
          {description}
        </p>
      ) : null}
      {action ? <div className="mt-4">{action}</div> : null}
    </div>
  );
}

/* ----------------------------------------------------------------- Skeleton */

export function Skeleton({ className }: { className?: string }): ReactNode {
  return <div className={cn('animate-pulse rounded-md bg-[var(--color-raised)]', className)} />;
}

/* ---------------------------------------------------------------- Key/value */

export function KeyValue({
  label,
  children,
  mono = false,
  className,
}: {
  label: string;
  children: ReactNode;
  mono?: boolean;
  className?: string;
}): ReactNode {
  return (
    <div className={cn('min-w-0 space-y-1', className)}>
      <dt className="text-[11px] font-medium tracking-wide text-[var(--color-ink-faint)] uppercase">
        {label}
      </dt>
      <dd className={cn('truncate text-[13px] text-[var(--color-ink)]', mono && 'numeric')}>{children}</dd>
    </div>
  );
}
