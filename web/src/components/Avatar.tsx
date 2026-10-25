import type { CSSProperties, ReactNode } from 'react';

import { cn } from '@/lib/cn';

/** A person's initials in a circle, toned by who they are so a list of people is easy to scan. */
export function Avatar({
  name,
  email,
  size = 'md',
  muted = false,
  className,
}: {
  name: string | null | undefined;
  email?: string | null;
  size?: 'sm' | 'md' | 'lg';
  /** For a deactivated person. */
  muted?: boolean;
  className?: string;
}): ReactNode {
  const label = initialsOf(name, email);
  const hue = hueOf(email ?? name ?? '');
  return (
    <span
      aria-hidden
      className={cn(
        'inline-grid shrink-0 place-items-center rounded-full font-semibold tracking-tight select-none',
        // A pale tint with dark initials, or a deep one with light initials in the dark theme.
        'bg-[hsl(var(--avatar-hue)_70%_92%)] text-[hsl(var(--avatar-hue)_55%_38%)]',
        'dark:bg-[hsl(var(--avatar-hue)_35%_24%)] dark:text-[hsl(var(--avatar-hue)_70%_82%)]',
        size === 'sm' && 'size-6 text-[10px]',
        size === 'md' && 'size-8 text-[11px]',
        size === 'lg' && 'size-12 text-[15px]',
        muted && 'opacity-50 grayscale',
        className,
      )}
      style={{ '--avatar-hue': String(hue) } as CSSProperties}
    >
      {label}
    </span>
  );
}

export function initialsOf(name: string | null | undefined, email?: string | null): string {
  const words = (name ?? '').trim().split(/\s+/).filter(Boolean);
  if (words.length >= 2) return `${words[0]![0]}${words.at(-1)![0]}`.toUpperCase();
  if (words.length === 1) return words[0]!.slice(0, 2).toUpperCase();
  return (email ?? '?').slice(0, 2).toUpperCase();
}

function hueOf(text: string): number {
  let hash = 0;
  for (const character of text) hash = (hash * 31 + character.charCodeAt(0)) % 360;
  return hash;
}
