/** What a sign-in through a provider did, in words (handbook 20). No React: for tests. */

import type { SsoDomainOutcome, SsoOutcome } from '@shared/types';

/** Why a domain turned a provider sign-in away, briefly, when it said nothing better. */
const REASONS: Record<string, string> = {
  no_account: 'no account there',
  provider_disabled: 'it does not take this sign-in',
  email_not_allowed: 'your email domain is not allowed there',
  email_unverified: 'your email is not verified',
  account_deactivated: 'your account there is deactivated',
  invalid_credential: 'the sign-in did not check out',
  provider_unreachable: 'it could not reach the provider',
  provisioning: 'your account there is still being made',
  unreachable: 'it could not be reached',
};

export interface OutcomeSummary {
  title: string;
  detail: string | null;
  signedIn: SsoDomainOutcome[];
  refused: SsoDomainOutcome[];
}

/** A sign-in's outcome as one notice: where it signed in, and where not and why. */
export function describeOutcome(outcome: SsoOutcome): OutcomeSummary {
  const signedIn = outcome.domains.filter((entry) => entry.status === 'signed_in');
  const refused = outcome.domains.filter((entry) => entry.status !== 'signed_in');
  if (outcome.error) return { title: `Signing in with ${outcome.label} failed`, detail: outcome.error, signedIn, refused };
  const names = (entries: SsoDomainOutcome[]): string => entries.map((entry) => entry.name).join(', ');
  const title =
    signedIn.length === 0
      ? `Signing in with ${outcome.label} gave you no session`
      : outcome.domains.length === 1
        ? `Signed in with ${outcome.label}`
        : `Signed in to ${names(signedIn)}`;
  const detail = refused.length
    ? refused.map((entry) => `${entry.name}: ${REASONS[entry.reason ?? ''] ?? entry.message ?? 'refused'}`).join(' · ')
    : null;
  return { title, detail, signedIn, refused };
}
