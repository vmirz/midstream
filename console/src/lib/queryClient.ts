import { QueryClient } from '@tanstack/react-query';

/**
 * The page's query caches: one for what is the console's own - its config, who the browser is in
 * every domain, the all-domains lists - and one per domain for everything under `/d/<domain>/`
 * (handbook 20). Two domains can hold a dataset of the same name, or a user of the same id; with a
 * cache each, nothing one domain answered is ever shown as another's.
 */
function makeClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        // Live data arrives over SSE; queries here are for on-demand resources only.
        refetchOnWindowFocus: false,
        retry: 1,
        staleTime: 10_000,
      },
    },
  });
}

export const rootQueryClient = makeClient();

const domainClients = new Map<string, QueryClient>();

/** A domain's own cache, made the first time a page of the domain is opened. */
export function queryClientFor(domain: string): QueryClient {
  let client = domainClients.get(domain);
  if (!client) {
    client = makeClient();
    domainClients.set(domain, client);
  }
  return client;
}

/** Every cache the page holds: the console's, then each domain's opened so far. */
export function allQueryClients(): QueryClient[] {
  return [rootQueryClient, ...domainClients.values()];
}
