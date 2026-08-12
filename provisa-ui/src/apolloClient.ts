// Copyright (c) 2026 Kenneth Stott
// Canary: 299d13ab-c444-40e0-b9ba-dee6a9d4eb91
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { ApolloClient, InMemoryCache, HttpLink, ApolloLink } from "@apollo/client";
import { map } from "rxjs/operators";
import { currentBearer, ORG_HEADER } from "./lib/authFetch";

const API_BASE = import.meta.env.VITE_API_BASE || "";

const httpLink = new HttpLink({
  uri: `${API_BASE}/admin/graphql`,
  credentials: "include",
  // REQ-1434: the bearer is attached here rather than in a link because it has to be awaited —
  // a Firebase session re-mints an expired token on demand, and a link runs synchronously, so a
  // link can only read the mirrored copy, which is stale after a sleep or a throttled tab.
  fetch: async (input, init) => {
    const token = await currentBearer();
    const headers = new Headers(init?.headers);
    if (token) headers.set("Authorization", `Bearer ${token}`);
    return fetch(input, { ...init, headers });
  },
});

const authLink = new ApolloLink((operation, forward) => {
  const orgId = localStorage.getItem("provisa_org");
  // REQ-1317: the name the middleware reads.
  if (orgId) operation.setContext({ headers: { [ORG_HEADER]: orgId } });
  return forward(operation);
});

// Always-replace merge: incoming wholly supersedes the cached array.
const replace = { merge: (_: unknown, incoming: unknown) => incoming };

const cache = new InMemoryCache({
  typePolicies: {
    Query: {
      fields: {
        domains: replace,
        tables: replace,
        relationships: replace,
        roles: replace,
      },
    },
  },
});

// Bump when the GraphQL schema or any persisted entity shape changes. A
// mismatch discards the stored snapshot so stale/partial entities (dangling
// refs, dropped non-null fields) can never be replayed into a live read.
const CACHE_VERSION = "4";
const CACHE_KEY = "apollo-cache";
const CACHE_VERSION_KEY = "apollo-cache-version";
const SCHEMA_VERSION_KEY = "admin-schema-version";

/** REQ-1326: discard the persisted admin snapshot. The cache is written to localStorage every 5s
 * and restored at module load, so without this a new sign-in replays the PREVIOUS identity's roles,
 * domains and tables — org-scoped data belonging to an org the new user may not even be a member
 * of. Called when a session starts or ends, never mid-session. */
export function clearPersistedAdminCache(): void {
  localStorage.removeItem(CACHE_KEY);
  localStorage.removeItem(SCHEMA_VERSION_KEY);
  void client.clearStore();
}

if (typeof window !== "undefined") {
  const stored = localStorage.getItem(CACHE_KEY);
  if (stored && localStorage.getItem(CACHE_VERSION_KEY) === CACHE_VERSION) {
    try {
      cache.restore(JSON.parse(stored));
    } catch (e) {
      console.warn("Failed to restore Apollo cache:", e);
    }
  } else {
    localStorage.removeItem(CACHE_KEY);
    localStorage.setItem(CACHE_VERSION_KEY, CACHE_VERSION);
  }
}

// Afterware: read X-Schema-Version from every /admin/graphql response.
// When the server-side version advances (schema rebuilt after table mutations),
// re-fetch every active query so no view keeps rendering pre-rebuild data.
//
// The refetch is DEBOUNCED (trailing 300ms) and single-flight. A save flow like the
// tables page fires four sequential mutations, each of whose responses advances the
// version; refetching every active query per response quadruples the load for no
// fresher end state — TablesQuery alone is ~200 KB and takes seconds under a loaded
// backend, and the storm-per-mutation pattern is what pushed e2e save chains past
// their wait budgets on 2-core CI runners. Bumps that land while a storm is in
// flight schedule exactly one follow-up storm, so the final state is always fetched.
let _refetchTimer: ReturnType<typeof setTimeout> | null = null;
let _refetchInFlight = false;
function _scheduleActiveRefetch() {
  if (_refetchTimer !== null) return;
  _refetchTimer = setTimeout(() => {
    _refetchTimer = null;
    if (_refetchInFlight) {
      // A storm is running; run one more after it so the newest version's data lands.
      _scheduleActiveRefetch();
      return;
    }
    _refetchInFlight = true;
    // refetchQueries, NOT resetStore. This runs off a response interceptor, so a sibling
    // query issued by the same page mount may still be in flight; resetStore() clears the
    // store out from under it and terminates it with "Store reset while query was in
    // flight". Its useQuery then holds error set and data undefined permanently —
    // cache-and-network never retries — which is how MetricsPage ended up rendering a
    // forever-disabled fact picker (factTables derives from useTables(), and an errored
    // TablesQuery yields zero tables). refetchQueries re-runs the same active queries
    // without touching the store, so nothing in flight is disturbed; the Query.tables/
    // domains/relationships/roles merge policies above replace each list wholesale, so
    // the refetched result cannot merge with stale entries. A navigation away mid-refetch
    // aborts them and rejects this promise; nothing awaits it, so an unhandled rejection
    // would otherwise surface as an uncaught page error.
    client.refetchQueries({ include: "active" })
      .catch(() => {})
      .finally(() => { _refetchInFlight = false; });
  }, 300);
}
const schemaVersionLink = new ApolloLink((operation, forward) =>
  forward(operation).pipe(map((response) => {
    if (typeof window === "undefined") return response;
    const ctx = operation.getContext();
    const version = ctx.response?.headers?.get("x-schema-version");
    if (version === null || version === undefined) return response;
    const stored = localStorage.getItem(SCHEMA_VERSION_KEY);
    if (stored !== null && stored !== version) {
      localStorage.setItem(SCHEMA_VERSION_KEY, version);
      _scheduleActiveRefetch();
    } else if (stored === null) {
      localStorage.setItem(SCHEMA_VERSION_KEY, version);
    }
    return response;
  }))
);

export const client = new ApolloClient({
  ssrMode: typeof window === "undefined",
  link: ApolloLink.from([authLink, schemaVersionLink, httpLink]),
  cache,
  defaultOptions: {
    watchQuery: {
      fetchPolicy: "cache-and-network",
    },
    query: {
      fetchPolicy: "cache-first",
    },
  },
});

if (typeof window !== "undefined") {
  const persist = () => {
    const cacheData = cache.extract();
    localStorage.setItem(CACHE_KEY, JSON.stringify(cacheData));
    localStorage.setItem(CACHE_VERSION_KEY, CACHE_VERSION);
  };
  setInterval(persist, 5000);
  // The interval alone loses up to 5s of writes: a mutation's refetch lands, the user
  // navigates (full document load), and the restored snapshot resurrects PRE-save values
  // (#98 — the edit form then initializes from them). pagehide is the last synchronous
  // moment before the document is torn down, so the restored snapshot is always current.
  window.addEventListener("pagehide", persist);
}
