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
import { get as idbGet, set as idbSet, del as idbDel } from "idb-keyval";
import { map } from "rxjs/operators";
import { currentBearer, ENV_HEADER, ORG_HEADER, selectedEnv } from "./lib/authFetch";

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
  const env = selectedEnv();
  // REQ-1317: the name the middleware reads. REQ-1487: and the environment within it — GraphQL
  // does not pass through the fetch interceptor's header path, so a query left without this would
  // read prod's model while every REST surface beside it read the branch.
  const headers: Record<string, string> = {};
  if (orgId) headers[ORG_HEADER] = orgId;
  if (env !== null) headers[ENV_HEADER] = env;
  if (Object.keys(headers).length > 0) operation.setContext({ headers });
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
export const CACHE_VERSION = "4";
// IndexedDB, not localStorage. The snapshot grows with the catalog (TablesQuery alone is ~200 KB
// for a 54-table model) and localStorage is a ~5 MB budget shared by the whole origin — every
// other feature that stores anything, the guided tour's saved state among them. On a large model
// the write exceeded it and threw QuotaExceededError out of a setInterval, where nothing could
// catch it, and the snapshot already on disk went on occupying the budget: the guided tour then
// hung because its OWN localStorage write was the one that threw. IndexedDB's quota is derived
// from free disk rather than a fixed 5 MB, and it is a separate store, so a large catalog can no
// longer starve anything else. It also stores the extract structurally, so nothing is stringified.
const CACHE_KEY = "apollo-cache";
// Read synchronously by the response link below, and a few bytes — it stays in localStorage.
const SCHEMA_VERSION_KEY = "admin-schema-version";

/** REQ-1326: discard the persisted admin snapshot. The cache is written to IndexedDB every 5s and
 * restored during boot, so without this a new sign-in replays the PREVIOUS identity's roles,
 * domains and tables — org-scoped data belonging to an org the new user may not even be a member
 * of. Called when a session starts or ends, never mid-session. */
export function clearPersistedAdminCache(): void {
  void dropPersistedCache();
  localStorage.removeItem(SCHEMA_VERSION_KEY);
  void client.clearStore();
}

/** REQ-1487/REQ-1543: the reader is now looking at a DIFFERENT model — another environment, or the
 * same one stepped back along its history — and every entity in the cache belongs to the one it was
 * just looking at. Both stores go: the in-memory one, and the persisted snapshot, which would
 * otherwise be restored on the next load and replay the model that was left.
 *
 * This is what a full page reload used to do, and the reload is why choosing an environment threw
 * the whole application away and rebuilt it — a splash screen in the middle of a two-click gesture.
 * Resetting the store is the part of that which was actually needed: active queries re-run against
 * the environment now selected, and the views holding them re-render with the answer.
 */
export function modelReplaced(): Promise<unknown> {
  localStorage.removeItem(SCHEMA_VERSION_KEY);
  return Promise.all([dropPersistedCache(), client.resetStore().catch(() => {})]);
}

/** Discard the persisted snapshot in both stores. The localStorage removal is not dead code: the
 * snapshot lived there until the move to IndexedDB, and a browser that ran the previous build
 * still holds that entry — several megabytes of the origin's 5 MB budget, charged to every other
 * feature that stores anything, until something deletes it. */
function dropPersistedCache(): Promise<void> {
  localStorage.removeItem(CACHE_KEY);
  return idbDel(CACHE_KEY).catch((e: unknown) => {
    console.warn("Failed to discard the persisted Apollo cache:", e);
  });
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
    client
      .refetchQueries({ include: "active" })
      .catch(() => {})
      .finally(() => {
        _refetchInFlight = false;
      });
  }, 300);
}
const schemaVersionLink = new ApolloLink((operation, forward) =>
  forward(operation).pipe(
    map((response) => {
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
    }),
  ),
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

/** Resolves when the persisted snapshot has been restored into the cache, or when there is nothing
 * to restore. Awaited by the bootstrap before the first render (main.tsx): an IndexedDB read is
 * asynchronous, where the localStorage read it replaces happened at module load, so without the
 * gate the first queries would run against an empty cache and the warm start would be lost.
 *
 * A snapshot written by an older schema is refused, not migrated — that is what CACHE_VERSION is
 * for: stale or partial entities (dangling refs, dropped non-null fields) must never be replayed
 * into a live read. */
export const cacheRestored: Promise<void> =
  typeof window === "undefined"
    ? Promise.resolve()
    : idbGet<{ version: string; data: unknown }>(CACHE_KEY)
        .then((stored) => {
          if (stored === undefined) return;
          if (stored.version !== CACHE_VERSION) return dropPersistedCache();
          cache.restore(stored.data as Parameters<typeof cache.restore>[0]);
        })
        .catch((e: unknown) => {
          // A snapshot that cannot be read is a warm start that does not happen; the session runs
          // from an empty cache exactly as a first visit does.
          console.warn("Failed to restore Apollo cache:", e);
        });

if (typeof window !== "undefined") {
  // A snapshot that does not fit is not persisted at all: the copy on disk is dropped (it is a
  // read-through convenience, and a stale one is what version-gating already exists to refuse), and
  // persistence stops for the session rather than failing again every 5 s. The cache itself is
  // untouched — the session keeps running from memory and only loses the warm start on reload.
  // IndexedDB's quota is far larger than the localStorage budget this used to exhaust, but it is
  // still a quota: a full disk raises QuotaExceededError here too.
  let persistIntervalId: ReturnType<typeof setInterval> | null = null;
  let persisting: Promise<void> = Promise.resolve();
  const persist = () => {
    // Serialized: idbSet is asynchronous and the interval does not wait for it, so two writes of a
    // multi-megabyte extract could otherwise overlap on every tick.
    persisting = persisting.then(() =>
      idbSet(CACHE_KEY, { version: CACHE_VERSION, data: cache.extract() }).catch((e: unknown) => {
        if (persistIntervalId !== null) clearInterval(persistIntervalId);
        document.removeEventListener("visibilitychange", flushIfHidden);
        void dropPersistedCache();
        console.warn("Apollo cache snapshot could not be persisted; disabled for this session:", e);
      }),
    );
    return persisting;
  };
  persistIntervalId = setInterval(persist, 5000);
  // The interval alone loses up to 5s of writes: a mutation's refetch lands, the user navigates
  // (full document load), and the restored snapshot resurrects PRE-save values (#98 — the edit form
  // then initializes from them). The localStorage version flushed on `pagehide`, the last
  // synchronous moment before teardown; an IndexedDB write started there is not guaranteed to
  // commit, so the flush moves one event earlier to `visibilitychange`, which fires when the tab is
  // hidden — including on the way to being unloaded — while the document is still live.
  const flushIfHidden = () => {
    if (document.visibilityState === "hidden") void persist();
  };
  document.addEventListener("visibilitychange", flushIfHidden);
}
