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

const API_BASE = import.meta.env.VITE_API_BASE || "";

const httpLink = new HttpLink({
  uri: `${API_BASE}/admin/graphql`,
  credentials: "include",
});

const authLink = new ApolloLink((operation, forward) => {
  const token = localStorage.getItem("provisa_token");
  if (token) {
    operation.setContext({
      headers: {
        authorization: `Bearer ${token}`,
      },
    });
  }
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
const CACHE_VERSION = "3";
const CACHE_KEY = "apollo-cache";
const CACHE_VERSION_KEY = "apollo-cache-version";
const SCHEMA_VERSION_KEY = "admin-schema-version";

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
// reset the store so all active queries re-fetch and stale cached data is evicted.
let _resetting = false;
const schemaVersionLink = new ApolloLink((operation, forward) =>
  forward(operation).pipe(map((response) => {
    if (typeof window === "undefined" || _resetting) return response;
    const ctx = operation.getContext();
    const version = ctx.response?.headers?.get("x-schema-version");
    if (version === null || version === undefined) return response;
    const stored = localStorage.getItem(SCHEMA_VERSION_KEY);
    if (stored !== null && stored !== version) {
      localStorage.setItem(SCHEMA_VERSION_KEY, version);
      _resetting = true;
      client.resetStore().finally(() => { _resetting = false; });
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
  setInterval(() => {
    const cacheData = cache.extract();
    localStorage.setItem(CACHE_KEY, JSON.stringify(cacheData));
    localStorage.setItem(CACHE_VERSION_KEY, CACHE_VERSION);
  }, 5000);
}
