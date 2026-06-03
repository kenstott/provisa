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

export const client = new ApolloClient({
  ssrMode: typeof window === "undefined",
  link: authLink.concat(httpLink),
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
