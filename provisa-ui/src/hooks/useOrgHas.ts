// Copyright (c) 2026 Kenneth Stott
// Canary: 1e1692c9-fcee-4515-b4cb-b863c3bf6b7a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useRef } from "react";
import { useLazyQuery } from "@apollo/client/react";
import type { DataProduct, RegisteredTable, Source } from "../types/admin";
import type { OrgHas } from "../tour/tourPrerequisites";
import { fetchMcpChatStatus } from "../api/mcpChat";
import {
  DataProductsQuery as DATA_PRODUCTS_QUERY,
  SourcesQuery as SOURCES_QUERY,
  TablesQuery as TABLES_QUERY,
} from "./admin.graphql";

/**
 * REQ-2013: a function reading what the current org has that a tour's steps may need -- its
 * sources, its registered tables (`<source>.<table>`) and how many data products -- from the
 * cached catalog, else the server, and whether Polly has a model configured. A read that fails rejects; it is never taken for "none".
 *
 * A call made while a read is in flight shares it: a lazy query's execute aborts the call still in
 * flight when it runs again, and two quick starts of the tour (`?tour=1`) left the first rejected
 * with "The operation was aborted" and the tour not started.
 *
 * A read the store cut short is read again: a reset or clear of the store (a session starting,
 * the cache scope adopted, another environment chosen) stops every query in flight, and Apollo
 * rejects a lazy query's promise with the same AbortError then. It is retried up to
 * {@link READ_ATTEMPTS} times; a read that fails any other way rejects at once.
 */
export const READ_ATTEMPTS = 3;

/** Apollo's AbortError: a DOMException where there is one, which is not an Error everywhere. */
function isAbort(e: unknown): boolean {
  return typeof e === "object" && e !== null && "name" in e && e.name === "AbortError";
}

async function unlessCutShort<T>(load: () => Promise<T>): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await load();
    } catch (e) {
      if (!isAbort(e) || attempt === READ_ATTEMPTS) throw e;
    }
  }
}

export function useOrgHas(): () => Promise<OrgHas> {
  const [loadSources] = useLazyQuery<{ sources: Source[] }>(SOURCES_QUERY);
  const [loadTables] = useLazyQuery<{ tables: RegisteredTable[] }>(TABLES_QUERY);
  const [loadProducts] = useLazyQuery<{ dataProducts: DataProduct[] }>(DATA_PRODUCTS_QUERY);
  const inFlight = useRef<Promise<OrgHas> | null>(null);
  const read = useCallback(async (): Promise<OrgHas> => {
    const [sources, tables, products, polly] = await Promise.all([
      unlessCutShort(() => loadSources()),
      unlessCutShort(() => loadTables()),
      unlessCutShort(() => loadProducts()),
      fetchMcpChatStatus(),
    ]);
    if (!sources.data || !tables.data || !products.data) {
      throw new Error("Tour: the org's sources, tables or data products could not be read");
    }
    return {
      sources: new Set(sources.data.sources.map((x) => x.id)),
      tables: new Set(tables.data.tables.map((x) => `${x.sourceId}.${x.tableName}`)),
      dataProducts: products.data.dataProducts.length,
      pollyConfigured: polly.configured,
    };
  }, [loadSources, loadTables, loadProducts]);
  return useCallback(() => {
    if (inFlight.current === null) {
      inFlight.current = read().finally(() => {
        inFlight.current = null;
      });
    }
    return inFlight.current;
  }, [read]);
}
