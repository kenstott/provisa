// Copyright (c) 2026 Kenneth Stott
// Canary: 9e3a51c7-2b84-4d06-8f1e-5c7d2a9b4e63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923 (amended 2026-11-20): a SPARQL table's columns are read from its query's SELECT
// variables, every binding text, so neither registering nor editing it waits on a live query.

import { useEffect, useState } from "react";
import { fetchSparqlColumns, type WikidataColumn } from "../../api/wikidata";

const READ_DELAY_MS = 400;

/** What the query's SELECT says its columns are. */
export type SparqlColumns =
  | { state: "empty" }
  | { state: "reading"; query: string }
  | { state: "read"; query: string; columns: WikidataColumn[] }
  | { state: "refused"; query: string; problem: string };

/** The columns of `query` as the query's SELECT names them, read now: what was read already for
 * this very text, else asked at once (a refusal is thrown, naming why). */
export async function sparqlColumnsNow(
  query: string,
  read: SparqlColumns,
): Promise<WikidataColumn[]> {
  if (read.state === "read" && read.query === query.trim()) return read.columns;
  return fetchSparqlColumns(query.trim());
}

/** The columns of `query`, read again a moment after it last changed; `empty` with no query. */
export function useSparqlColumns(query: string): SparqlColumns {
  const text = query.trim();
  const [read, setRead] = useState<SparqlColumns>({ state: "empty" });
  useEffect(() => {
    if (!text) return;
    let live = true;
    const timer = setTimeout(() => {
      fetchSparqlColumns(text)
        .then((columns) => live && setRead({ state: "read", query: text, columns }))
        .catch(
          (e: Error) => live && setRead({ state: "refused", query: text, problem: e.message }),
        );
    }, READ_DELAY_MS);
    return () => {
      live = false;
      clearTimeout(timer);
    };
  }, [text]);
  if (!text) return { state: "empty" };
  // An answer for an earlier text is not this query's.
  if (read.state === "empty" || read.query !== text) return { state: "reading", query: text };
  return read;
}
