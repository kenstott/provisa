// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { get as idbGet, set as idbSet } from "idb-keyval";
import { kept, restored, type KeptResult } from "../lib/resultWithQuery";

/**
 * The GraphQL explorer's last response, kept with the query that produced it. GraphiQL saves the
 * query on every edit and never the response; the response is the explorer's own, so it is kept
 * here with its query and restored only beside it.
 */
const KEY = "provisa.graphql.response";

/** The query of the last run (or restore): the one the response shown answers. */
let runQuery: string | null = null;

/** A run is starting with this query. */
export function noteRun(query: string): void {
  runQuery = query;
}

/** Keep the response shown, when the editor still holds the query of the run that produced it. */
export async function keepResponse(editorQuery: string, response: string): Promise<void> {
  if (runQuery === null || runQuery.trim() !== editorQuery.trim()) return;
  await idbSet(KEY, kept(runQuery, response));
}

/** The kept response, when the editor holds the query that produced it; otherwise none. */
export async function restoreResponse(editorQuery: string): Promise<string | null> {
  const k = await idbGet<KeptResult<string>>(KEY);
  const response = restored(k, editorQuery);
  if (response !== null && k) runQuery = k.query;
  return response;
}
