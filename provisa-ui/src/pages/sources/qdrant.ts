// Copyright (c) 2026 Kenneth Stott
// Canary: c6c7b209-17a6-4a06-8290-1d1f34f77636
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1983: a Qdrant source is reached at its URL (the source's host) with its API key (its
// password). Whether it reads over gRPC rides in its federation hints (provisa/qdrant/fetch.py).
// Its one schema is "default"; a collection is a table, and a search is a table whose
// arguments are the search's (provisa/qdrant/table.py).

import { hintKey } from "./descriptor";

export const QDRANT = "qdrant";
// REQ-2114: the hint key is the descriptor's (provisa/api/admin/source_types.py).
const PREFER_GRPC = hintKey(QDRANT, "prefer_grpc");
export const QDRANT_SCHEMA = "default";

/** The source's federation hints for what the fields say. */
export function qdrantHints(fields: Record<string, string>): Record<string, string> {
  // Text, as every hint is: the server resolves each one as a secret reference.
  return { [PREFER_GRPC]: fields.qdrant_grpc === "true" ? "true" : "false" };
}

/** The fields an existing source's hints fill in. */
export function qdrantFieldsFromHints(
  hintsJson: string | null | undefined,
): Record<string, string> {
  const hints = hintsJson ? (JSON.parse(hintsJson) as Record<string, unknown>) : {};
  return { qdrant_grpc: String(hints[PREFER_GRPC]) === "true" ? "true" : "false" };
}

/** The argument a search's query names ("@q" names "q"), or null where the text names none. */
export function searchQueryArgument(template: string): string | null {
  try {
    const query = (JSON.parse(template) as { query?: unknown }).query;
    const found = typeof query === "string" ? /^@([A-Za-z_][A-Za-z0-9_]*)$/.exec(query) : null;
    return found ? found[1] : null;
  } catch {
    // Not JSON yet: the steward is still writing it; the server refuses it by name on save.
    return null;
  }
}

/** A collection's vector columns are offered but left unselected: each holds a whole vector. */
export function offeredUnselected(sourceType: string, path: string | null | undefined): boolean {
  return sourceType === QDRANT && (path === "@vector" || (path ?? "").startsWith("@vector."));
}
