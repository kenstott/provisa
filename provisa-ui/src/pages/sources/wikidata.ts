// Copyright (c) 2026 Kenneth Stott
// Canary: 9a6c2e41-3b7d-4f58-8e19-d2c5b7a0f364
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: Wikidata, a branded source carried by the SPARQL source. Picking it fills the
// query service endpoint and asks for no credential; its tables are replicated on a schedule by
// default (provisa/sparql/wikidata.py, which applies the same preset to any caller).

import type { SourceFormState } from "./SourceFormFields";
import { fieldValue, hintKey } from "./descriptor";

export const WIKIDATA = "wikidata";

// REQ-2114: the endpoint and the brand hint are the descriptor's (provisa/api/admin/
// source_types.py, from provisa/sparql/wikidata.py).
const BRAND = hintKey(WIKIDATA, "brand");

/** The Wikidata query service. */
export const WIKIDATA_ENDPOINT = fieldValue(WIKIDATA, "input", "host") as string;

/** Reads come from the replica (replicate 0: Always), refreshed once a day (the Cache TTL). */
export const WIKIDATA_REPLICATE = 0;
export const WIKIDATA_REFRESH_SECONDS = 86400;

/** The form as picking Wikidata leaves it: the endpoint and scheduled replication. */
export function wikidataPreset(form: SourceFormState): SourceFormState {
  return {
    ...form,
    host: WIKIDATA_ENDPOINT,
    replicate: WIKIDATA_REPLICATE,
    cacheTtl: String(WIKIDATA_REFRESH_SECONDS),
  };
}

/** What a Wikidata source saves in its federation hints: its brand. */
export function wikidataHints(): Record<string, string> {
  return { [BRAND]: fieldValue(WIKIDATA, "federationHintsJson", BRAND) as string };
}
