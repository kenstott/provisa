// Copyright (c) 2026 Kenneth Stott
// Canary: a64e6e31-fd05-4cdf-8395-4b2da9ae8ad9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1982: an Azure Cosmos DB for NoSQL source signs in with its account key, or as a Microsoft
// Entra service principal. The endpoint is the source's host and the database its database; the
// key, or the principal's client secret, is its password. How it signs in, and the principal's
// tenant and client, ride in its federation hints (provisa/cosmosdb/fetch.py).

import { fieldChoices, fieldDefault, hintKey } from "./descriptor";

export const COSMOSDB = "cosmosdb";

// REQ-2114: the hint keys, the two sign-ins and the default one are the descriptor's
// (provisa/api/admin/source_types.py, from provisa/cosmosdb/fetch.py).
const HINTS = "federationHintsJson";
const AUTH = hintKey(COSMOSDB, "auth");
const TENANT_ID = hintKey(COSMOSDB, "tenant_id");
const CLIENT_ID = hintKey(COSMOSDB, "client_id");
export const COSMOS_KEY = fieldDefault(COSMOSDB, HINTS, AUTH) as string;
export const COSMOS_ENTRA = ((): string => {
  const others = fieldChoices(COSMOSDB, HINTS, AUTH).filter((c) => c !== COSMOS_KEY);
  if (others.length !== 1) throw new Error("the cosmosdb descriptor states no one other sign-in");
  return others[0];
})();

/** The source's federation hints for what the fields say. */
export function cosmosHints(fields: Record<string, string>): Record<string, string> {
  if ((fields.cosmos_auth ?? COSMOS_KEY) !== COSMOS_ENTRA) return { [AUTH]: COSMOS_KEY };
  return {
    [AUTH]: COSMOS_ENTRA,
    [TENANT_ID]: (fields.cosmos_tenant_id ?? "").trim(),
    [CLIENT_ID]: (fields.cosmos_client_id ?? "").trim(),
  };
}

/** The fields an existing source's hints fill in. */
export function cosmosFieldsFromHints(
  hintsJson: string | null | undefined,
): Record<string, string> {
  const hints = hintsJson ? (JSON.parse(hintsJson) as Record<string, string>) : {};
  return {
    cosmos_auth: hints[AUTH] ?? COSMOS_KEY,
    cosmos_tenant_id: hints[TENANT_ID] ?? "",
    cosmos_client_id: hints[CLIENT_ID] ?? "",
  };
}
