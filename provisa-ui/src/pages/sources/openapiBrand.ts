// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923, REQ-2265: what an OpenAPI brand's registration sends. The spec and the address are
// the brand's; the steward gives its setup values (a base URL, for a brand each customer runs
// at its own host) and the credential its auth type states (BRAND_AUTH).

import { BRAND_AUTH } from "./constants";

export interface OpenapiBrandRegistration {
  source_id: string;
  brand: string;
  setup: Record<string, string>;
  token?: string;
  client_id?: string;
  client_secret?: string;
}

/** The body of POST /admin/openapi/register for a branded source. A credential is sent as
 * typed; the server refuses a missing one by name. */
export function openapiBrandRegistration(
  sourceId: string,
  brand: string,
  setup: Record<string, string>,
  authFields: Record<string, string>,
): OpenapiBrandRegistration {
  const base = { source_id: sourceId, brand, setup };
  if (BRAND_AUTH[brand] === "client_credentials") {
    return {
      ...base,
      client_id: authFields.client_id ?? "",
      client_secret: authFields.client_secret ?? "",
    };
  }
  return { ...base, token: authFields.token ?? "" };
}

/** Whether a branded source's form was given a new credential. A stored one is never sent
 * back (REQ-320), so an edit without one keeps it. */
export function brandCredentialTyped(brand: string, authFields: Record<string, string>): boolean {
  return BRAND_AUTH[brand] === "client_credentials"
    ? Boolean(authFields.client_secret)
    : Boolean(authFields.token);
}
