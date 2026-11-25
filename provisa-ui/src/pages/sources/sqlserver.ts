// Copyright (c) 2026 Kenneth Stott
// Canary: 10f3ba78-9ec7-4f03-8484-c522b29bdea3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A SQL Server source may trust a self-signed or internal-CA certificate the truststore cannot
// chain, common on internal deployments (provisa/core/models.py's Source.jdbc_url reads it from
// the hints).

import { hintKey, hintKeys } from "./descriptor";
import { heldOf, type AuthSeed } from "./descriptorPick";

export const SQLSERVER = "sqlserver";
// REQ-2114: the hint key is the descriptor's (provisa/api/admin/source_types.py).
const TRUST_SERVER_CERTIFICATE = hintKey(SQLSERVER, "trust_server_certificate");
/** The form's choice that trusts the server's certificate. */
export const SQLSERVER_TRUST_CERTIFICATE = "trust_server_certificate";

/** The hints a form saves: the trust flag, when it is chosen. */
export function sqlserverHints(authType: string): Record<string, string> {
  return authType === SQLSERVER_TRUST_CERTIFICATE ? { [TRUST_SERVER_CERTIFICATE]: "true" } : {};
}

/** A saved source's hints into the form. */
export function sqlserverSeed(hints: Record<string, unknown>): AuthSeed {
  return {
    authType: hints[TRUST_SERVER_CERTIFICATE] ? SQLSERVER_TRUST_CERTIFICATE : "none",
    authFields: heldOf(hintKeys(SQLSERVER), hints),
  };
}
