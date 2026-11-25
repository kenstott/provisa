// Copyright (c) 2026 Kenneth Stott
// Canary: 7705a1ac-eb6b-4d7a-bb7f-6b211a4fc5eb
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// An Exasol source may pin a self-signed server certificate by its fingerprint, which the
// truststore cannot chain (provisa/core/models.py's Source.jdbc_url reads it from the hints).

import { hintKey, hintKeys } from "./descriptor";
import { heldOf, type AuthSeed } from "./descriptorPick";

export const EXASOL = "exasol";
// REQ-2114: the hint key is the descriptor's (provisa/api/admin/source_types.py).
const TLS_FINGERPRINT = hintKey(EXASOL, "tls_fingerprint");
/** The form's choice that pins the certificate. */
export const EXASOL_TLS_FINGERPRINT = "tls_fingerprint";

/** The hints a form saves: the fingerprint, when pinning is chosen and one is given. */
export function exasolHints(
  authType: string,
  fields: Record<string, string>,
): Record<string, string> {
  return authType === EXASOL_TLS_FINGERPRINT && fields[TLS_FINGERPRINT]
    ? { [TLS_FINGERPRINT]: fields[TLS_FINGERPRINT] }
    : {};
}

/** A saved source's hints into the form. */
export function exasolSeed(hints: Record<string, unknown>): AuthSeed {
  return {
    authType: hints[TLS_FINGERPRINT] ? EXASOL_TLS_FINGERPRINT : "none",
    authFields: heldOf(hintKeys(EXASOL), hints),
  };
}
