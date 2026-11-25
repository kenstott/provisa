// Copyright (c) 2026 Kenneth Stott
// Canary: f124caa7-3a08-4270-87e9-9f39c87a4afa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A Databricks source's SQL warehouse HTTP path and how it is read, beside its workspace URL
// (host) and catalog (database), saved as its federation hints
// (provisa/executor/drivers/databricks.py; `reach` read by provisa/federation/engine.py).

import { hintKey, hintKeys } from "./descriptor";
import { heldOf, type AuthSeed } from "./descriptorPick";

export const DATABRICKS = "databricks";
// REQ-2114: the hint keys are the descriptor's (provisa/api/admin/source_types.py).
const HTTP_PATH = hintKey(DATABRICKS, "http_path");
const REACH = hintKey(DATABRICKS, "reach");
/** REQ-1867: read in place. Left out, the source is read from a replica. */
export const REACH_SCAN = "scan";
/** The sign-in a saved Databricks source opens in: a personal access token. */
export const DATABRICKS_TOKEN = "token";

/** The hints a form saves; none until the HTTP path is given. */
export function databricksHints(fields: Record<string, string>): Record<string, string> {
  if (!fields[HTTP_PATH]) return {};
  return {
    [HTTP_PATH]: fields[HTTP_PATH],
    // REQ-1867: read in place only when the steward chose it; the default lands
    ...(fields[REACH] === REACH_SCAN ? { [REACH]: REACH_SCAN } : {}),
  };
}

/** A saved source's hints, and the token its password field holds, into the form. */
export function databricksSeed(
  hints: Record<string, unknown>,
  passwordRef: string | null | undefined,
): AuthSeed {
  return {
    authType: DATABRICKS_TOKEN,
    authFields: {
      ...heldOf(hintKeys(DATABRICKS), hints),
      ...(passwordRef ? { access_token: passwordRef } : {}),
    },
  };
}
