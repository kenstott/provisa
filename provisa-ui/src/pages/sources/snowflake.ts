// Copyright (c) 2026 Kenneth Stott
// Canary: 632f2b8f-d792-4a37-9cc9-d6d4e04b5035
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A Snowflake source's warehouse, schema and role, beside its account URL (host) and database,
// saved as its federation hints (provisa/executor/drivers/snowflake.py reads them).

import { hintKeys } from "./descriptor";
import { givenOf, heldOf, type AuthSeed } from "./descriptorPick";

export const SNOWFLAKE = "snowflake";
// REQ-2114: the hints are the descriptor's (provisa/api/admin/source_types.py).
const HINTS = hintKeys(SNOWFLAKE);
/** The sign-in a saved Snowflake source opens in: its username and password. */
export const SNOWFLAKE_PASSWORD = "password";

/** The hints a form saves: each one given. */
export function snowflakeHints(fields: Record<string, string>): Record<string, string> {
  return givenOf(HINTS, fields);
}

/** A saved source's hints, and the password field as it holds it, into the form. */
export function snowflakeSeed(
  hints: Record<string, unknown>,
  passwordRef: string | null | undefined,
): AuthSeed {
  return {
    authType: SNOWFLAKE_PASSWORD,
    authFields: { ...heldOf(HINTS, hints), ...(passwordRef ? { password: passwordRef } : {}) },
  };
}
