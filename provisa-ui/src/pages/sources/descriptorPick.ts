// Copyright (c) 2026 Kenneth Stott
// Canary: 4c23a1ce-b849-4a1a-9f3f-e0a91cf7713f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2114 (amended 2026-11-24): the per-type builders write, and read back, only the keys the
// server's descriptor lists (./descriptor). These pick those keys out of the form's fields and out
// of a saved source's settings.

/** How a saved source opens in the form: the auth mode it was saved in and the fields it fills. */
export interface AuthSeed {
  authType: string;
  authFields: Record<string, string>;
}

/** Each of `keys` the form gives a value for, as given. */
export function givenOf(
  keys: readonly string[],
  fields: Record<string, string | undefined>,
): Record<string, string> {
  return Object.fromEntries(keys.filter((k) => fields[k]).map((k) => [k, fields[k] as string]));
}

/** Each of `keys` a saved source holds, as held. */
export function heldOf(
  keys: readonly string[],
  saved: Record<string, unknown>,
): Record<string, string> {
  return Object.fromEntries(
    keys.filter((k) => saved[k] !== undefined).map((k) => [k, String(saved[k])]),
  );
}
