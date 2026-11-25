// Copyright (c) 2026 Kenneth Stott
// Canary: 4f060d67-139d-4b5c-a9e5-6c8263146ef4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1731: a HiveServer2 source's SASL mechanism (provisa/executor/drivers/hive.py's
// HiveDriver.configure() reads `auth_mechanism` from the hints). The driver's own default is
// saved as no hint, so a source opens again as it was saved.

import { fieldDefault, hintKey } from "./descriptor";

export const HIVESERVER2 = "hiveserver2";
// REQ-2114: the key and the driver's default are the descriptor's
// (provisa/api/admin/source_types.py).
const AUTH_MECHANISM = hintKey(HIVESERVER2, "auth_mechanism");
const DEFAULT_MECHANISM = fieldDefault(HIVESERVER2, "federationHintsJson", AUTH_MECHANISM);

/** The hints a form saves: the mechanism, when it is not the driver's default. */
export function hiveserver2Hints(fields: Record<string, string>): Record<string, string> {
  const mechanism = fields[AUTH_MECHANISM];
  return mechanism && mechanism !== DEFAULT_MECHANISM ? { [AUTH_MECHANISM]: mechanism } : {};
}

/** A saved source's hints into the form's fields. */
export function hiveserver2Fields(hints: Record<string, unknown> | null): Record<string, string> {
  const mechanism = hints?.[AUTH_MECHANISM];
  return mechanism ? { [AUTH_MECHANISM]: String(mechanism) } : {};
}
