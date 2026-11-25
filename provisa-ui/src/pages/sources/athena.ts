// Copyright (c) 2026 Kenneth Stott
// Canary: 952f635c-33e2-4b0b-a8ee-06bd77bff38b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1981: an Amazon Athena source's settings beside its Glue database (`database`) and its
// access key pair (`username` / `password`): its region, workgroup, result location, catalog and
// the role it assumes, saved as its federation hints (provisa/executor/drivers/athena.py).

import { hintKeys } from "./descriptor";

// REQ-2114: the hints are the descriptor's (provisa/api/admin/source_types.py).
export const ATHENA_HINTS: readonly string[] = hintKeys("athena");

/** The hints a form saves: each one given, trimmed. */
export function athenaHints(fields: Record<string, string>): Record<string, string> {
  return Object.fromEntries(
    ATHENA_HINTS.filter((k) => fields[k]?.trim()).map((k) => [k, fields[k].trim()]),
  );
}

/** Whether a result location is one Athena takes: an s3:// URL, or a reference to one. */
export function athenaResultLocationValid(location: string): boolean {
  const value = location.trim();
  return value === "" || value.startsWith("${") || /^s3:\/\/[^/\s]+\/\S*$/.test(value);
}
