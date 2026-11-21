// Copyright (c) 2026 Kenneth Stott
// Canary: 706fc73f-8564-4ab1-9c04-8dd6c1f49c73
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-2249

import { serverMessage } from "../../i18n/serverMessage";
import type { IncrementalOption } from "../../types/admin";

/** The refusal of a save with the incremental setting on and no interval between whole rebuilds. */
export const NEEDS_REBUILD_EVERY = "schema.incremental_needs_rebuild_every";

/**
 * Why the incremental setting cannot be turned on, as the reader's language words it: the
 * option's code in the serverErrors catalog, with what it names (`params`). Undefined while the
 * setting is available.
 */
export function incrementalReason(
  option: IncrementalOption,
  params: Record<string, string>,
): string | undefined {
  if (option.available) return undefined;
  return serverMessage({ code: option.code, params, message: option.reason }, "");
}

/** The interval a form holds as text, in whole seconds; null when none is given. */
export function rebuildEverySeconds(text: string): number | null {
  const seconds = parseInt(text, 10);
  return Number.isFinite(seconds) && seconds > 0 ? seconds : null;
}
