// Copyright (c) 2026 Kenneth Stott
// Canary: e74ffe04-7a0f-4296-ac87-f95525fa09e3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { notifications } from "@mantine/notifications";
import { serverMessage } from "../i18n/serverMessage";
import type { MutationWarning } from "../types/admin";

/** REQ-1919: a change that was made can carry warnings — an object a config file declares was
 *  edited or deleted, and the next load of the config re-applies the file. They ride on every
 *  mutation result and on the REST answers of the same changes. */

/** The warnings carried by the results in one GraphQL response's `data`, or by one REST answer. */
export function warningsIn(data: unknown): MutationWarning[] {
  if (data === null || typeof data !== "object") return [];
  const own = (data as { warnings?: unknown }).warnings;
  const results = Array.isArray(own) ? [data] : Object.values(data as Record<string, unknown>);
  return results.flatMap((result) => {
    const listed = (result as { warnings?: unknown } | null)?.warnings;
    return Array.isArray(listed) ? (listed as MutationWarning[]) : [];
  });
}

/** Show each warning in `data` to the person who made the change. It stays until dismissed: the
 *  change went through, so nothing else on the page says there is something to read. */
export function announceWarnings(data: unknown): void {
  for (const warning of warningsIn(data)) {
    notifications.show({
      color: "yellow",
      autoClose: false,
      message: serverMessage(warning, warning.message),
    });
  }
}
