// Copyright (c) 2026 Kenneth Stott
// Canary: 9c1d47b2-8e5a-4f36-b0d7-25a1c6e93f84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * Expand/collapse state for the expandable section panels, kept per panel so one a user opened
 * stays open on the next visit. Mantine's single-value Accordion carries `string | null` (the open
 * item, or nothing); the `multiple` variant carries `string[]` and TeamPage persists its own.
 */
import { useLocalStorage } from "../components/graph/graph-persistence";

export function usePanelState(key: string, initial: string | null = null) {
  return useLocalStorage<string | null>(`provisa.panel.${key}`, initial);
}
