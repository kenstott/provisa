// Copyright (c) 2026 Kenneth Stott
// Canary: 2847a029-af16-475c-a1b4-7b7fe43fd342
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { HISTORY_KEY, HISTORY_MAX } from "./types";
import type { HistoryEntry } from "./types";
import { getStoredJson, setStored } from "../../lib/browserStorage";

export function loadHistory(): HistoryEntry[] {
  return getStoredJson<HistoryEntry[]>(HISTORY_KEY) ?? [];
}

export function saveHistory(entries: HistoryEntry[]) {
  setStored(HISTORY_KEY, JSON.stringify(entries.slice(0, HISTORY_MAX)));
}
