// Copyright (c) 2026 Kenneth Stott
// Canary: 8f82caa0-6292-4859-84af-c3b0688338e7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A list's filter chip, remembered per viewer in browser storage so the list reopens as it was
// left. A per-viewer convenience, so every storage access is wrapped: a private window or blocked
// site data must never break the list, it just does not remember. A list's search box remembers
// itself (components/admin/FilterInput).

import { useCallback, useState } from "react";
import { getStored, removeStored, setStored } from "../lib/browserStorage";

function readChoice<T extends string>(key: string, choices: readonly T[]): T | null {
  try {
    const saved = getStored(key);
    if (saved === null) return null;
    if (choices.includes(saved as T)) return saved as T;
    // A choice the list no longer offers is dropped.
    removeStored(key);
    return null;
  } catch {
    return null;
  }
}

/** The choice among ``choices`` and its setter, which remembers it; ``initial`` with none saved. */
export function useRememberedChoice<T extends string>(
  key: string,
  choices: readonly T[],
  initial: T,
): [T, (choice: T) => void] {
  const [choice, setChoice] = useState<T>(() => readChoice(key, choices) ?? initial);
  const choose = useCallback(
    (next: T) => {
      setChoice(next);
      try {
        setStored(key, next);
      } catch {
        // Blocked storage: the choice holds while the page is open and is not kept.
      }
    },
    [key],
  );
  return [choice, choose];
}
