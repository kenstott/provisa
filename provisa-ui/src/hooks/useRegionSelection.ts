// Copyright (c) 2026 Kenneth Stott
// Canary: 216ef7dd-311a-4f46-aef6-81a934857d2e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: the admin region selection, remembered per viewer in browser storage. The choice is a
// per-viewer convenience, so every storage access is wrapped — a private window or blocked site data
// must never break the admin, it just does not remember the selection.

import { useCallback, useState } from "react";
import { coerceSelection, type RegionSelection } from "./regionFilter";

const KEY = "provisa.admin.regionFilter";

function readStored(): string | null {
  try {
    return localStorage.getItem(KEY);
  } catch {
    return null;
  }
}

function writeStored(value: string): void {
  try {
    localStorage.setItem(KEY, value);
  } catch {
    // Per-viewer convenience only: nothing to do if storage is unavailable.
  }
}

/**
 * The region selection for the admin lists and a setter that remembers it for this viewer.
 *
 * ``regions``/``connected`` come from :func:`useRegionChoices` and arrive after the first render, so
 * the effective selection is coerced on every render: a stored region that still exists is kept, and
 * anything else (nothing stored yet, or a region since removed) falls back to the connected-region
 * default once that is known.
 */
export function useRegionSelection(
  regions: readonly string[],
  connected: string | null,
): [RegionSelection, (selection: RegionSelection) => void] {
  // The viewer's explicit/stored choice; null until they pick or a value is read from storage.
  const [chosen, setChosen] = useState<string | null>(() => readStored());
  const selection = coerceSelection(chosen, regions, connected);
  const setSelection = useCallback((picked: RegionSelection) => {
    setChosen(picked);
    writeStored(picked);
  }, []);
  return [selection, setSelection];
}
