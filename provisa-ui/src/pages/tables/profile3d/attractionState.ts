// Copyright (c) 2026 Kenneth Stott
// Canary: 13bd368c-dffc-4880-9ff2-8c3983c8b33c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Attraction slider's value (Attraction.tsx): how close a force-laid-out 3D graph draws its nodes: one slider, loose to tight, in each such
// graph's toolbar. 1 is the usual spacing. The value lasts for the browser session per graph (this
// tab, never the model); a graph with a fixed layout (the terrain, the strata) has none.

import { useState } from "react";
import { getStored, setStored } from "../../../lib/browserStorage";

export const ATTRACTION_MIN = 0.5;
export const ATTRACTION_MAX = 2;

const keyOf = (graph: string) => `graph3d.attraction.${graph}`;

function readAttraction(graph: string): number {
  try {
    const v = Number(getStored(keyOf(graph), { session: true }));
    return v >= ATTRACTION_MIN && v <= ATTRACTION_MAX ? v : 1;
  } catch {
    // A per-viewer convenience only: blocked or cleared storage means the usual spacing.
    return 1;
  }
}

/** ``graph``'s attraction for this session, and its setter. */
export function useAttraction(graph: string): [number, (v: number) => void] {
  const [value, setValue] = useState(() => readAttraction(graph));
  const set = (v: number) => {
    setValue(v);
    try {
      setStored(keyOf(graph), String(v), { session: true });
    } catch {
      // Blocked storage: the spacing holds while the graph is open and is not kept.
    }
  };
  return [value, set];
}
