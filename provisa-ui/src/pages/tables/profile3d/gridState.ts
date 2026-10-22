// Copyright (c) 2026 Kenneth Stott
// Canary: fe7eef4e-0a1b-4b64-9789-821f11ae07ab
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Whether a 3D network graph draws its floor grid: one checkbox in each such graph's toolbar,
// checked at first. The choice lasts for the browser session per graph (this tab, never the model),
// as the attraction does (attractionState.ts).

import { useState } from "react";
import { getStored, setStored } from "../../../lib/browserStorage";

const keyOf = (graph: string) => `graph3d.grid.${graph}`;

function readGrid(graph: string): boolean {
  try {
    return getStored(keyOf(graph), { session: true }) !== "false";
  } catch {
    // A per-viewer convenience only: blocked or cleared storage means the grid is drawn.
    return true;
  }
}

/** Whether ``graph`` draws its floor grid this session, and its setter. */
export function useGrid(graph: string): [boolean, (shown: boolean) => void] {
  const [value, setValue] = useState(() => readGrid(graph));
  const set = (shown: boolean) => {
    setValue(shown);
    try {
      setStored(keyOf(graph), String(shown), { session: true });
    } catch {
      // Blocked storage: the choice holds while the graph is open and is not kept.
    }
  };
  return [value, set];
}
