// Copyright (c) 2026 Kenneth Stott
// Canary: 3e8b5d10-7f2c-4a96-8c1e-5b0d9a6f2e47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The ERD's view options -- what its toolbar sets -- as one value, kept across sessions: the one
// place a saved layout reads them from and writes them back to.

import { useCallback, useEffect, useState } from "react";
import { getStored, setStored } from "../../lib/browserStorage";
import type { ColumnDetail } from "./erd-model";

export interface ErdOptions {
  columnDetail: ColumnDetail;
  edgeRouting: "bezier" | "taxi";
  /** The grid step in pixels; 0 for none. */
  gridSnap: number;
  showOrphans: boolean;
}

const KEYS: Record<keyof ErdOptions, string> = {
  columnDetail: "erd.columnDetail",
  edgeRouting: "erd.edgeRouting",
  gridSnap: "erd.gridSnap",
  showOrphans: "erd.showOrphans",
};

export function readErdOptions(): ErdOptions {
  return {
    columnDetail: (getStored(KEYS.columnDetail) as ColumnDetail | null) ?? "key",
    edgeRouting: (getStored(KEYS.edgeRouting) as "bezier" | "taxi" | null) ?? "bezier",
    gridSnap: parseInt(getStored(KEYS.gridSnap) ?? "0", 10),
    showOrphans: getStored(KEYS.showOrphans) !== "false",
  };
}

export function writeErdOptions(options: ErdOptions): void {
  for (const key of Object.keys(KEYS) as (keyof ErdOptions)[])
    setStored(KEYS[key], String(options[key]));
}

/** The options, and a change to some of them, kept as it is made. */
export function useErdOptions(): [ErdOptions, (change: Partial<ErdOptions>) => void] {
  const [options, setOptions] = useState(readErdOptions);
  useEffect(() => writeErdOptions(options), [options]);
  const change = useCallback(
    (part: Partial<ErdOptions>) => setOptions((before) => ({ ...before, ...part })),
    [],
  );
  return [options, change];
}
