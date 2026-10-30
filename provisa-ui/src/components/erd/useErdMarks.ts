// Copyright (c) 2026 Kenneth Stott
// Canary: 2a9e6d41-7c13-4f58-b0e2-8d3c5f1a6b97
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.


// REQ-2187: what is marked on the drawn diagram: the tables a focus names (highlighted and fitted
// into view) and what the page's filter box matches (the rest dimmed). Both are classes on the drawn
// elements, applied when they change and again by every layout, which draws the elements anew --
// so a keystroke moves and rebuilds nothing.

import { useCallback, useEffect, useRef, useState, type RefObject } from "react";
import type { CyInstance } from "../graph/cytoscape-types";
import { applyErdSearch, highlightErdTables } from "./erd-focus";

export function useErdMarks(
  cyRef: RefObject<CyInstance | null>,
  highlightTableIds: number[],
  search: string,
  page: "tables" | "relationships",
) {
  const highlightRef = useRef(highlightTableIds);
  const searchRef = useRef({ search, page });
  const [highlighted, setHighlighted] = useState<number[]>([]);
  const applyHighlight = useCallback((cy: CyInstance) => {
    setHighlighted(highlightErdTables(cy, highlightRef.current));
    applyErdSearch(cy, searchRef.current.search, searchRef.current.page);
  }, []);
  useEffect(() => {
    highlightRef.current = highlightTableIds;
    if (cyRef.current) applyHighlight(cyRef.current);
  }, [highlightTableIds, applyHighlight, cyRef]);
  useEffect(() => {
    searchRef.current = { search, page };
    if (cyRef.current) applyErdSearch(cyRef.current, search, page);
  }, [search, page, cyRef]);
  return { highlighted, applyHighlight };
}
