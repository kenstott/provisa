// Copyright (c) 2026 Kenneth Stott
// Canary: 5558a53e-2137-4b66-b1ec-95bfb1d97b7f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { ListedTable, Relationship, Domain } from "../../../types/admin";

export interface TooltipState {
  visible: boolean;
  x: number;
  y: number;
  title: string;
  body: string;
}

export interface ErdPanelProps {
  /** The domains a link asks to collapse, "all" for every one; null when it asks nothing. */
  collapsed?: string | null;
  /** Reports the collapsed domains, "all" when every visible one is. */
  onCollapsed?: (collapsed: string[] | "all") => void;
  tables: ListedTable[];
  relationships: Relationship[];
  domains: Domain[];
  // Domains to render, driven by the host's filter or focus. Null means unfiltered (show all).
  checkedDomains: Set<string> | null;
  // REQ-2187: the tables highlighted and fitted into view.
  highlightTableIds?: number[];
  // The relationship whose detail panel is open: its line highlighted and fitted into view.
  highlightRelationshipIds?: string[];
  // The page's filter box: what matches is marked and the rest dimmed, nothing rebuilt.
  search?: string;
  page?: "tables" | "relationships";
  onTableClick: (tableId: number, tableName: string) => void;
  // A line in the diagram clicked: a host with no use for it leaves it out.
  onRelationshipClick?: (relationshipId: string) => void;
}
