// Copyright (c) 2026 Kenneth Stott
// Canary: 5558a53e-2137-4b66-b1ec-95bfb1d97b7f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { RegisteredTable, Relationship, Domain } from "../../../types/admin";

export interface TooltipState {
  visible: boolean;
  x: number;
  y: number;
  title: string;
  body: string;
}

export interface ErdPanelProps {
  tables: RegisteredTable[];
  relationships: Relationship[];
  domains: Domain[];
  // Domains to render, driven by the host page's own domain filter. Null means unfiltered (show all).
  checkedDomains: Set<string> | null;
  // Omit to embed as a plain panel (no close button, no modal chrome).
  onClose?: () => void;
}

export interface ErdModalProps {
  tables: RegisteredTable[];
  relationships: Relationship[];
  domains: Domain[];
  checkedDomains: Set<string> | null;
  onClose: () => void;
}
