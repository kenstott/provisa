// Copyright (c) 2026 Kenneth Stott
// Canary: a3d9e2f1-7b4c-4a8e-9d5f-2c1b6e3a7f8d
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

export interface ErdModalProps {
  tables: RegisteredTable[];
  relationships: Relationship[];
  domains: Domain[];
  activeDomain: string | null;
  onClose: () => void;
}
