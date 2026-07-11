// Copyright (c) 2026 Kenneth Stott
// Canary: 86e3a208-fe01-4c31-997c-e38e02935844
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Relationship, RegisteredTable } from "../../types/admin";

export interface ModelingCandidate {
  id: string;
  sourceTable: string;
  sourceCol: string;
  targetTable: string;
  targetCol: string;
  cardinality: string;
  promoted: boolean;
  existingRel?: Relationship;
}

export interface Props {
  tables: RegisteredTable[];
  existingRels: Relationship[];
  onClose: () => void;
  onPromote?: (candidate: ModelingCandidate) => Promise<void>;
}

export type ResultTab = "results" | "profile" | "candidates" | "errors" | "history";
export type TopTab = "sql" | "canvas";

export interface HistoryEntry {
  sql: string;
  role: string;
  executedAt: number;
  durationMs: number;
  rowCount: number;
  error: string;
}

// Canvas types
export interface CanvasTable {
  tableName: string;
  x: number;
  y: number;
}
export interface CanvasJoin {
  id: string;
  fromTable: string;
  fromCol: string;
  toTable: string;
  toCol: string;
  cardinality: "many-to-one" | "one-to-many";
}
export interface JoinCanvasProps {
  tables: RegisteredTable[];
  existingRels: Relationship[];
  onGenerateSql: (sql: string) => void;
}
export interface CanvasTableCardProps {
  ct: CanvasTable;
  tbl: RegisteredTable;
  onMove: (x: number, y: number) => void;
  onRemove: () => void;
  onStartConnect: (colName: string) => void;
}

export interface ColumnProfile {
  col: string;
  nullCount: number;
  blankCount: number;
  distinctCount: number;
  constantValue: unknown | undefined;
  min: number | string | null;
  max: number | string | null;
  mean: number | null;
  topValues: { value: string; count: number }[];
}

export const CARD_W = 200;
export const CARD_HEADER_H = 34;
export const COL_ROW_H = 27;

export const HISTORY_KEY = "sql_modeling_history";
export const HISTORY_MAX = 50;

export const PAGE_SIZE = 100;
export const COL_MAX = 280;
export const COL_MIN = 60;
export const CHAR_PX = 7.5; // approximate px per character at 0.78rem

export const normalizeDomain = (id: string) =>
  id.replace(/[^a-zA-Z0-9]/g, "_").replace(/^_+|_+$/g, "");
