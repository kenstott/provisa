// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Relationship, RegisteredTable } from "../../types/admin";

export type ResultTab = "results" | "profile" | "errors" | "history" | "stats";
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
  selectedCols: Set<string>;
  onToggleCol: (colName: string) => void;
}

export const CARD_W = 200;
export const CARD_HEADER_H = 34;
export const COL_ROW_H = 27;

export const HISTORY_KEY = "sql_modeling_history";
export const HISTORY_MAX = 50;
export const SQL_QUERY_KEY = "provisa.sql.query"; // legacy single-doc key (migration source)
export const NL_PROMPT_KEY = "provisa.sql.nl_prompt"; // legacy single-doc key (migration source)
export const TABS_KEY = "provisa.sql.tabs";
export const tabSqlKey = (id: string) => `provisa.sql.tab.${id}`;
export const tabNlKey = (id: string) => `provisa.sql.tab.${id}.nl`;
export const tabResultsKey = (id: string) => `provisa.sql.tab.${id}.results`;

export const PAGE_SIZE = 100;
export const COL_MAX = 280;
export const COL_MIN = 60;
export const CHAR_PX = 7.5;
export const DOMAIN_PAGE_SIZE = 30;

export interface SqlResults {
  columns: string[];
  rows: Record<string, unknown>[];
  error: string;
}

export interface SqlTab {
  id: string;
  title: string;
  sqlText: string;
  nlText: string;
  resultColumns: string[];
  resultRows: Record<string, unknown>[];
  resultError: string;
  execMs: number | null;
}

export interface ViewColumnConfig {
  name: string;
  alias: string;
  description: string;
  scope: "domain" | "public" | "restricted";
  visibleTo: string[];
  maskType: "" | "regex" | "constant" | "truncate";
  maskPattern: string;
  maskReplace: string;
  maskValue: string;
  maskPrecision: string;
  unmaskedTo: string;
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
