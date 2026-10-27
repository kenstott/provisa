// Copyright (c) 2026 Kenneth Stott
// Canary: 5eb8eb31-018f-4d5a-9d58-0f6e74f17599
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// Canonical IR data-type vocabulary (REQ-846) for the schema-discovery UI. The authoritative list
// lives in provisa/core/ir_types.py and is fetched at runtime (fetchIrTypes); IR_TYPES_FALLBACK is
// used only if that fetch fails. Names are engine-independent (text, not VARCHAR) — the landing
// write face maps IR → the store's physical type.

export const IR_TYPES_FALLBACK: string[] = [
  "bigint",
  "boolean",
  "bytea",
  "date",
  "double",
  "float",
  "integer",
  "interval",
  "json",
  "numeric",
  "smallint",
  "text",
  "time",
  "timestamp",
  "timestamptz", // REQ-2050: an instant, in UTC with its zone
  "uuid",
];

// Common native/engine spellings → canonical IR name, mirroring the backend aliases so a discovered
// column lands on a valid IR dropdown value. Unmapped spellings fall through lowercased.
const NATIVE_TO_IR: Record<string, string> = {
  varchar: "text",
  "character varying": "text",
  char: "text",
  character: "text",
  string: "text",
  json: "json", // REQ-980
  jsonb: "json",
  int: "integer",
  int4: "integer",
  int2: "smallint",
  int8: "bigint",
  tinyint: "smallint",
  bool: "boolean",
  real: "float",
  float4: "float",
  float8: "double",
  "double precision": "double",
  decimal: "numeric",
  datetime: "timestamp",
  "timestamp without time zone": "timestamp",
  "timestamp with time zone": "timestamptz",
  timestamptz: "timestamptz",
  varbinary: "bytea",
  blob: "bytea",
};

export function toIrType(native: string): string {
  const base = native.split("(")[0].trim().toLowerCase();
  return NATIVE_TO_IR[base] ?? base;
}

/** REQ-2171: a column's type as shown: its base, with its subtype beside it when it has one. */
export function shownType(dataType: string | null | undefined, subtype?: string | null): string {
  if (!dataType) return "";
  return subtype ? `${toIrType(dataType)} · ${subtype}` : toIrType(dataType);
}

// REQ-1961: a numeric column's size. Stored in one spelling, `numeric(p,s)`; a bare `numeric`
// declares none and is read as floating point.
export const NUMERIC_MAX_PRECISION = 76;

export interface NumericSize {
  precision: number;
  scale: number;
}

const SIZED_NUMERIC = /^\s*(?:numeric|decimal)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)\s*$/i;

/** The precision and scale a column type declares, or null when it declares none. */
export function numericSize(dataType: string): NumericSize | null {
  const sized = SIZED_NUMERIC.exec(dataType);
  if (!sized) return null;
  return { precision: Number(sized[1]), scale: sized[2] === undefined ? 0 : Number(sized[2]) };
}

/** Whether a precision and scale size a numeric column: 1 to 76 digits, 0 to all of them after the point. */
export function isNumericSize(precision: number, scale: number): boolean {
  return (
    Number.isInteger(precision) &&
    Number.isInteger(scale) &&
    precision >= 1 &&
    precision <= NUMERIC_MAX_PRECISION &&
    scale >= 0 &&
    scale <= precision
  );
}

/** The stored type of a numeric column: sized, or bare when it declares no size. */
export function numericType(size: NumericSize | null): string {
  return size ? `numeric(${size.precision},${size.scale})` : "numeric";
}
