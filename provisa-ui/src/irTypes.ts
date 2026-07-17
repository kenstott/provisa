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
  "numeric",
  "smallint",
  "text",
  "time",
  "timestamp",
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
  json: "text",
  jsonb: "text",
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
  "timestamp with time zone": "timestamp",
  timestamptz: "timestamp",
  varbinary: "bytea",
  blob: "bytea",
};

export function toIrType(native: string): string {
  const base = native.split("(")[0].trim().toLowerCase();
  return NATIVE_TO_IR[base] ?? base;
}
