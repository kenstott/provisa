// Copyright (c) 2026 Kenneth Stott
// Canary: 8972327c-8d83-4735-9fce-9cba1dae8b18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Detection + query construction for the trace/span drill-down offered on any
// results grid column that carries an OTel id. Details come from the governed
// ops table "ops"."traces" (registered by startup_seed under source
// provisa-otel, domain ops, schema signals), so the same governance that gates
// the report gates the drill-down.

export type TelemetryIdKind = "trace" | "span";

const TELEMETRY_ID_COLUMNS: Record<string, TelemetryIdKind> = {
  trace_id: "trace",
  traceid: "trace",
  span_id: "span",
  spanid: "span",
  parent_span_id: "span",
  parentspanid: "span",
};

/** The kind of telemetry id a column holds, or null when it holds none. */
export function telemetryIdKind(column: string): TelemetryIdKind | null {
  const key = column
    .trim()
    .toLowerCase()
    .replace(/[\s.-]/g, "_");
  const kind = TELEMETRY_ID_COLUMNS[key];
  return kind === undefined ? null : kind;
}

/** OTel ids are lowercase hex — 16 chars for a span, 32 for a trace. */
export const TELEMETRY_ID_RE = /^[0-9a-f]{8,}$/i;

/**
 * All spans for a trace, or the single span for a span id. Ids are interpolated,
 * so the hex shape is enforced here rather than trusted from the caller.
 */
export function traceDetailSql(kind: TelemetryIdKind, id: string): string {
  if (!TELEMETRY_ID_RE.test(id)) throw new Error(`not a telemetry id: ${id}`);
  const col = kind === "trace" ? "trace_id" : "span_id";
  return `SELECT * FROM "ops"."traces" WHERE ${col} = '${id}' ORDER BY "timestamp"`;
}

/** Attribute maps arrive as objects; render them expanded rather than as [object Object]. */
export function formatTelemetryValue(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v, null, 2);
  return String(v);
}
