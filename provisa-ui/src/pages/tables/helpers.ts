// Copyright (c) 2026 Kenneth Stott
// Canary: 7c2e5d83-4a1f-4b6e-9d0c-2f8a3e7b1c94
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { RegisteredTable } from "../../types/admin";

export function normalizeDomain(domain: string): string {
  return domain.replace(/[^a-zA-Z0-9]/g, "_").replace(/^_+|_+$/g, "");
}

/** Map a RegisteredTable to the full TableInput payload for updateTable. Shared by the
 * table edit form and the REQ-1318 view definition-mode editor so both send the same
 * complete input (updateTable takes the whole TableInput). */
export function buildTableUpdateInput(t: RegisteredTable): Record<string, unknown> {
  return {
    sourceId: t.sourceId,
    domainId: t.domainId,
    schemaName: t.schemaName,
    tableName: t.tableName,
    alias: t.alias || undefined,
    description: t.description || undefined,
    watermarkColumn: t.watermarkColumn || null,
    changeSignal: t.changeSignal || null,
    probeQuery: t.probeQuery || null,
    probeType: t.probeType || null,
    viewSql: t.viewSql || undefined,
    materialize: t.materialize,
    mvRefreshInterval: t.mvRefreshInterval,
    mvDebounceQuiet: t.mvDebounceQuiet,
    mvDebounceMaxDelay: t.mvDebounceMaxDelay,
    mvConsistency: t.mvConsistency,
    mvPreprocess: t.mvPreprocess || null, // REQ-957
    mvBitemporalMode: t.mvBitemporalMode || null, // REQ-1162
    mvBitemporalKey: t.mvBitemporalKey, // REQ-1162
    mvPersist: t.mvPersist, // REQ-965
    // REQ-970: the MV row-identity key IS the table Primary Key — derive it from the per-column
    // PK checkboxes (single source of truth), not a separate field.
    mvPrimaryKey: t.columns.filter((c) => c.isPrimaryKey).map((c) => c.columnName),
    mvIncremental: t.mvIncremental, // REQ-969
    mvCalendar: t.mvCalendar || null, // REQ-962
    mvGrain: t.mvGrain || null, // REQ-962/1168
    mvAllowedLateness: t.mvAllowedLateness, // REQ-961
    mvExpectedEvents: t.mvExpectedEvents, // REQ-961
    mvBusinessDayGrain: t.mvBusinessDayGrain, // REQ-962
    dataProduct: t.dataProduct,
    enableAggregates: t.enableAggregates,
    enableGroupBy: t.enableGroupBy,
    live: t.live
      ? {
          queryId: t.live.queryId ?? undefined,
          watermarkColumn: t.live.watermarkColumn ?? undefined,
          pollInterval: t.live.pollInterval,
          strategy: t.live.strategy,
          kafka:
            t.live.strategy === "kafka" && t.live.kafka
              ? {
                  topic: t.live.kafka.topic,
                  format: t.live.kafka.format ?? undefined,
                  keyColumn: t.live.kafka.keyColumn ?? undefined,
                }
              : undefined,
          outputs: t.live.outputs.map((o) => ({
            type: o.type,
            topic: o.topic ?? undefined,
            keyColumn: o.keyColumn ?? undefined,
            bootstrapServers: o.bootstrapServers ?? undefined,
          })),
        }
      : null,
    columnPresets: t.columnPresets,
    // REQ-1093: persist edited UNIQUE constraints; drop empty/incomplete rows.
    uniqueConstraints: (t.uniqueConstraints ?? [])
      .filter((u) => u.name.trim() && u.columns.length > 0)
      .map((u) => ({ name: u.name.trim(), columns: u.columns })),
    columns: t.columns.map((c) => ({
      name: c.columnName,
      visibleTo: c.visibleTo,
      writableBy: c.writableBy,
      unmaskedTo: c.unmaskedTo,
      maskType: c.maskType || undefined,
      maskPattern: c.maskPattern || undefined,
      maskReplace: c.maskReplace || undefined,
      maskValue: c.maskValue || undefined,
      maskPrecision: c.maskPrecision || undefined,
      alias: c.alias || undefined,
      description: c.description || undefined,
      dataType: c.dataType || undefined, // REQ-846: steward type override (metadata only)
      nativeFilterType: c.nativeFilterType || undefined,
      isPrimaryKey: c.isPrimaryKey || undefined,
      isForeignKey: c.isForeignKey || undefined,
      isAlternateKey: c.isAlternateKey || undefined,
      scope: c.scope || "domain",
    })),
  };
}

export function isWatermarkEligible(dataType: string): boolean {
  const t = dataType.toLowerCase();
  return (
    t.includes("timestamp") ||
    t.includes("datetime") ||
    t.includes("date") ||
    t === "bigint" ||
    t === "int8" ||
    t === "integer" ||
    t === "int4" ||
    t === "int" ||
    t === "int2" ||
    t === "smallint" ||
    t === "long" ||
    t === "numeric" ||
    t === "number"
  );
}

export function computeProfile(columns: string[], rows: Record<string, unknown>[]) {
  return columns.map((col) => {
    const vals = rows.map((r) => r[col]);
    const nullCount = vals.filter((v) => v === null || v === undefined).length;
    const blankCount = vals.filter((v) => typeof v === "string" && v.trim() === "").length;
    const nonNull = vals.filter((v) => v !== null && v !== undefined);
    const freq = new Map<string, number>();
    for (const v of vals) {
      const k = v === null || v === undefined ? "NULL" : String(v);
      freq.set(k, (freq.get(k) ?? 0) + 1);
    }
    const distinctCount = freq.size;
    const constantValue = distinctCount === 1 ? vals[0] : undefined;
    const numbers = nonNull.filter((v) => typeof v === "number") as number[];
    const mean = numbers.length > 0 ? numbers.reduce((a, b) => a + b, 0) / numbers.length : null;
    const sorted = [...nonNull].sort((a, b) => (a! < b! ? -1 : a! > b! ? 1 : 0));
    const min = sorted.length > 0 ? (sorted[0] as string | number) : null;
    const max = sorted.length > 0 ? (sorted[sorted.length - 1] as string | number) : null;
    const topValues = [...freq.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([value, count]) => ({ value, count }));
    return {
      col,
      nullCount,
      blankCount,
      distinctCount,
      constantValue,
      min,
      max,
      mean,
      topValues,
    };
  });
}
