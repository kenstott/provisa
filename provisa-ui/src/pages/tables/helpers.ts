// Copyright (c) 2026 Kenneth Stott
// Canary: b9c58944-862c-4c3c-9aaa-f1bcc80776b8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { TFunction } from "i18next";
import { grantsLabel } from "../../lib/grants";
import type { RegisteredTable } from "../../types/admin";
import type { MaskEntry } from "../../lib/masks";

/** Map a RegisteredTable to the full TableInput payload for updateTable. Shared by the
 * table edit form and the REQ-1318 view definition-mode editor so both send the same
 * complete input (updateTable takes the whole TableInput). */
export function buildTableUpdateInput(t: RegisteredTable): Record<string, unknown> {
  const withheld = governanceWithheld(t);
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
    dqContract: t.dqContract || null, // REQ-1443
    profilerSourceId: t.profilerSourceId || null, // REQ-1934
    queryTemplate: t.queryTemplate || null, // REQ-1670
    fileGlob: t.fileGlob || null, // REQ-788
    sourceFileColumn: t.sourceFileColumn || null, // REQ-788
    arguments: t.arguments ?? null, // REQ-2061
    landingFilter: t.landingFilter || null, // REQ-2062
    materialize: t.materialize,
    mvRefreshInterval: t.mvRefreshInterval,
    mvDebounceQuiet: t.mvDebounceQuiet,
    mvDebounceMaxDelay: t.mvDebounceMaxDelay,
    pushDebounceQuiet: t.pushDebounceQuiet,
    pushDebounceMaxDelay: t.pushDebounceMaxDelay,
    mvConsistency: t.mvConsistency,
    mvPreprocess: t.mvPreprocess || null, // REQ-957
    businessKey: t.businessKey, // REQ-2109
    validTime: t.validTime, // REQ-2109
    recordedTime: t.recordedTime, // REQ-2109
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
    // REQ-1634. REQ-1443 clause 10: a checker table (the only kind registration lets carry a
    // contract) inherits its product from the table it scans; the server derives what it shows
    // as productId and refuses one on the row, so the payload never carries it back.
    productId: t.dqContract ? null : t.productId,
    enableAggregates: t.enableAggregates,
    enableGroupBy: t.enableGroupBy,
    // REQ-1141: the table's load protection, saved with it (left out, the update cleared it).
    loadProtected: t.loadProtected,
    offPeakWindow: t.offPeakWindow,
    offPeakTz: t.offPeakTz,
    // REQ-2109 (settled): settings with mutations of their own travel with the table, so one save
    // applies them all in one transaction. Write grants are saved on their own section's Save.
    gqlNamingConvention: t.gqlNamingConvention || null,
    roleTtl: t.roleTtl.map(({ role, ttl }) => ({ role, ttl })),
    replicate: t.replicate,
    draft: t.draft,
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
    // REQ-874: the incremental-reload declaration. The cursor field is the table's watermark.
    delta: t.delta
      ? {
          query: t.delta.query ?? undefined,
          apply: t.delta.apply,
          deletes: t.delta.deletes,
          tombstoneColumn: t.delta.tombstoneColumn ?? undefined,
          rebuildEvery: t.delta.rebuildEvery ?? undefined,
        }
      : null,
    // REQ-1093: persist edited UNIQUE constraints; drop empty/incomplete rows.
    uniqueConstraints: (t.uniqueConstraints ?? [])
      .filter((u) => u.name.trim() && u.columns.length > 0)
      .map((u) => ({ name: u.name.trim(), columns: u.columns })),
    columns: t.columns.map((c) => ({
      name: c.columnName,
      ...governanceInput(c, withheld),
      alias: c.alias || undefined,
      description: c.description || undefined,
      dataType: c.dataType || undefined, // REQ-846: steward type override (metadata only)
      nativeFilterType: c.nativeFilterType || undefined,
      // A recorded false is a fact (the parameter is optional): only null is left out.
      nativeFilterRequired: c.nativeFilterRequired ?? undefined,
      requiredBySteward: c.requiredBySteward || undefined, // REQ-1986
      path: c.path || undefined, // REQ-1739: ingest per-column JSON extraction path
      // REQ-421, REQ-2000: carried by the form, which sets them.
      embedding: c.embedding || Boolean(c.embeddingModel) || undefined,
      embeddingModel: c.embeddingModel || undefined,
      isPrimaryKey: c.isPrimaryKey || undefined,
      isForeignKey: c.isForeignKey || undefined,
      isAlternateKey: c.isAlternateKey || undefined,
      scope: c.scope || "domain",
      fake: c.fake?.trim() || undefined, // REQ-1494
      fakeStable: c.fakeStable || undefined,
      syntheticRule: c.syntheticRule?.trim() || undefined,
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

/**
 * REQ-1958/REQ-1134: whether the table's grant lists and mask settings were withheld from this
 * caller (it lacks view_governance) -- the server answers them as null, never as an empty list.
 */
export function governanceWithheld(table: { columns: { visibleTo: string[] | null }[] }): boolean {
  return table.columns.some((c) => c.visibleTo === null);
}

type GovernanceColumn = Pick<RegisteredTable["columns"][number], "visibleTo" | "masks">;

/**
 * A column's grant lists and mask as a save sends them (REQ-1958). A caller who is not shown
 * them names none: the lists go empty and the mask is left out, which the server reads as "not
 * set by this save" and keeps what is stored. A value sent for a field the caller is not shown
 * is refused by the server, so nothing hidden is ever echoed back.
 */
export function governanceInput(c: GovernanceColumn, withheld: boolean) {
  if (withheld) {
    return { visibleTo: [] as string[] };
  }
  return {
    visibleTo: shownGrants(c.visibleTo),
    masks: shownMasks(c.masks),
  };
}

/**
 * A grant list the caller was shown. The table editor is offered only when they were
 * (governanceWithheld), so a null here is a broken invariant, never a value to save as "nobody".
 */
export function shownGrants(list: string[] | null): string[] {
  if (list === null) {
    throw new Error("grant lists were withheld; the table editor is not offered without them");
  }
  return list;
}

/** A mask list the caller was shown (see :func:`shownGrants`). */
export function shownMasks(list: MaskEntry[] | null): MaskEntry[] {
  if (list === null) {
    throw new Error("mask lists were withheld; the table editor is not offered without them");
  }
  return list;
}

/** A mask list in a few words: its entries' count, or none. */
export function masksSummary(list: MaskEntry[] | null, t: TFunction): string {
  const shown = shownMasks(list);
  return shown.length === 0 ? t("maskList.none") : t("maskList.entries", { count: shown.length });
}

/** A grant list as a reader reads it, composites as "a and b" (REQ-2058). */
export function shownGrantsLabel(list: string[] | null, t: TFunction): string {
  return grantsLabel(shownGrants(list), t);
}
