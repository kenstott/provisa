// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2124: the drift strata's model — one column's distribution in each of several runs, cut into
// the same bins so the runs can stand one behind another and be compared.

import type { ProfileRunResults } from "../../../../api/profiler";
import { formatValue, sketchCdf, type TerrainFamily } from "../terrain/terrainModel";

type Row = Record<string, unknown>;

export const STRATA_BINS = 40;
const MAX_CATEGORY_BINS = 24;
const FENCE_IQR = 3;

export interface StrataRunInput {
  runId: string;
  runTime: string;
  results: ProfileRunResults;
}

export interface StrataRun {
  runId: string;
  runTime: string;
  /** Each bin's share of the run's non-null rows; empty where this run holds no values for it. */
  shares: number[];
  rows: number | null;
  nullShare: number | null;
  /** The run's median along the bins, 0..1; sketch columns only. */
  medianT: number | null;
  median: string | null;
  driftReason: string | null;
}

export interface StrataModel {
  column: string;
  family: TerrainFamily;
  kind: "sketch" | "categories" | "none";
  ordered: boolean;
  smooth: boolean;
  labels: string[];
  runs: StrataRun[];
  maxShare: number;
}

const num = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);

function columnRow(r: ProfileRunResults, column: string): Row | undefined {
  return (r.columns ?? []).find((c) => String(c.column_name) === column);
}

function sketchOf(r: ProfileRunResults, column: string): number[] | null {
  const s = (r.quantiles ?? [])
    .filter((q) => String(q.column_name) === column && q.measure === "value")
    .sort((a, b) => Number(a.q) - Number(b.q))
    .map((q) => Number(q.value));
  return s.length === 101 ? s : null;
}

function valuesOf(r: ProfileRunResults, column: string): Row[] {
  const vals = (r.top_values ?? []).filter(
    (v) => String(v.column_name) === column && v.value != null,
  );
  const freq = vals.filter((v) => v.kind === "frequency");
  return freq.length > 0 ? freq : vals.filter((v) => v.kind === "top");
}

const isFrequency = (vs: Row[]) => vs.length > 0 && vs[0].kind === "frequency";

function driftOf(r: ProfileRunResults, column: string): string | null {
  const d = (r.drift ?? []).find((x) => x.drifting === true && String(x.column_name) === column);
  return d ? String(d.drift_reason) : null;
}

/** Columns that any of the runs can draw, in the newest run's order. */
export function strataColumns(runs: StrataRunInput[]): string[] {
  const out: string[] = [];
  for (const run of [...runs].reverse()) {
    for (const c of run.results.columns ?? []) {
      const name = String(c.column_name);
      if (
        !out.includes(name) &&
        (sketchOf(run.results, name) || valuesOf(run.results, name).length > 0)
      )
        out.push(name);
    }
  }
  return out;
}

/** ``runs`` oldest first. */
export function buildStrata(runs: StrataRunInput[], column: string): StrataModel {
  // The column is listed by strataColumns only when some run profiled it.
  const latest = [...runs].reverse().find((r) => columnRow(r.results, column)) as StrataRunInput;
  const family = String((columnRow(latest.results, column) as Row).family) as TerrainFamily;
  const sketches = runs.map((r) => sketchOf(r.results, column));
  const values = runs.map((r) => valuesOf(r.results, column));
  const base = runs.map((r) => {
    const c = columnRow(r.results, column);
    const rows = c ? num(c.row_count) : null;
    const nulls = c ? num(c.null_count) : null;
    return {
      runId: r.runId,
      runTime: r.runTime,
      rows,
      nonNull: rows != null && nulls != null ? rows - nulls : null,
      nullShare: c ? num(c.null_share) : null,
      driftReason: driftOf(r.results, column),
    };
  });
  // As on the terrain: exact value counts first, then the quantile sketches, then top values.
  const useValues = values.some(isFrequency) || !sketches.some((s) => s != null);
  if (!useValues) {
    let lo = Infinity;
    let hi = -Infinity;
    for (const s of sketches) {
      if (!s) continue;
      const iqr = s[75] - s[25];
      lo = Math.min(lo, iqr > 0 ? Math.max(s[0], s[25] - FENCE_IQR * iqr) : s[0]);
      hi = Math.max(hi, iqr > 0 ? Math.min(s[100], s[75] + FENCE_IQR * iqr) : s[100]);
    }
    if (hi === lo) hi = lo + 1;
    const span = hi - lo;
    const w = span / STRATA_BINS;
    const labels = Array.from(
      { length: STRATA_BINS },
      (_, j) =>
        `${formatValue(family, lo + j * w, span)} – ${formatValue(family, lo + (j + 1) * w, span)}`,
    );
    const out = base.map((b, i) => {
      const s = sketches[i];
      if (!s) return { ...b, shares: [], medianT: null, median: null };
      const shares = Array.from(
        { length: STRATA_BINS },
        (_, j) => sketchCdf(s, lo + (j + 1) * w) - sketchCdf(s, lo + j * w),
      );
      return {
        ...b,
        shares,
        medianT: Math.min(1, Math.max(0, (s[50] - lo) / span)),
        median: formatValue(family, s[50], span),
      };
    });
    return finish(column, family, "sketch", true, true, labels, out);
  }
  if (values.some((v) => v.length > 0)) {
    const ordered = family === "numeric" || family === "temporal";
    const totals = new Map<string, number>();
    values.forEach((vs, i) => {
      const nn = base[i].nonNull;
      // A run whose counts the role may not see adds nothing to the ordering.
      if (!nn) return;
      for (const v of vs)
        totals.set(String(v.value), (totals.get(String(v.value)) ?? 0) + Number(v.row_count) / nn);
    });
    let labels = [...totals.keys()];
    if (ordered)
      labels.sort((a, b) => (family === "numeric" ? Number(a) - Number(b) : a.localeCompare(b)));
    else labels.sort((a, b) => (totals.get(b) as number) - (totals.get(a) as number));
    const capped = !ordered && labels.length > MAX_CATEGORY_BINS;
    const kept = capped ? labels.slice(0, MAX_CATEGORY_BINS - 1) : labels;
    const index = new Map(kept.map((l, i) => [l, i]));
    if (capped) labels = [...kept, `+${labels.length - kept.length}`];
    const out = base.map((b, i) => {
      const vs = values[i];
      if (vs.length === 0 || !b.nonNull) return { ...b, shares: [], medianT: null, median: null };
      const shares = new Array<number>(labels.length).fill(0);
      for (const v of vs) {
        // Only a capped axis leaves values out of the index; they go to its last, "+N" bin.
        const j = index.get(String(v.value)) ?? labels.length - 1;
        shares[j] += Number(v.row_count) / b.nonNull;
      }
      return { ...b, shares, medianT: null, median: null };
    });
    return finish(column, family, "categories", ordered, false, labels, out);
  }
  return finish(
    column,
    family,
    "none",
    false,
    false,
    [],
    base.map((b) => ({ ...b, shares: [], medianT: null, median: null })),
  );
}

function finish(
  column: string,
  family: TerrainFamily,
  kind: StrataModel["kind"],
  ordered: boolean,
  smooth: boolean,
  labels: string[],
  runs: (StrataRun & { nonNull: number | null })[],
): StrataModel {
  let maxShare = 0;
  for (const r of runs) for (const s of r.shares) maxShare = Math.max(maxShare, s);
  return {
    column,
    family,
    kind,
    ordered,
    smooth,
    labels,
    runs: runs.map(({ nonNull: _nonNull, ...r }) => r),
    // With no values in any run nothing is drawn against it.
    maxShare: maxShare || 1,
  };
}
