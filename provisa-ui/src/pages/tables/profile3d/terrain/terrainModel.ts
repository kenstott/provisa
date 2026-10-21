// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: the profile terrain's model: one run's result rows (columns, quantiles, top_values, drift) turned
// into one ridge per column. Pure — the three.js scene and the panel both draw from it.

import type { ProfileRunResults } from "../../../../api/profiler";

type Row = Record<string, unknown>;

export type TerrainFamily = "numeric" | "temporal" | "text" | "boolean" | "other";

export interface TerrainBin {
  label: string;
  count: number;
}

export interface TerrainFlag {
  kind: "warn" | "info";
  /** An i18n key under ``profileTerrain.``. */
  key: string;
  params?: Record<string, string>;
}

export interface TerrainColumn {
  name: string;
  family: TerrainFamily;
  dataType: string;
  /** Null, like the counts below, for a column whose values the viewer's role may not see. */
  rows: number | null;
  nullCount: number | null;
  nullShare: number | null;
  distinct: number | null;
  /** No value rows reached the viewer: the ridge is not drawn. */
  restricted: boolean;
  /** ``sketch``: equal-width bins re-cut from the 101-point quantile sketch. ``frequency``: every
   * value of a low-cardinality column, counted exactly. ``top``: the most frequent values and the
   * rest as one bucket. */
  binning: "sketch" | "frequency" | "top" | "none";
  /** Bins run low to high (a value axis); otherwise most to least common. */
  ordered: boolean;
  /** Steps for discrete bins, a smooth ridge for a continuous range. */
  smooth: boolean;
  bins: TerrainBin[];
  maxCount: number;
  /** Sketch binning only: the value range the bins span and the p5/p50/p95 values. */
  lo: number | null;
  hi: number | null;
  integerBins: boolean;
  quantiles: { p5: number; p50: number; p95: number } | null;
  /** Rows (estimated from the sketch) past 3×IQR from the quartiles, left off the ridge. */
  beyondLow: number;
  beyondHigh: number;
  min: string | null;
  max: string | null;
  mean: number | null;
  stddev: number | null;
  skewness: number | null;
  driftReason: string | null;
  flags: TerrainFlag[];
}

export const SKETCH_BINS = 40;
const MAX_CATEGORY_BINS = 24;
const MAX_INTEGER_SPAN = 60;
const FENCE_IQR = 3;

const num = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);
const str = (v: unknown): string | null => (v == null ? null : String(v));

export function pctText(share: number): string {
  const p = share * 100;
  return (p >= 10 || p === 0 ? p.toFixed(0) : p.toFixed(1)) + "%";
}

const numberFmt = new Intl.NumberFormat(undefined, { maximumSignificantDigits: 4 });

/** A raw value of the column as text: epoch seconds for a temporal column, a number otherwise. */
export function formatValue(family: TerrainFamily, v: number, spanSeconds?: number): string {
  if (family === "temporal") {
    const iso = new Date(v * 1000).toISOString();
    return spanSeconds != null && spanSeconds < 3 * 86400
      ? iso.slice(0, 16).replace("T", " ")
      : iso.slice(0, 10);
  }
  return numberFmt.format(v);
}

/** The sketch's cumulative share at ``x`` — the interpolation the backend's histogram uses. */
export function sketchCdf(sketch: number[], x: number): number {
  const n = sketch.length;
  if (x <= sketch[0]) return 0;
  if (x >= sketch[n - 1]) return 1;
  let hi = 0;
  while (hi < n && sketch[hi] <= x) hi++;
  const lo = hi - 1;
  const span = sketch[hi] - sketch[lo];
  return (lo + (x - sketch[lo]) / span) / (n - 1);
}

function groupBy(rows: Row[] | undefined): Map<string, Row[]> {
  const m = new Map<string, Row[]>();
  for (const r of rows ?? []) {
    const k = String(r.column_name);
    const list = m.get(k);
    if (list) list.push(r);
    else m.set(k, [r]);
  }
  return m;
}

function sketchBins(
  family: TerrainFamily,
  sketch: number[],
  nonNull: number,
  integerOnly: boolean,
) {
  const p25 = sketch[25];
  const p75 = sketch[75];
  const iqr = p75 - p25;
  let lo = sketch[0];
  let hi = sketch[sketch.length - 1];
  if (iqr > 0) {
    lo = Math.max(lo, p25 - FENCE_IQR * iqr);
    hi = Math.min(hi, p75 + FENCE_IQR * iqr);
  }
  const span = hi - lo;
  if (span === 0) {
    return {
      lo,
      hi,
      integerBins: false,
      smooth: false,
      bins: [{ label: formatValue(family, lo), count: nonNull }],
      beyondLow: 0,
      beyondHigh: 0,
    };
  }
  const integerBins =
    integerOnly && family === "numeric" && Math.round(hi) - Math.round(lo) <= MAX_INTEGER_SPAN;
  const bins: TerrainBin[] = [];
  if (integerBins) {
    lo = Math.round(lo);
    hi = Math.round(hi);
    for (let k = lo; k <= hi; k++) {
      const share =
        sketchCdf(sketch, k + 0.5) -
        (k === lo ? sketchCdf(sketch, lo - 0.5) : sketchCdf(sketch, k - 0.5));
      bins.push({ label: formatValue(family, k), count: share * nonNull });
    }
  } else {
    const w = span / SKETCH_BINS;
    for (let j = 0; j < SKETCH_BINS; j++) {
      const a = lo + j * w;
      const b = j === SKETCH_BINS - 1 ? hi : lo + (j + 1) * w;
      const share = sketchCdf(sketch, b) - sketchCdf(sketch, a);
      bins.push({
        label: `${formatValue(family, a, span)} – ${formatValue(family, b, span)}`,
        count: share * nonNull,
      });
    }
  }
  const lowEdge = integerBins ? lo - 0.5 : lo;
  const highEdge = integerBins ? hi + 0.5 : hi;
  return {
    lo,
    hi,
    integerBins,
    smooth: !integerBins,
    bins,
    beyondLow: sketchCdf(sketch, lowEdge) * nonNull,
    beyondHigh: (1 - sketchCdf(sketch, highEdge)) * nonNull,
  };
}

function capCategories(bins: TerrainBin[], ordered: boolean): TerrainBin[] {
  if (ordered || bins.length <= MAX_CATEGORY_BINS) return bins;
  const keep = bins.slice(0, MAX_CATEGORY_BINS - 1);
  const rest = bins.slice(MAX_CATEGORY_BINS - 1);
  return [...keep, { label: `+${rest.length}`, count: rest.reduce((s, b) => s + b.count, 0) }];
}

export function countPeaks(c: number[], smooth: boolean): number {
  const s = smooth ? c.map((v, i) => ((c[i - 1] ?? v) + 2 * v + (c[i + 1] ?? v)) / 4) : c.slice();
  const mx = Math.max(...s);
  const peaks: number[] = [];
  for (let i = 0; i < s.length; i++) {
    const l = i > 0 ? s[i - 1] : -1;
    const r = i < s.length - 1 ? s[i + 1] : -1;
    if (s[i] >= 0.25 * mx && s[i] > l && s[i] >= r) peaks.push(i);
  }
  const kept: number[] = [];
  for (const i of peaks) {
    if (kept.length === 0) {
      kept.push(i);
      continue;
    }
    const j = kept[kept.length - 1];
    let valley = Infinity;
    for (let k = j; k <= i; k++) valley = Math.min(valley, s[k]);
    if (valley < 0.6 * Math.min(s[i], s[j])) kept.push(i);
    else if (s[i] > s[j]) kept[kept.length - 1] = i;
  }
  return kept.length;
}

function flagsOf(c: Omit<TerrainColumn, "flags">): TerrainFlag[] {
  const f: TerrainFlag[] = [];
  if (c.driftReason != null)
    f.push({ kind: "warn", key: "flagDrifting", params: { reason: c.driftReason } });
  if (c.nullShare != null) {
    if (c.nullShare >= 0.2)
      f.push({ kind: "warn", key: "flagNulls", params: { pct: pctText(c.nullShare) } });
    else if (c.nullShare >= 0.05)
      f.push({ kind: "info", key: "flagNulls", params: { pct: pctText(c.nullShare) } });
  }
  if (c.restricted) {
    f.push({ kind: "info", key: "flagRestricted" });
    return f;
  }
  if (c.rows == null || c.nullCount == null) return f;
  const nonNull = c.rows - c.nullCount;
  if (c.distinct === 1) f.push({ kind: "warn", key: "flagConstant" });
  if (!c.ordered && c.bins.length > 1 && nonNull > 0) {
    const share = c.bins[0].count / nonNull;
    if (share >= 0.85)
      f.push({
        kind: "warn",
        key: "flagDominant",
        params: { value: c.bins[0].label, pct: pctText(share) },
      });
  }
  if (c.skewness != null && (c.family === "numeric" || c.family === "temporal")) {
    if (c.skewness > 1) f.push({ kind: "info", key: "flagRightSkew" });
    else if (c.skewness < -1) f.push({ kind: "info", key: "flagLeftSkew" });
  }
  if (c.ordered && c.bins.length >= 3) {
    const counts = c.bins.map((b) => b.count);
    const peaks = countPeaks(counts, c.smooth);
    if (peaks >= 3) f.push({ kind: "info", key: "flagMultimodal" });
    else if (peaks === 2) f.push({ kind: "info", key: "flagBimodal" });
    let best = -1;
    let bestShare = 0;
    counts.forEach((n, j) => {
      const nb = Math.max(j > 0 ? counts[j - 1] : 0, j < counts.length - 1 ? counts[j + 1] : 0);
      const share = n / nonNull;
      if (counts.length >= 8 && share >= 0.03 && n >= 3 * nb && share > bestShare) {
        best = j;
        bestShare = share;
      }
    });
    if (best >= 0) {
      const label =
        c.integerBins || !c.smooth ? c.bins[best].label : c.bins[best].label.split(" – ")[0];
      f.push({ kind: "warn", key: "flagSpike", params: { value: label, pct: pctText(bestShare) } });
    }
  }
  const beyond = c.beyondLow + c.beyondHigh;
  if (beyond >= 0.5 && nonNull > 0) {
    f.push({
      kind: beyond / nonNull >= 0.002 ? "warn" : "info",
      key: "flagBeyond",
      params: { rows: Math.round(beyond).toLocaleString() },
    });
  }
  if (c.binning === "top" && c.distinct != null && c.distinct > c.bins.length) {
    f.push({ kind: "info", key: "flagDistinct", params: { count: c.distinct.toLocaleString() } });
  }
  return f;
}

/** One ridge per profiled column, in the run's column order. */
export function buildTerrain(results: ProfileRunResults): TerrainColumn[] {
  const quantiles = groupBy(results.quantiles);
  const values = groupBy(results.top_values);
  const drifting = new Map<string, string>();
  for (const d of results.drift ?? []) {
    if (d.drifting === true && d.column_name != null && !drifting.has(String(d.column_name))) {
      drifting.set(String(d.column_name), String(d.drift_reason));
    }
  }
  return (results.columns ?? []).map((row) => {
    const name = String(row.column_name);
    const family = String(row.family) as TerrainFamily;
    const rows = num(row.row_count);
    const nullCount = num(row.null_count);
    const nullShare = num(row.null_share);
    const nonNull = rows == null || nullCount == null ? null : rows - nullCount;
    const sketch = (quantiles.get(name) ?? [])
      .filter((q) => q.measure === "value")
      .sort((a, b) => Number(a.q) - Number(b.q))
      .map((q) => Number(q.value));
    const vals = (values.get(name) ?? []).filter((v) => v.value != null);
    const frequency = vals.filter((v) => v.kind === "frequency");
    const top = vals.filter((v) => v.kind === "top");
    const ordered = family === "numeric" || family === "temporal";

    const base = {
      name,
      family,
      dataType: String(row.data_type),
      rows,
      nullCount,
      nullShare,
      distinct: num(row.distinct_count),
      min: str(row.min_value),
      max: str(row.max_value),
      mean: num(row.mean),
      stddev: num(row.stddev),
      skewness: num(row.skewness),
      driftReason: drifting.get(name) ?? null,
      lo: null as number | null,
      hi: null as number | null,
      integerBins: false,
      quantiles: null as TerrainColumn["quantiles"],
      beyondLow: 0,
      beyondHigh: 0,
    };
    let shape: Pick<TerrainColumn, "binning" | "ordered" | "smooth" | "bins" | "restricted">;
    if (frequency.length > 0) {
      const bins = frequency.map((v) => ({ label: String(v.value), count: Number(v.row_count) }));
      if (ordered) {
        bins.sort((a, b) =>
          family === "numeric" ? Number(a.label) - Number(b.label) : a.label.localeCompare(b.label),
        );
      } else bins.sort((a, b) => b.count - a.count);
      shape = {
        binning: "frequency",
        ordered,
        smooth: false,
        bins: capCategories(bins, ordered),
        restricted: false,
      };
    } else if (sketch.length === 101 && nonNull != null) {
      const s = sketchBins(family, sketch, nonNull, row.integer_only === true);
      Object.assign(base, {
        lo: s.lo,
        hi: s.hi,
        integerBins: s.integerBins,
        beyondLow: s.beyondLow,
        beyondHigh: s.beyondHigh,
        quantiles: { p5: sketch[5], p50: sketch[50], p95: sketch[95] },
      });
      shape = {
        binning: "sketch",
        ordered: true,
        smooth: s.smooth,
        bins: s.bins,
        restricted: false,
      };
    } else if (top.length > 0 && nonNull != null) {
      const bins = top
        .map((v) => ({ label: String(v.value), count: Number(v.row_count) }))
        .sort((a, b) => b.count - a.count);
      const rest = nonNull - bins.reduce((s, b) => s + b.count, 0);
      if (rest > 0) bins.push({ label: "…", count: rest });
      shape = { binning: "top", ordered: false, smooth: false, bins, restricted: false };
    } else {
      shape = { binning: "none", ordered: false, smooth: false, bins: [], restricted: true };
    }
    const col = { ...base, ...shape, maxCount: Math.max(1, ...shape.bins.map((b) => b.count)) };
    return { ...col, flags: flagsOf(col) };
  });
}

/** Where a raw value sits along a sketch ridge, 0 (front, low) .. 1 (back, high). */
export function tOfValue(c: TerrainColumn, v: number): number {
  if (c.lo == null || c.hi == null) return 0;
  if (c.integerBins) return Math.min(1, Math.max(0, (v - c.lo + 0.5) / c.bins.length));
  if (c.hi === c.lo) return 0.5;
  return Math.min(1, Math.max(0, (v - c.lo) / (c.hi - c.lo)));
}

export type HeightMode = "peak" | "share";

/** A bin's height, 0..1: against the column's own peak, or as a share of rows against the largest
 * share any column reaches. */
export function heightShare(
  c: TerrainColumn,
  count: number,
  mode: HeightMode,
  log: boolean,
  maxShare: number,
): number {
  if (mode === "peak") return log ? Math.log1p(count) / Math.log1p(c.maxCount) : count / c.maxCount;
  const s = c.rows ? count / c.rows : 0;
  return log ? Math.log1p(s * 1000) / Math.log1p(maxShare * 1000) : s / maxShare;
}

export function maxShareOf(cols: TerrainColumn[]): number {
  let m = 0;
  for (const c of cols) if (c.rows && c.bins.length) m = Math.max(m, c.maxCount / c.rows);
  return m || 1;
}

export type TerrainOrder = "schema" | "nulls" | "skew" | "distinct" | "drift";

export function orderColumns(cols: TerrainColumn[], order: TerrainOrder): string[] {
  const key: Record<Exclude<TerrainOrder, "schema">, (c: TerrainColumn) => number> = {
    nulls: (c) => -(c.nullShare ?? -1),
    skew: (c) => (c.skewness == null ? Infinity : -Math.abs(c.skewness)),
    distinct: (c) => -(c.distinct ?? -1),
    drift: (c) => (c.driftReason != null ? 0 : 1),
  };
  const list = cols.map((c, i) => ({ c, i }));
  if (order !== "schema") list.sort((a, b) => key[order](a.c) - key[order](b.c) || a.i - b.i);
  return list.map((x) => x.c.name);
}
