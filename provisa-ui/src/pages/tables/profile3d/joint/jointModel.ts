// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2124: the joint cloud's model — a column and its parents in the dependency network, their
// rows counted in a grid of deciles (numbers) and categories (everything else), each cell with its
// lift over what the columns' own distributions would give if they were independent.

import type { ProfileRunResults } from "../../../../api/profiler";

type Row = Record<string, unknown>;

export const MAX_CATEGORIES = 12;

export interface JointAxis {
  column: string;
  kind: "decile" | "category" | "single";
  /** A slot's value; null is the column's null, ``other`` the categories past the first ones. */
  slots: { value: string | null; other: boolean }[];
}

export interface JointCell {
  x: number;
  y: number;
  z: number;
  count: number;
  /** Observed rows over the rows independence would put here. */
  lift: number;
}

export interface JointModel {
  target: string;
  /** x: the first parent; y: the column; z: the second parent (one slot when there is none). */
  axes: [JointAxis, JointAxis, JointAxis];
  cells: JointCell[];
  total: number;
  maxCount: number;
  mutualInformation: number | null;
  uncertainty: number | null;
}

/** The columns a joint cloud can be drawn for, in the order the run lists them. */
export function jointTargets(results: ProfileRunResults): string[] {
  const out: string[] = [];
  for (const r of results.joint_counts ?? []) {
    const c = String(r.column_name);
    if (!out.includes(c)) out.push(c);
  }
  return out;
}

const slotKey = (bucket: unknown, value: unknown): string | null =>
  bucket != null ? `#${Number(bucket)}` : value != null ? `=${String(value)}` : null;

function axisOf(column: string, rows: Row[], bucketField: string, valueField: string) {
  const decile = rows.some((r) => r[bucketField] != null);
  const totals = new Map<string | null, number>();
  for (const r of rows) {
    const k = slotKey(r[bucketField], r[valueField]);
    totals.set(k, (totals.get(k) ?? 0) + Number(r.row_count));
  }
  let keys: (string | null)[];
  if (decile) {
    keys = Array.from({ length: 10 }, (_, i) => `#${i}`);
    if (totals.has(null)) keys.push(null);
  } else {
    keys = [...totals.keys()].sort((a, b) => (totals.get(b) as number) - (totals.get(a) as number));
  }
  const kept = keys.length > MAX_CATEGORIES ? keys.slice(0, MAX_CATEGORIES - 1) : keys;
  const index = new Map<string | null, number>(kept.map((k, i) => [k, i]));
  const otherIndex = keys.length > MAX_CATEGORIES ? kept.length : -1;
  const slots: JointAxis["slots"] = kept.map((k) => ({
    value: k == null ? null : k.slice(1),
    other: false,
  }));
  if (otherIndex >= 0) slots.push({ value: String(keys.length - kept.length), other: true });
  const axis: JointAxis = { column, kind: decile ? "decile" : "category", slots };
  const slotOf = (r: Row) => {
    const i = index.get(slotKey(r[bucketField], r[valueField]));
    return i ?? otherIndex;
  };
  return { axis, slotOf };
}

export function buildJoint(results: ProfileRunResults, target: string): JointModel | null {
  const rows = (results.joint_counts ?? []).filter((r) => String(r.column_name) === target);
  if (rows.length === 0) return null;
  const p1 = String(rows[0].parent_1);
  const p2 = rows[0].parent_2 == null ? null : String(rows[0].parent_2);
  const t = axisOf(target, rows, "target_bucket", "target_value");
  const a = axisOf(p1, rows, "parent_1_bucket", "parent_1_value");
  const b =
    p2 == null
      ? {
          axis: { column: "", kind: "single" as const, slots: [{ value: null, other: false }] },
          slotOf: () => 0,
        }
      : axisOf(p2, rows, "parent_2_bucket", "parent_2_value");
  const grid = new Map<string, JointCell>();
  const mx = new Map<number, number>();
  const my = new Map<number, number>();
  const mz = new Map<number, number>();
  let total = 0;
  for (const r of rows) {
    const x = a.slotOf(r);
    const y = t.slotOf(r);
    const z = b.slotOf(r);
    const n = Number(r.row_count);
    const key = `${x}|${y}|${z}`;
    const cell = grid.get(key);
    if (cell) cell.count += n;
    else grid.set(key, { x, y, z, count: n, lift: 0 });
    mx.set(x, (mx.get(x) ?? 0) + n);
    my.set(y, (my.get(y) ?? 0) + n);
    mz.set(z, (mz.get(z) ?? 0) + n);
    total += n;
  }
  const cells = [...grid.values()];
  for (const c of cells) {
    const expected =
      ((mx.get(c.x) as number) * (my.get(c.y) as number) * (mz.get(c.z) as number)) /
      (total * total);
    c.lift = c.count / expected;
  }
  const dep = (results.dependencies ?? []).find(
    (r) => String(r.column_name) === target && r.in_network === true,
  );
  return {
    target,
    axes: [a.axis, t.axis, b.axis],
    cells,
    total,
    maxCount: Math.max(...cells.map((c) => c.count)),
    mutualInformation:
      dep && typeof dep.mutual_information === "number" ? dep.mutual_information : null,
    uncertainty: dep && typeof dep.uncertainty === "number" ? dep.uncertainty : null,
  };
}
