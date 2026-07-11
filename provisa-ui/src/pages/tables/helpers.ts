// Copyright (c) 2026 Kenneth Stott
// Canary: 7c2e5d83-4a1f-4b6e-9d0c-2f8a3e7b1c94
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

export function normalizeDomain(domain: string): string {
  return domain.replace(/[^a-zA-Z0-9]/g, "_").replace(/^_+|_+$/g, "");
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
