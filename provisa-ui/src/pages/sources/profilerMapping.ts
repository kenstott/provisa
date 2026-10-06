// Copyright (c) 2026 Kenneth Stott
// Canary: d81a3c64-5e29-4b07-9f16-b2e8c4a7d930
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1934: a Data Profiler source's mapping (provisa/profiler/source.py profiler_settings) and the
// form fields it is edited through.

/** The low-cardinality threshold a new profiler starts from (REQ-1934): a column with no more
 * distinct values has its full value-frequency table recorded. The form is where it is defaulted. */
export const DEFAULT_LOW_CARDINALITY_MAX = "100";

/** The profiler's mapping as the source stores it, from the form's fields. */
export function profilerMappingJson(authFields: Record<string, string>): string {
  const sample = (authFields.sample_above_rows ?? "").trim();
  return JSON.stringify({
    cron: (authFields.cron ?? "").trim(),
    ...(sample === "" ? {} : { sample_above_rows: Number(sample) }),
    low_cardinality_max: Number((authFields.low_cardinality_max ?? "").trim()),
  });
}

/** The form's fields from a stored profiler mapping. */
export function profilerFieldsFromMapping(mappingJson: string): Record<string, string> {
  const m = JSON.parse(mappingJson) as {
    cron?: string;
    sample_above_rows?: number | null;
    low_cardinality_max: number;
  };
  return {
    cron: m.cron ?? "",
    sample_above_rows: m.sample_above_rows == null ? "" : String(m.sample_above_rows),
    low_cardinality_max: String(m.low_cardinality_max),
  };
}
