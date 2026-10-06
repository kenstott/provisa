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

/** The run defaults a new profiler starts from (REQ-1934), each stored in its mapping. The form is
 * where they are defaulted: the low-cardinality threshold (a column with no more distinct values has
 * its full value-frequency table recorded) and the drift window, season and thresholds. */
export const PROFILER_DEFAULTS: Record<string, string> = {
  low_cardinality_max: "100",
  drift_window: "7",
  drift_season: "none",
  drift_distance: "3",
  drift_slope: "3",
  drift_ks: "0.2",
  drift_psi: "0.25",
};

/** The drift seasons a profiler's baseline can follow (provisa/profiler/source.py DRIFT_SEASONS). */
export const DRIFT_SEASONS = ["none", "daily", "weekly", "monthly"] as const;

// The run defaults stored as text; every other one is a number.
const TEXT_SETTINGS = new Set(["drift_season"]);

/** The profiler's mapping as the source stores it, from the form's fields. */
export function profilerMappingJson(authFields: Record<string, string>): string {
  const sample = (authFields.sample_above_cells ?? "").trim();
  const defaults = Object.fromEntries(
    Object.keys(PROFILER_DEFAULTS).map((k) => {
      const v = (authFields[k] ?? "").trim();
      return [k, TEXT_SETTINGS.has(k) ? v : Number(v)];
    }),
  );
  return JSON.stringify({
    cron: (authFields.cron ?? "").trim(),
    ...(sample === "" ? {} : { sample_above_cells: Number(sample) }),
    ...defaults,
  });
}

/** The form's fields from a stored profiler mapping. */
export function profilerFieldsFromMapping(mappingJson: string): Record<string, string> {
  const m = JSON.parse(mappingJson) as Record<string, string | number | null | undefined>;
  return {
    cron: m.cron == null ? "" : String(m.cron),
    sample_above_cells: m.sample_above_cells == null ? "" : String(m.sample_above_cells),
    ...Object.fromEntries(Object.keys(PROFILER_DEFAULTS).map((k) => [k, String(m[k])])),
  };
}
