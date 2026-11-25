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

import { descriptorFields, fieldChoices, mappingKey } from "./descriptor";

// REQ-2114: the keys, the run defaults and the drift seasons are the descriptor's
// (provisa/api/admin/source_types.py, from provisa/profiler/source.py).
const PROFILER = "data_profiler";
const CRON = mappingKey(PROFILER, "cron");
const SAMPLE_ABOVE = mappingKey(PROFILER, "sample_above_cells");
const DEFAULTED = descriptorFields(PROFILER, "mappingJson").filter((f) => f.default !== undefined);

/** The run defaults a new profiler starts from (REQ-1934), each stored in its mapping. The form is
 * where they are defaulted: the low-cardinality threshold (a column with no more distinct values has
 * its full value-frequency table recorded) and the drift window, season and thresholds. */
export const PROFILER_DEFAULTS: Record<string, string> = Object.fromEntries(
  DEFAULTED.map((f) => [f.name, String(f.default)]),
);

/** The drift seasons a profiler's baseline can follow (provisa/profiler/source.py DRIFT_SEASONS). */
export const DRIFT_SEASONS: readonly string[] = fieldChoices(
  PROFILER,
  "mappingJson",
  "drift_season",
);

// The run defaults stored as text; every other one is a number.
const TEXT_SETTINGS = new Set(
  DEFAULTED.filter((f) => typeof f.default === "string").map((f) => f.name),
);

/** The profiler's mapping as the source stores it, from the form's fields. */
export function profilerMappingJson(authFields: Record<string, string>): string {
  const sample = (authFields[SAMPLE_ABOVE] ?? "").trim();
  const defaults = Object.fromEntries(
    Object.keys(PROFILER_DEFAULTS).map((k) => {
      const v = (authFields[k] ?? "").trim();
      return [k, TEXT_SETTINGS.has(k) ? v : Number(v)];
    }),
  );
  return JSON.stringify({
    [CRON]: (authFields[CRON] ?? "").trim(),
    ...(sample === "" ? {} : { [SAMPLE_ABOVE]: Number(sample) }),
    ...defaults,
  });
}

/** The form's fields from a stored profiler mapping. */
export function profilerFieldsFromMapping(mappingJson: string): Record<string, string> {
  const m = JSON.parse(mappingJson) as Record<string, string | number | null | undefined>;
  return {
    [CRON]: m[CRON] == null ? "" : String(m[CRON]),
    [SAMPLE_ABOVE]: m[SAMPLE_ABOVE] == null ? "" : String(m[SAMPLE_ABOVE]),
    ...Object.fromEntries(Object.keys(PROFILER_DEFAULTS).map((k) => [k, String(m[k])])),
  };
}
