// Copyright (c) 2026 Kenneth Stott
// Canary: c62f8e15-0b47-4a39-9d81-e3a5b7f0c294
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { NumberInput, Select, TextInput } from "@mantine/core";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";
import { DRIFT_SEASONS, PROFILER_DEFAULTS } from "./profilerMapping";

// A run default edited as a number: its mapping key, its i18n stem, whether it is whole, its minimum.
const NUMBER_FIELDS: { key: string; stem: string; whole: boolean; min: number }[] = [
  { key: "low_cardinality_max", stem: "lowCardinality", whole: true, min: 1 },
  { key: "drift_window", stem: "driftWindow", whole: true, min: 1 },
  { key: "drift_distance", stem: "driftDistance", whole: false, min: 0 },
  { key: "drift_slope", stem: "driftSlope", whole: false, min: 0 },
  { key: "drift_ks", stem: "driftKs", whole: false, min: 0 },
  { key: "drift_psi", stem: "driftPsi", whole: false, min: 0 },
  { key: "correlation_max_columns", stem: "correlationMax", whole: true, min: 1 },
  { key: "joint_max_distinct", stem: "jointMaxDistinct", whole: true, min: 1 },
];

// REQ-1934: a Data Profiler source holds a name, a schedule and its run defaults, nothing else. The
// schedule is a cron expression, the recurrence scheduled triggers use; the run defaults are the cell
// budget above which a run profiles a sample (empty: every row), the low-cardinality threshold, the
// drift window, season and thresholds, and the dependence bounds, each defaulted here for a new
// profiler. All travel in the
// source's mapping (provisa/profiler/source.py profiler_settings).
export function ProfilerFormSection({
  authFields,
  setAuthFields,
}: {
  authFields: Record<string, string>;
  setAuthFields: (f: Record<string, string>) => void;
}) {
  const { t } = useTranslation();
  useEffect(() => {
    const missing = Object.entries(PROFILER_DEFAULTS).filter(([k]) => authFields[k] === undefined);
    if (missing.length > 0) setAuthFields({ ...authFields, ...Object.fromEntries(missing) });
  }, [authFields, setAuthFields]);
  return (
    <>
      <TextInput
        label={t("profilerFormSection.cronLabel")}
        description={t("profilerFormSection.cronDescription")}
        placeholder="0 3 * * *"
        required
        value={authFields.cron ?? ""}
        onChange={(e) => setAuthFields({ ...authFields, cron: e.currentTarget.value })}
        style={{ gridColumn: "1 / -1" }}
        data-testid="profiler-cron-input"
      />
      <NumberInput
        label={t("profilerFormSection.sampleLabel")}
        description={t("profilerFormSection.sampleDescription")}
        placeholder={t("profilerFormSection.samplePlaceholder")}
        min={1}
        allowDecimal={false}
        value={authFields.sample_above_cells ?? ""}
        onChange={(v) =>
          setAuthFields({ ...authFields, sample_above_cells: v === "" ? "" : String(v) })
        }
        style={{ gridColumn: "1 / -1" }}
        data-testid="profiler-sample-input"
      />
      <Select
        label={t("profilerFormSection.driftSeasonLabel")}
        description={t("profilerFormSection.driftSeasonDescription")}
        required
        allowDeselect={false}
        data={DRIFT_SEASONS.map((s) => ({
          value: s,
          label: t(`profilerFormSection.driftSeason_${s}`),
        }))}
        value={authFields.drift_season ?? null}
        onChange={(v) => v != null && setAuthFields({ ...authFields, drift_season: v })}
        style={{ gridColumn: "1 / -1" }}
        data-testid="profiler-drift_season-input"
      />
      {NUMBER_FIELDS.map((f) => (
        <NumberInput
          key={f.key}
          label={t(`profilerFormSection.${f.stem}Label`)}
          description={t(`profilerFormSection.${f.stem}Description`)}
          required
          min={f.min}
          allowDecimal={!f.whole}
          value={authFields[f.key] ?? ""}
          onChange={(v) => setAuthFields({ ...authFields, [f.key]: v === "" ? "" : String(v) })}
          style={{ gridColumn: "1 / -1" }}
          data-testid={
            f.key === "low_cardinality_max"
              ? "profiler-low-cardinality-input"
              : `profiler-${f.key}-input`
          }
        />
      ))}
    </>
  );
}
