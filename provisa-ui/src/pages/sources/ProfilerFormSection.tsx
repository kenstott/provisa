// Copyright (c) 2026 Kenneth Stott
// Canary: c62f8e15-0b47-4a39-9d81-e3a5b7f0c294
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { NumberInput, TextInput } from "@mantine/core";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";
import { DEFAULT_LOW_CARDINALITY_MAX } from "./profilerMapping";

// REQ-1934: a Data Profiler source holds a name, a schedule and its run defaults, nothing else. The
// schedule is a cron expression, the recurrence scheduled triggers use; the run defaults are the row
// count above which a run profiles a sample of about that many rows (empty: every row), and the
// low-cardinality threshold, defaulted here for a new profiler. All travel in the source's mapping
// (provisa/profiler/source.py profiler_settings).
export function ProfilerFormSection({
  authFields,
  setAuthFields,
}: {
  authFields: Record<string, string>;
  setAuthFields: (f: Record<string, string>) => void;
}) {
  const { t } = useTranslation();
  useEffect(() => {
    if (authFields.low_cardinality_max === undefined)
      setAuthFields({ ...authFields, low_cardinality_max: DEFAULT_LOW_CARDINALITY_MAX });
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
        value={authFields.sample_above_rows ?? ""}
        onChange={(v) =>
          setAuthFields({ ...authFields, sample_above_rows: v === "" ? "" : String(v) })
        }
        style={{ gridColumn: "1 / -1" }}
        data-testid="profiler-sample-input"
      />
      <NumberInput
        label={t("profilerFormSection.lowCardinalityLabel")}
        description={t("profilerFormSection.lowCardinalityDescription")}
        required
        min={1}
        allowDecimal={false}
        value={authFields.low_cardinality_max ?? ""}
        onChange={(v) =>
          setAuthFields({ ...authFields, low_cardinality_max: v === "" ? "" : String(v) })
        }
        style={{ gridColumn: "1 / -1" }}
        data-testid="profiler-low-cardinality-input"
      />
    </>
  );
}
