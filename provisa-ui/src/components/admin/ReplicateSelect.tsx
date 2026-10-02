// Copyright (c) 2026 Kenneth Stott
// Canary: 34252441-15e1-4f42-acb9-2a99ac98baad
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The "Replicate" drop-down (REQ-826), shared by the table form and the source's Load Management
// panel: Default, Never, the Hot thresholds and Always. Only Always is a guarantee; the others
// are best effort, which the (i) tooltip says.

import { useTranslation } from "react-i18next";
import { Select } from "@mantine/core";
import { FieldLabel } from "../../pages/tables/FieldLabel";
import {
  REPLICATE_ALWAYS,
  REPLICATE_NEVER,
  parseReplicateOption,
  replicateContradictsLoadProtection,
  replicateOptionValue,
  replicateValues,
} from "./replicate";

export function ReplicateSelect({
  value,
  onChange,
  scope,
  loadProtected,
  testId,
}: {
  /** The stored setting: null = Default. */
  value: number | null;
  onChange: (value: number | null) => void;
  /** Which form this is in: a table's own value, or the source value its tables inherit. */
  scope: "table" | "source";
  /** Whether load protection is on for this table or source (Never then contradicts it). */
  loadProtected: boolean;
  testId: string;
}) {
  const { t } = useTranslation();
  const label = (replicate: number | null): string => {
    if (replicate === null) return t("replicateSelect.default");
    if (replicate === REPLICATE_NEVER) return t("replicateSelect.never");
    if (replicate === REPLICATE_ALWAYS) return t("replicateSelect.always");
    return t("replicateSelect.hot", { threshold: replicate });
  };
  return (
    <Select
      label={
        <FieldLabel
          text={t("replicateSelect.label")}
          help={t(scope === "table" ? "replicateSelect.helpTable" : "replicateSelect.helpSource")}
        />
      }
      data={replicateValues(value).map((replicate) => ({
        value: replicateOptionValue(replicate),
        label: label(replicate),
      }))}
      value={replicateOptionValue(value)}
      onChange={(option) => {
        if (option !== null) onChange(parseReplicateOption(option));
      }}
      error={
        replicateContradictsLoadProtection(value, loadProtected)
          ? t("replicateSelect.neverWithLoadProtection")
          : undefined
      }
      comboboxProps={{ withinPortal: true }}
      allowDeselect={false}
      data-testid={testId}
    />
  );
}
