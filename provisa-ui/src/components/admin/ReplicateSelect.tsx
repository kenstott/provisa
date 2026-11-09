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
import { useHelpNaming } from "../form/useHelpNaming";
import { ReadRow } from "../form/FormLayout";
import { useFormRead } from "../form/formRead";
import {
  REPLICATE_ALWAYS,
  parseReplicateOption,
  replicateContradictsLoadProtection,
  replicateLabel,
  replicateOptionValue,
  replicateValues,
} from "./replicate";

/** The drop-down's value for load protection: no Replicate value of its own. */
const SCHEDULED = "scheduled";

export function ReplicateSelect({
  value,
  onChange,
  scope,
  loadProtected,
  onScheduledChange,
  testId,
  disabled,
}: {
  /** The stored setting: null = Default. */
  value: number | null;
  onChange: (value: number | null) => void;
  /** Which form this is in: a table's own value, or the source value its tables inherit. */
  scope: "table" | "source";
  /**
   * Whether load protection is this table's or source's OWN setting (Never then contradicts it).
   * A table that only inherits it from its source shows Default.
   */
  loadProtected: boolean;
  /**
   * Given, the drop-down also offers "Always, source read only on schedule" (REQ-1141), which is
   * load protection: choosing it reports Always with load protection on, and any other choice
   * reports that choice with load protection off. A value stored with load protection shows as
   * that choice whatever its Replicate value is. A table's Default inherits both from its source
   * and reports load protection as null; a source has no inheritance and never reports null.
   */
  onScheduledChange?: (value: number | null, loadProtected: boolean | null) => void;
  testId: string;
  /** Shown, not changeable: a value its connector fixes. */
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const read = useFormRead();
  const label = (replicate: number | null) => replicateLabel(t, replicate);
  // One sentence for the setting and its Default, which differ by scope, then each other option
  // once.
  const help = [
    t(scope === "table" ? "replicateSelect.helpTable" : "replicateSelect.helpSource"),
    t("replicateSelect.helpOptions"),
    ...(onScheduledChange ? [t("replicateSelect.helpScheduled")] : []),
  ].join(" ");
  // Named by the label's text alone; the help is its description.
  const { textId, naming } = useHelpNaming(help);
  if (read) {
    return (
      <ReadRow
        label={t("replicateSelect.label")}
        value={onScheduledChange && loadProtected ? t("replicateSelect.scheduled") : label(value)}
      />
    );
  }
  return (
    <Select
      {...naming}
      label={<FieldLabel text={t("replicateSelect.label")} help={help} textId={textId} />}
      data={[
        ...replicateValues(value).map((replicate) => ({
          value: replicateOptionValue(replicate),
          label: label(replicate),
        })),
        ...(onScheduledChange ? [{ value: SCHEDULED, label: t("replicateSelect.scheduled") }] : []),
      ]}
      value={onScheduledChange && loadProtected ? SCHEDULED : replicateOptionValue(value)}
      onChange={(option) => {
        if (option === null) return;
        if (!onScheduledChange) onChange(parseReplicateOption(option));
        else if (option === SCHEDULED) onScheduledChange(REPLICATE_ALWAYS, true);
        else {
          const replicate = parseReplicateOption(option);
          onScheduledChange(replicate, scope === "table" && replicate === null ? null : false);
        }
      }}
      error={
        // With the scheduled choice in the list the two cannot be set against each other here.
        !onScheduledChange && replicateContradictsLoadProtection(value, loadProtected)
          ? t("replicateSelect.neverWithLoadProtection")
          : undefined
      }
      comboboxProps={{ withinPortal: true }}
      allowDeselect={false}
      disabled={disabled}
      data-testid={testId}
    />
  );
}
