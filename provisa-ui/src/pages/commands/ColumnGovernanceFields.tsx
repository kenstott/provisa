// Copyright (c) 2026 Kenneth Stott
// Canary: 2b8d4f7a-9c1e-4e6b-a5d3-7f0e9c2b4a61
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1679: per-column governance of an action's response — the shape a table column carries
// (visible_to, unmasked_to, mask), on an output-contract column or an inline return field. The
// keys are the wire keys: the column dict is stored as posted and read back by the server as is.

import React from "react";
import { useTranslation } from "react-i18next";
import { Group, Select, TextInput } from "@mantine/core";

export interface GovernedColumn {
  name: string;
  type: string;
  visible_to?: string[] | null;
  unmasked_to?: string[];
  mask_type?: string | null;
  mask_pattern?: string | null;
  mask_replace?: string | null;
  mask_value?: string | number | null;
  mask_precision?: string | null;
}

const splitRoles = (s: string): string[] =>
  s
    .split(",")
    .map((r) => r.trim())
    .filter(Boolean);

interface Props {
  col: GovernedColumn;
  onChange: (patch: Partial<GovernedColumn>) => void;
  testId: string;
}

export function ColumnGovernanceFields({ col, onChange, testId }: Props): React.ReactElement {
  const { t } = useTranslation();
  return (
    <Group gap="xs" mb="xs" align="flex-end" wrap="wrap" pl="md" data-testid={testId}>
      <TextInput
        label={t("commandFormFields.colVisibleTo")}
        placeholder={t("commandFormFields.colVisibleToPlaceholder")}
        value={(col.visible_to ?? []).join(", ")}
        onChange={(e) => {
          const roles = splitRoles(e.currentTarget.value);
          onChange({ visible_to: roles.length ? roles : null });
        }}
        style={{ flex: 1, minWidth: 140 }}
        data-testid={`${testId}-visible-to`}
      />
      <Select
        label={t("commandFormFields.colMask")}
        data={[
          { value: "", label: t("commandFormFields.maskNone") },
          { value: "regex", label: t("commandFormFields.maskRegex") },
          { value: "constant", label: t("commandFormFields.maskConstant") },
          { value: "truncate", label: t("commandFormFields.maskTruncate") },
        ]}
        value={col.mask_type ?? ""}
        onChange={(v) => onChange({ mask_type: v || null })}
        allowDeselect={false}
        w={130}
        data-testid={`${testId}-mask-type`}
      />
      {col.mask_type === "regex" && (
        <>
          <TextInput
            label={t("commandFormFields.maskPattern")}
            value={col.mask_pattern ?? ""}
            onChange={(e) => onChange({ mask_pattern: e.currentTarget.value })}
            w={140}
          />
          <TextInput
            label={t("commandFormFields.maskReplace")}
            value={col.mask_replace ?? ""}
            onChange={(e) => onChange({ mask_replace: e.currentTarget.value })}
            w={110}
          />
        </>
      )}
      {col.mask_type === "constant" && (
        <TextInput
          label={t("commandFormFields.maskValue")}
          value={col.mask_value == null ? "" : String(col.mask_value)}
          onChange={(e) => onChange({ mask_value: e.currentTarget.value })}
          w={120}
          data-testid={`${testId}-mask-value`}
        />
      )}
      {col.mask_type === "truncate" && (
        <Select
          label={t("commandFormFields.maskPrecision")}
          data={["year", "month", "day", "hour"].map((p) => ({ value: p, label: p }))}
          value={col.mask_precision ?? null}
          onChange={(v) => onChange({ mask_precision: v })}
          w={110}
        />
      )}
      {col.mask_type && (
        <TextInput
          label={t("commandFormFields.colUnmaskedTo")}
          placeholder={t("commandFormFields.colVisibleToPlaceholder")}
          value={(col.unmasked_to ?? []).join(", ")}
          onChange={(e) => onChange({ unmasked_to: splitRoles(e.currentTarget.value) })}
          style={{ flex: 1, minWidth: 140 }}
          data-testid={`${testId}-unmasked-to`}
        />
      )}
    </Group>
  );
}
