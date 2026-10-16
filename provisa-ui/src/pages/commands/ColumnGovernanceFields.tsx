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
// (visible_to and its ordered mask list), on an output-contract column or an inline return field. The
// keys are the wire keys: the column dict is stored as posted and read back by the server as is.

import React from "react";
import { useTranslation } from "react-i18next";
import { Text, TextInput } from "@mantine/core";
import { MaskListEditor } from "../../components/MaskListEditor";
import type { MaskEntry } from "../../lib/masks";
import { FieldGroup, FormField } from "../../components/form/FormLayout";

export interface GovernedColumn {
  name: string;
  type: string;
  visible_to?: string[] | null;
  masks?: MaskEntry[];
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
  roleOptions: { id: string; label: string }[];
}

export function ColumnGovernanceFields({
  col,
  onChange,
  testId,
  roleOptions,
}: Props): React.ReactElement {
  const { t } = useTranslation();
  return (
    // Who sees the column and how it is masked: one row of fields, set in under the column it governs.
    <FieldGroup
      row
      style={{ paddingInlineStart: "var(--mantine-spacing-md)" }}
      data-testid={testId}
    >
      <FormField size="long">
        <TextInput
          label={t("commandFormFields.colVisibleTo")}
          placeholder={t("commandFormFields.colVisibleToPlaceholder")}
          value={(col.visible_to ?? []).join(", ")}
          onChange={(e) => {
            const roles = splitRoles(e.currentTarget.value);
            onChange({ visible_to: roles.length ? roles : null });
          }}
          data-testid={`${testId}-visible-to`}
        />
      </FormField>
      <FormField size="full">
        <Text size="sm" fw={500} mb={4}>
          {t("commandFormFields.colMask")}
        </Text>
        <MaskListEditor
          value={col.masks ?? []}
          onChange={(masks) => onChange({ masks })}
          roleOptions={roleOptions}
          testId={`${testId}-masks`}
        />
      </FormField>
    </FieldGroup>
  );
}
