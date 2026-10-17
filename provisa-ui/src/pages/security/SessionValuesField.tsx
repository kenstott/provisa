// Copyright (c) 2026 Kenneth Stott
// Canary: 41d0d2fc-53a0-465e-bb2b-5336d02f8759
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1682: a role's constant session values -- what a row rule's
// current_setting('provisa.<name>') reads when the request binds nothing of that name. One row per
// value, name and value; the server judges each name and refuses one it cannot hold.

import { useTranslation } from "react-i18next";
import { ActionIcon, Button, TextInput } from "@mantine/core";
import { Trash2 } from "lucide-react";
import type { SessionVar } from "../../types/auth";
import { FieldLabel } from "../tables/FieldLabel";
import { FieldGroup, FormField, FormLayout } from "../../components/form/FormLayout";

export function SessionValuesField({
  rows,
  onChange,
}: {
  rows: SessionVar[];
  onChange: (rows: SessionVar[]) => void;
}) {
  const { t } = useTranslation();
  const update = (index: number, patch: Partial<SessionVar>) =>
    onChange(rows.map((r, i) => (i === index ? { ...r, ...patch } : r)));

  return (
    <FormLayout data-testid="role-session-values">
      <FormField size="full">
        <FieldLabel text={t("roleSessionValues.label")} help={t("roleSessionValues.help")} />
      </FormField>
      {rows.map((row, i) => (
        // One entry of the list: its boxes keep their widths and carry no label of their own, so
        // the action sits level with them.
        <FieldGroup row key={i} className="session-value-row" data-testid="role-session-value-row">
          <FormField size="short">
            <TextInput
              aria-label={t("roleSessionValues.name")}
              placeholder={t("roleSessionValues.name")}
              value={row.name}
              onChange={(e) => update(i, { name: e.currentTarget.value })}
              data-testid={`role-session-value-name-${i}`}
            />
          </FormField>
          <FormField size="medium">
            <TextInput
              aria-label={t("roleSessionValues.value")}
              placeholder={t("roleSessionValues.value")}
              value={row.value}
              onChange={(e) => update(i, { value: e.currentTarget.value })}
              data-testid={`role-session-value-value-${i}`}
            />
          </FormField>
          <ActionIcon
            variant="subtle"
            color="red"
            aria-label={t("roleSessionValues.remove", { name: row.name })}
            onClick={() => onChange(rows.filter((_, j) => j !== i))}
            data-testid={`role-session-value-remove-${i}`}
          >
            <Trash2 size={14} />
          </ActionIcon>
        </FieldGroup>
      ))}
      <FormField size="full">
        <Button
          variant="light"
          size="xs"
          onClick={() => onChange([...rows, { name: "", value: "" }])}
          data-testid="role-session-value-add"
        >
          {t("roleSessionValues.add")}
        </Button>
      </FormField>
    </FormLayout>
  );
}
