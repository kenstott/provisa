// Copyright (c) 2026 Kenneth Stott
// Canary: 317355c6-0dab-4680-82bc-ca9d784985d3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { MultiSelect, Select } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";

/** REQ-2109: the three values a table or a view maps to its columns to keep its versions. */
export interface HistoryDeclaration {
  businessKey: string[];
  validTime: string | null;
  recordedTime: string | null;
}

// REQ-2109: the three columns a table or a view keeps its versions by -- its business key, its
// valid time and its recorded time -- each picked from its own columns. They are declared
// together or not at all; the server refuses a partial declaration by name. `locked`: versions
// already exist, so the business key and valid time they were recorded under are not offered.
// `withBusinessKey` false: the form names the business key itself (a model's key).
export function BitemporalFields({
  value,
  columns,
  onChange,
  locked,
  withBusinessKey,
}: {
  value: HistoryDeclaration;
  columns: string[];
  onChange: (next: HistoryDeclaration) => void;
  locked: boolean;
  withBusinessKey: boolean;
}) {
  const { t } = useTranslation();
  return (
    <>
      {withBusinessKey && !locked && (
        <FormField
          size="medium"
          hint={t("tableEditForm.businessKeyHint")}
          help={t("tableEditForm.businessKeyHelp")}
        >
          <MultiSelect
            label={t("tableEditForm.businessKeyLabel")}
            data-testid="business-key"
            data={columns}
            value={value.businessKey}
            onChange={(v) => onChange({ ...value, businessKey: v })}
            searchable
            comboboxProps={{ withinPortal: true }}
          />
        </FormField>
      )}
      {!locked && (
        <FormField
          size="medium"
          hint={t("tableEditForm.validTimeHint")}
          help={t("tableEditForm.validTimeHelp")}
        >
          <Select
            label={t("tableEditForm.validTimeLabel")}
            data-testid="valid-time"
            data={columns}
            value={value.validTime}
            onChange={(v) => onChange({ ...value, validTime: v })}
            clearable
            comboboxProps={{ withinPortal: true }}
          />
        </FormField>
      )}
      <FormField
        size="medium"
        hint={t("tableEditForm.recordedTimeHint")}
        help={t("tableEditForm.recordedTimeHelp")}
      >
        <Select
          label={t("tableEditForm.recordedTimeLabel")}
          data-testid="recorded-time"
          data={columns}
          value={value.recordedTime}
          onChange={(v) => onChange({ ...value, recordedTime: v })}
          clearable
          comboboxProps={{ withinPortal: true }}
        />
      </FormField>
    </>
  );
}
