// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { MultiSelect, Select } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { RegisteredTable } from "../../types/admin";
import { FormField } from "../../components/form/FormLayout";

// REQ-2109: the three columns a table or a view keeps its versions by -- its business key, its
// valid time and its recorded time -- each picked from the table's own columns. They are declared
// together or not at all; the server refuses a partial declaration by name.
export function BitemporalFields({
  editingTable,
  setEditingTable,
}: {
  editingTable: RegisteredTable;
  setEditingTable: (t: RegisteredTable) => void;
}) {
  const { t } = useTranslation();
  const columns = editingTable.columns.map((c) => c.columnName);
  return (
    <>
      <FormField
        size="medium"
        hint={t("tableEditForm.businessKeyHint")}
        help={t("tableEditForm.businessKeyHelp")}
      >
        <MultiSelect
          label={t("tableEditForm.businessKeyLabel")}
          data-testid="business-key"
          data={columns}
          value={editingTable.businessKey}
          onChange={(v) => setEditingTable({ ...editingTable, businessKey: v })}
          searchable
          comboboxProps={{ withinPortal: true }}
        />
      </FormField>
      <FormField
        size="medium"
        hint={t("tableEditForm.validTimeHint")}
        help={t("tableEditForm.validTimeHelp")}
      >
        <Select
          label={t("tableEditForm.validTimeLabel")}
          data-testid="valid-time"
          data={columns}
          value={editingTable.validTime}
          onChange={(v) => setEditingTable({ ...editingTable, validTime: v })}
          clearable
          comboboxProps={{ withinPortal: true }}
        />
      </FormField>
      <FormField
        size="medium"
        hint={t("tableEditForm.recordedTimeHint")}
        help={t("tableEditForm.recordedTimeHelp")}
      >
        <Select
          label={t("tableEditForm.recordedTimeLabel")}
          data-testid="recorded-time"
          data={columns}
          value={editingTable.recordedTime}
          onChange={(v) => setEditingTable({ ...editingTable, recordedTime: v })}
          clearable
          comboboxProps={{ withinPortal: true }}
        />
      </FormField>
    </>
  );
}
