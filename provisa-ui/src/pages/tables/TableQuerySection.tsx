// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Checkbox } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { RegisteredTable } from "../../types/admin";
import { FormChecks, FormField, FormLayout } from "../../components/form/FormLayout";
import { ParametersPanel } from "./ParametersPanel";
import type { UpdateEditCol } from "./tableEditTypes";

/** The Query section of a table's form: its aggregates, group-by and parameters. */
export function TableQuerySection({
  editingTable,
  setEditingTable,
  updateEditCol,
}: {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
  updateEditCol: UpdateEditCol;
}) {
  const { t } = useTranslation();
  return (
    <>
      <FormLayout className="form-card" data-testid="table-section-query-panel">
        <FormChecks>
          <FormField size="short" help={t("tableEditForm.enableAggregatesDesc")}>
            <Checkbox
              checked={editingTable.enableAggregates}
              onChange={(e) =>
                setEditingTable({
                  ...editingTable,
                  enableAggregates: e.currentTarget.checked,
                })
              }
              label={t("tableEditForm.enableAggregatesLabel")}
            />
          </FormField>
          <FormField size="short" help={t("tableEditForm.enableGroupByDesc")}>
            <Checkbox
              checked={editingTable.enableGroupBy}
              onChange={(e) =>
                setEditingTable({
                  ...editingTable,
                  enableGroupBy: e.currentTarget.checked,
                })
              }
              label={t("tableEditForm.enableGroupByLabel")}
            />
          </FormField>
        </FormChecks>
      </FormLayout>
      {/* REQ-1986: the table's parameters, and the steward's required switch. */}
      <ParametersPanel
        columns={editingTable.columns}
        onRequiredChange={(i, required) => updateEditCol(i, "requiredBySteward", required)}
      />
    </>
  );
}
