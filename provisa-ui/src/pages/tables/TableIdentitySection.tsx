// Copyright (c) 2026 Kenneth Stott
// Canary: 93db998a-3efd-4e8b-acb8-c4d511188241
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { Select, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { RegisteredTable } from "../../types/admin";
import {
  FormChecks,
  FormField,
  FormIdentifiers,
  FormLayout,
} from "../../components/form/FormLayout";
import { namingConventionLabel, useNamingConventions } from "../../hooks/useNamingConventions";
import { DescriptionField } from "./DescriptionField";
import { FieldLabel } from "./FieldLabel";
import { TableDraftTick, TableRegionField } from "./TablePlacementFields";

/** The Identity section of a table's form: the same when it is created and when it is edited. */
export function TableIdentitySection({
  editingTable,
  setEditingTable,
  isView,
  describe,
}: {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
  isView: boolean;
  /** Writes a description from the saved table; absent while the table is being created. */
  describe?: () => Promise<string>;
}) {
  const { t } = useTranslation();
  const conventions = useNamingConventions();
  const [generatingDesc, setGeneratingDesc] = useState(false);
  return (
    <FormLayout className="form-card" data-testid="table-section-identity-panel">
      <FormIdentifiers>
        <FormField size="medium">
          <TextInput
            label={t("tableSections.source")}
            value={editingTable.sourceId}
            readOnly
            styles={{ input: { color: "var(--text-muted)", cursor: "default" } }}
            data-testid="table-edit-source"
          />
        </FormField>
        <FormField size="medium">
          <TextInput
            label={t("tableSections.schema")}
            value={editingTable.schemaName}
            readOnly
            styles={{ input: { color: "var(--text-muted)", cursor: "default" } }}
            data-testid="table-edit-schema"
          />
        </FormField>
        <FormField size="medium">
          <TextInput
            label={t("tableSections.table")}
            value={editingTable.tableName}
            readOnly
            styles={{ input: { color: "var(--text-muted)", cursor: "default" } }}
            data-testid="table-edit-table"
          />
        </FormField>
        <FormField size="medium">
          <TextInput
            label={t("tableSections.domain")}
            value={editingTable.domainId}
            readOnly
            styles={{ input: { color: "var(--text-muted)", cursor: "default" } }}
            data-testid="table-edit-domain"
          />
        </FormField>
        <FormField help={t("tableEditForm.sqlAliasHelp")} size="medium">
          <TextInput
            label={t("tableEditForm.sqlAliasLabel")}
            value={editingTable.alias || ""}
            onChange={(e) =>
              setEditingTable({
                ...editingTable,
                alias: e.target.value || null,
              })
            }
            placeholder={t("tableEditForm.sqlAliasPlaceholder")}
          />
        </FormField>
      </FormIdentifiers>
      <FormField help={t("tableEditForm.namingConventionHelp")} size="medium">
        <Select
          label={t("tableEditForm.namingConventionLabel")}
          // REQ-416: the server's conventions; none of its own is the source's.
          data={[
            { value: "", label: t("tablesPage.inheritSource") },
            ...conventions.map((c) => ({ value: c, label: namingConventionLabel(t, c) })),
          ]}
          value={editingTable.gqlNamingConvention ?? ""}
          onChange={(v) =>
            setEditingTable({
              ...editingTable,
              gqlNamingConvention: v || null,
            })
          }
          comboboxProps={{ withinPortal: true }}
          allowDeselect={false}
        />
      </FormField>
      <TableRegionField table={editingTable} isView={isView} onChange={setEditingTable} />

      {editingTable.apiEndpoint && (
        <FormField size="long">
          <TextInput
            label={t("tableEditForm.apiEndpointLabel")}
            readOnly
            value={editingTable.apiEndpoint}
            styles={{ input: { color: "var(--text-muted)", cursor: "default" } }}
          />
        </FormField>
      )}
      <FormChecks>
        <TableDraftTick table={editingTable} onChange={setEditingTable} />
      </FormChecks>
      <FormField size="full">
        <div>
          <FieldLabel
            text={t("tableEditForm.descriptionLabel")}
            help={t("tableEditForm.descriptionHelp")}
          />
          <DescriptionField
            value={editingTable.description || ""}
            onChange={(v) => setEditingTable({ ...editingTable, description: v || null })}
            placeholder={t("tableEditForm.descriptionPlaceholder")}
            rows={2}
            generating={generatingDesc}
            onGenerate={
              describe &&
              (async () => {
                setGeneratingDesc(true);
                try {
                  const desc = await describe();
                  if (desc) setEditingTable({ ...editingTable, description: desc });
                } finally {
                  setGeneratingDesc(false);
                }
              })
            }
          />
        </div>
      </FormField>
    </FormLayout>
  );
}
