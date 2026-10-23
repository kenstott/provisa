// Copyright (c) 2026 Kenneth Stott
// Canary: e79cb9b1-d78b-4d3b-a2b8-4cc6b7a399f9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { Select, Text, TextInput } from "@mantine/core";
import type { RegisteredTable } from "../../types/admin";
import type { TrackedFunction } from "../../api/actions";
import { tableRef, type RelForm } from "./relationship-types";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";

interface AddRelationshipFormProps {
  form: RelForm;
  setForm: (f: RelForm) => void;
  tables: RegisteredTable[];
  functions: TrackedFunction[];
  saving: string | null;
  /** What the form says of a save that did not go through. */
  problem: string;
  onSave: () => void;
  onCancel: () => void;
}

/**
 * The first step of a new relationship: only what one needs in order to exist -- its id, what it
 * joins, and how many rows meet. It is the head of the form that edits a relationship; the rest
 * (aliases, materializing, the junction table) is edited in the detail the save opens.
 */
export function AddRelationshipForm({
  form,
  setForm,
  tables,
  functions,
  saving,
  problem,
  onSave,
  onCancel,
}: AddRelationshipFormProps) {
  const { t } = useTranslation();

  const tableOptions = tables.map((tbl) => ({ value: tableRef(tbl), label: tableRef(tbl) }));
  const sourceColumnOptions = (
    tables.find((tbl) => tableRef(tbl) === form.sourceTableId)?.columns ?? []
  ).map((c) => ({ value: c.columnName, label: c.columnName }));
  const targetColumnOptions = (
    tables.find((tbl) => tableRef(tbl) === form.targetTableId)?.columns ?? []
  ).map((c) => ({ value: c.columnName, label: c.columnName }));
  const functionOptions = functions.map((f) => ({ value: f.name, label: f.name }));

  return (
    <div data-testid="add-relationship-form">
      <FormLayout>
        <FormIdentifiers>
          <FormField size="medium">
            <TextInput
              label={t("addRelationshipForm.idLabel")}
              placeholder={t("addRelationshipForm.idPlaceholder")}
              value={form.id}
              onChange={(e) => setForm({ ...form, id: e.currentTarget.value })}
              data-testid="rel-form-id"
            />
          </FormField>
        </FormIdentifiers>
        <FormField size="medium">
          <Select
            label={t("addRelationshipForm.sourceTableLabel")}
            placeholder={t("addRelationshipForm.selectPlaceholder")}
            data={tableOptions}
            value={form.sourceTableId || null}
            onChange={(v) => setForm({ ...form, sourceTableId: v ?? "" })}
            data-testid="rel-form-source-table"
          />
        </FormField>
        <FormField size="medium">
          <Select
            label={t("addRelationshipForm.sourceColumnLabel")}
            placeholder={t("addRelationshipForm.selectPlaceholder")}
            data={sourceColumnOptions}
            value={form.sourceColumn || null}
            onChange={(v) => setForm({ ...form, sourceColumn: v ?? "" })}
            data-testid="rel-form-source-column"
          />
        </FormField>
        <FormField size="medium">
          <Select
            label={t("addRelationshipForm.targetTypeLabel")}
            data={[
              { value: "table", label: t("addRelationshipForm.targetTypeTable") },
              { value: "function", label: t("addRelationshipForm.targetTypeFunction") },
            ]}
            value={form.targetType}
            allowDeselect={false}
            onChange={(v) =>
              setForm({
                ...form,
                targetType: (v ?? "table") as "table" | "function",
                targetTableId: "",
                targetColumn: "",
                targetFunctionName: "",
                functionArg: "",
              })
            }
            data-testid="rel-form-target-type"
          />
        </FormField>
        {form.targetType === "table" ? (
          <>
            <FormField size="medium">
              <Select
                label={t("addRelationshipForm.targetTableLabel")}
                placeholder={t("addRelationshipForm.selectPlaceholder")}
                data={tableOptions}
                value={form.targetTableId || null}
                onChange={(v) => setForm({ ...form, targetTableId: v ?? "" })}
                data-testid="rel-form-target-table"
              />
            </FormField>
            <FormField size="medium">
              <Select
                label={t("addRelationshipForm.targetColumnLabel")}
                placeholder={t("addRelationshipForm.selectPlaceholder")}
                data={targetColumnOptions}
                value={form.targetColumn || null}
                onChange={(v) => setForm({ ...form, targetColumn: v ?? "" })}
                data-testid="rel-form-target-column"
              />
            </FormField>
          </>
        ) : (
          <>
            <FormField size="medium">
              <Select
                label={t("addRelationshipForm.functionLabel")}
                placeholder={t("addRelationshipForm.selectPlaceholder")}
                data={functionOptions}
                value={form.targetFunctionName || null}
                onChange={(v) => setForm({ ...form, targetFunctionName: v ?? "" })}
                data-testid="rel-form-function"
              />
            </FormField>
            <FormField size="medium">
              <TextInput
                label={t("addRelationshipForm.functionArgLabel")}
                placeholder={t("addRelationshipForm.functionArgPlaceholder")}
                value={form.functionArg}
                onChange={(e) => setForm({ ...form, functionArg: e.currentTarget.value })}
                data-testid="rel-form-function-arg"
              />
            </FormField>
          </>
        )}
        {form.targetType === "table" && (
          <>
            <FormField size="medium">
              <Select
                label={t("addRelationshipForm.cardinalityLabel")}
                data={[
                  { value: "many-to-one", label: t("addRelationshipForm.cardinalityManyToOne") },
                  { value: "one-to-many", label: t("addRelationshipForm.cardinalityOneToMany") },
                  { value: "one-to-one", label: t("addRelationshipForm.cardinalityOneToOne") },
                ]}
                value={form.cardinality}
                allowDeselect={false}
                onChange={(v) => setForm({ ...form, cardinality: v ?? "many-to-one" })}
                data-testid="rel-form-cardinality"
              />
            </FormField>
            {form.cardinality === "many-to-one" && (
              <FormField size="full">
                <Text c="var(--warning, #b45309)" fz="xs">
                  {t("addRelationshipForm.cardinalityWarning")}
                </Text>
              </FormField>
            )}
          </>
        )}
        <FormFooter
          panel
          problem={problem || undefined}
          onSave={onSave}
          saving={saving === "new"}
          onCancel={onCancel}
          saveProps={{ "data-testid": "rel-form-save" }}
        />
      </FormLayout>
    </div>
  );
}
