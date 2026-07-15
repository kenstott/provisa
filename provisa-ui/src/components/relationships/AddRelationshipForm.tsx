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
import { Button, Card, Checkbox, Group, NumberInput, Select, Stack, Text, TextInput } from "@mantine/core";
import type { RegisteredTable } from "../../types/admin";
import type { TrackedFunction } from "../../api/actions";
import type { RelForm } from "./relationship-types";

interface AddRelationshipFormProps {
  form: RelForm;
  setForm: (f: RelForm) => void;
  tables: RegisteredTable[];
  functions: TrackedFunction[];
  saving: string | null;
  onSave: () => void;
}

export function AddRelationshipForm({
  form,
  setForm,
  tables,
  functions,
  saving,
  onSave,
}: AddRelationshipFormProps) {
  const { t } = useTranslation();

  const tableOptions = tables.map((tbl) => ({ value: tbl.tableName, label: tbl.tableName }));
  const sourceColumnOptions = (
    tables.find((tbl) => tbl.tableName === form.sourceTableId)?.columns ?? []
  ).map((c) => ({ value: c.columnName, label: c.columnName }));
  const targetColumnOptions = (
    tables.find((tbl) => tbl.tableName === form.targetTableId)?.columns ?? []
  ).map((c) => ({ value: c.columnName, label: c.columnName }));
  const functionOptions = functions.map((f) => ({ value: f.name, label: f.name }));

  return (
    <Card withBorder padding="md" data-testid="add-relationship-form">
      <Stack gap="md">
        <Group align="flex-end" wrap="wrap">
          <TextInput
            label={t("addRelationshipForm.idLabel")}
            placeholder={t("addRelationshipForm.idPlaceholder")}
            value={form.id}
            onChange={(e) => setForm({ ...form, id: e.currentTarget.value })}
            data-testid="rel-form-id"
          />
          <TextInput
            label={t("addRelationshipForm.aliasLabel")}
            placeholder={t("addRelationshipForm.aliasPlaceholder")}
            value={form.alias}
            onChange={(e) => setForm({ ...form, alias: e.currentTarget.value })}
            data-testid="rel-form-alias"
          />
          <TextInput
            label={t("addRelationshipForm.graphqlAliasLabel")}
            placeholder={t("addRelationshipForm.graphqlAliasPlaceholder")}
            value={form.graphqlAlias}
            onChange={(e) => setForm({ ...form, graphqlAlias: e.currentTarget.value })}
            data-testid="rel-form-graphql-alias"
          />
          <Select
            label={t("addRelationshipForm.sourceTableLabel")}
            placeholder={t("addRelationshipForm.selectPlaceholder")}
            data={tableOptions}
            value={form.sourceTableId || null}
            onChange={(v) => setForm({ ...form, sourceTableId: v ?? "" })}
            data-testid="rel-form-source-table"
          />
          <Select
            label={t("addRelationshipForm.sourceColumnLabel")}
            placeholder={t("addRelationshipForm.selectPlaceholder")}
            data={sourceColumnOptions}
            value={form.sourceColumn || null}
            onChange={(v) => setForm({ ...form, sourceColumn: v ?? "" })}
            data-testid="rel-form-source-column"
          />
        </Group>
        <Group align="flex-end" wrap="wrap">
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
          {form.targetType === "table" ? (
            <>
              <Select
                label={t("addRelationshipForm.targetTableLabel")}
                placeholder={t("addRelationshipForm.selectPlaceholder")}
                data={tableOptions}
                value={form.targetTableId || null}
                onChange={(v) => setForm({ ...form, targetTableId: v ?? "" })}
                data-testid="rel-form-target-table"
              />
              <Select
                label={t("addRelationshipForm.targetColumnLabel")}
                placeholder={t("addRelationshipForm.selectPlaceholder")}
                data={targetColumnOptions}
                value={form.targetColumn || null}
                onChange={(v) => setForm({ ...form, targetColumn: v ?? "" })}
                data-testid="rel-form-target-column"
              />
            </>
          ) : (
            <>
              <Select
                label={t("addRelationshipForm.functionLabel")}
                placeholder={t("addRelationshipForm.selectPlaceholder")}
                data={functionOptions}
                value={form.targetFunctionName || null}
                onChange={(v) => setForm({ ...form, targetFunctionName: v ?? "" })}
                data-testid="rel-form-function"
              />
              <TextInput
                label={t("addRelationshipForm.functionArgLabel")}
                placeholder={t("addRelationshipForm.functionArgPlaceholder")}
                value={form.functionArg}
                onChange={(e) => setForm({ ...form, functionArg: e.currentTarget.value })}
                data-testid="rel-form-function-arg"
              />
            </>
          )}
        </Group>
        {form.targetType === "table" && (
          <Stack gap={4} maw={240}>
            <Select
              label={t("addRelationshipForm.cardinalityLabel")}
              data={[
                { value: "many-to-one", label: t("addRelationshipForm.cardinalityManyToOne") },
                { value: "one-to-many", label: t("addRelationshipForm.cardinalityOneToMany") },
              ]}
              value={form.cardinality}
              allowDeselect={false}
              onChange={(v) => setForm({ ...form, cardinality: v ?? "many-to-one" })}
              data-testid="rel-form-cardinality"
            />
            {form.cardinality === "many-to-one" && (
              <Text c="var(--warning, #b45309)" fz="xs">
                {t("addRelationshipForm.cardinalityWarning")}
              </Text>
            )}
          </Stack>
        )}
        <Group align="flex-end" wrap="wrap">
          <Checkbox
            label={t("addRelationshipForm.materializeLabel")}
            checked={form.materialize}
            onChange={(e) => setForm({ ...form, materialize: e.currentTarget.checked })}
            data-testid="rel-form-materialize"
          />
          <Checkbox
            label={t("addRelationshipForm.disableCypherLabel")}
            checked={form.disableCypher}
            onChange={(e) => setForm({ ...form, disableCypher: e.currentTarget.checked })}
            data-testid="rel-form-disable-cypher"
          />
          {form.materialize && (
            <NumberInput
              label={t("addRelationshipForm.refreshIntervalLabel")}
              value={form.refreshInterval}
              onChange={(v) => setForm({ ...form, refreshInterval: String(v) })}
              data-testid="rel-form-refresh-interval"
            />
          )}
          <Button onClick={onSave} disabled={saving === "new"} data-testid="rel-form-save">
            {t("addRelationshipForm.save")}
          </Button>
        </Group>
      </Stack>
    </Card>
  );
}
