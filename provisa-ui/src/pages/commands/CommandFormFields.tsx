// Copyright (c) 2026 Kenneth Stott
// Canary: a8c1e542-3d09-4f77-b236-7e4a90f12d83
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Button,
  Group,
  NumberInput,
  Select,
  Switch,
  TextInput,
  Title,
} from "@mantine/core";
import { X } from "lucide-react";
import type { Source, RegisteredTable } from "../../types/admin";
import type { TableMetadata } from "../../api/admin";
import type { ActionArg, InlineField } from "../../api/actions";
import {
  GRAPHQL_TYPES,
  IR_TYPES,
  IMPL_KINDS,
  ARG_KINDS,
  DATASET_ARG_KINDS,
  EMPTY_ARG,
  EMPTY_INLINE,
  EMPTY_DATASET_COLUMN,
} from "./types";
import type { FormState } from "./types";

interface CommandFormFieldsProps {
  form: FormState;
  setForm: React.Dispatch<React.SetStateAction<FormState>>;
  sources: Source[];
  tables: RegisteredTable[];
  domainHints: string[];
  availableFunctions: TableMetadata[];
  loadingFunctions: boolean;
}

export function CommandFormFields({
  form,
  setForm,
  sources,
  tables,
  domainHints,
  availableFunctions,
  loadingFunctions,
}: CommandFormFieldsProps): React.ReactElement {
  const { t } = useTranslation();

  const physicalTableOptions = (sourceId: string) =>
    tables
      .filter((tbl) => tbl.sourceId === sourceId)
      .map((tbl) => ({
        value: `${tbl.schemaName}.${tbl.tableName}`,
        label: `${tbl.schemaName}.${tbl.tableName}${tbl.alias ? ` (${tbl.alias})` : ""}`,
      }));

  const normalizePart = (s: string) => s.replace(/[^a-zA-Z0-9]/g, "_").replace(/^_+|_+$/g, "");
  const virtualTableOptions = tables.map((tbl) => ({
    value: `${normalizePart(tbl.sourceId)}.${normalizePart(tbl.schemaName)}.${tbl.tableName}`,
    label: `${normalizePart(tbl.sourceId)}.${normalizePart(tbl.schemaName)}.${tbl.tableName}${tbl.alias ? ` (${tbl.alias})` : ""}`,
  }));

  const handleAddArg = () => setForm({ ...form, arguments: [...form.arguments, { ...EMPTY_ARG }] });
  const handleRemoveArg = (idx: number) =>
    setForm({ ...form, arguments: form.arguments.filter((_, i) => i !== idx) });
  const handleArgChange = (idx: number, field: keyof ActionArg, value: string) => {
    const args = [...form.arguments];
    args[idx] = { ...args[idx], [field]: value };
    setForm({ ...form, arguments: args });
  };

  // REQ-1159: per-dataset input column contract (IR-typed) on a table_ref/result_set arg.
  const setArgColumns = (idx: number, columns: { name: string; type: string }[]) => {
    const args = [...form.arguments];
    args[idx] = { ...args[idx], columns };
    setForm({ ...form, arguments: args });
  };
  const addArgColumn = (idx: number) =>
    setArgColumns(idx, [...(form.arguments[idx].columns ?? []), { ...EMPTY_DATASET_COLUMN }]);
  const changeArgColumn = (idx: number, ci: number, field: "name" | "type", value: string) => {
    const cols = [...(form.arguments[idx].columns ?? [])];
    cols[ci] = { ...cols[ci], [field]: value };
    setArgColumns(idx, cols);
  };
  const removeArgColumn = (idx: number, ci: number) =>
    setArgColumns(idx, (form.arguments[idx].columns ?? []).filter((_, i) => i !== ci));

  // REQ-1159: canonical IR-typed output dataset contract.
  const addOutputColumn = () =>
    setForm({ ...form, outputColumns: [...form.outputColumns, { ...EMPTY_DATASET_COLUMN }] });
  const changeOutputColumn = (ci: number, field: "name" | "type", value: string) => {
    const cols = [...form.outputColumns];
    cols[ci] = { ...cols[ci], [field]: value };
    setForm({ ...form, outputColumns: cols });
  };
  const removeOutputColumn = (ci: number) =>
    setForm({ ...form, outputColumns: form.outputColumns.filter((_, i) => i !== ci) });
  const bindingStr = (key: string): string => {
    const v = form.binding[key];
    if (v == null) return "";
    return Array.isArray(v) ? v.join(" ") : String(v);
  };
  const setBinding = (key: string, value: unknown) =>
    setForm({ ...form, binding: { ...form.binding, [key]: value } });

  const handleAddInlineField = () =>
    setForm({ ...form, inlineReturnType: [...form.inlineReturnType, { ...EMPTY_INLINE }] });
  const handleRemoveInlineField = (idx: number) =>
    setForm({ ...form, inlineReturnType: form.inlineReturnType.filter((_, i) => i !== idx) });
  const handleInlineFieldChange = (idx: number, field: keyof InlineField, value: string) => {
    const fields = [...form.inlineReturnType];
    fields[idx] = { ...fields[idx], [field]: value };
    setForm({ ...form, inlineReturnType: fields });
  };

  const selectedSource = sources.find((s) => s.id === form.sourceId);
  const isOpenApiSource = selectedSource?.type === "openapi";

  return (
    <>
      {form.actionType === "function" && (
        <>
          <Select
            label={t("commandFormFields.implementation")}
            data={IMPL_KINDS}
            value={form.implKind}
            onChange={(val) => setForm({ ...form, implKind: val ?? "source_procedure" })}
            allowDeselect={false}
            data-testid="command-impl-kind-select"
          />
          {form.implKind === "source_procedure" && (
            <>
              <Select
                label={t("commandFormFields.source")}
                placeholder={t("commandFormFields.selectSource")}
                required
                data={sources.map((s) => ({ value: s.id, label: `${s.id} (${s.type})` }))}
                value={form.sourceId || null}
                onChange={(val) => {
                  const selectedSrc = sources.find((s) => s.id === val);
                  setForm({
                    ...form,
                    sourceId: val ?? "",
                    schemaName: selectedSrc?.type === "openapi" ? "openapi" : form.schemaName,
                    functionName: "",
                  });
                }}
                data-testid="command-source-select"
              />
              <TextInput
                label={t("commandFormFields.schema")}
                value={form.schemaName}
                onChange={(e) => setForm({ ...form, schemaName: e.currentTarget.value })}
                readOnly={isOpenApiSource}
                data-testid="command-schema-input"
              />
              {isOpenApiSource ? (
                <Select
                  label={t("commandFormFields.functionName")}
                  required
                  data={availableFunctions.map((f) => ({
                    value: f.name,
                    label: f.comment ? `${f.name} — ${f.comment}` : f.name,
                  }))}
                  value={form.functionName || null}
                  onChange={(val) => setForm({ ...form, functionName: val ?? "" })}
                  disabled={loadingFunctions}
                  placeholder={
                    loadingFunctions
                      ? t("commandFormFields.loading")
                      : t("commandFormFields.selectOperation")
                  }
                  data-testid="command-function-select"
                />
              ) : (
                <TextInput
                  label={t("commandFormFields.functionName")}
                  required
                  value={form.functionName}
                  onChange={(e) => setForm({ ...form, functionName: e.currentTarget.value })}
                  placeholder={t("commandFormFields.dbFunctionNamePlaceholder")}
                  data-testid="command-function-input"
                />
              )}
            </>
          )}
          {form.implKind === "script" && (
            <TextInput
              label={t("commandFormFields.bindingArgv")}
              required
              value={bindingStr("argv")}
              onChange={(e) => setBinding("argv", e.currentTarget.value.split(/\s+/).filter(Boolean))}
              placeholder="/usr/local/bin/transform --json"
              data-testid="command-binding-argv"
            />
          )}
          {form.implKind === "http" && (
            <>
              <TextInput
                label={t("commandFormFields.bindingUrl")}
                required
                value={bindingStr("url")}
                onChange={(e) => setBinding("url", e.currentTarget.value)}
                placeholder="https://svc.internal/fn"
                data-testid="command-binding-url"
              />
              <Select
                label={t("commandFormFields.method")}
                data={["POST", "GET", "PUT", "PATCH"]}
                value={bindingStr("method") || "POST"}
                onChange={(val) => setBinding("method", val ?? "POST")}
                allowDeselect={false}
              />
            </>
          )}
          {form.implKind === "grpc" && (
            <>
              <TextInput
                label={t("commandFormFields.bindingTarget")}
                required
                value={bindingStr("target")}
                onChange={(e) => setBinding("target", e.currentTarget.value)}
                placeholder="svc.internal:50051"
                data-testid="command-binding-target"
              />
              <TextInput
                label={t("commandFormFields.bindingGrpcMethod")}
                required
                value={bindingStr("method")}
                onChange={(e) => setBinding("method", e.currentTarget.value)}
                placeholder="pkg.Service.Method"
                data-testid="command-binding-grpc-method"
              />
              <Switch
                label={t("commandFormFields.bindingTls")}
                checked={!!form.binding.tls}
                onChange={(e) => setBinding("tls", e.currentTarget.checked)}
              />
            </>
          )}
          {form.implKind === "python" && (
            <TextInput
              label={t("commandFormFields.bindingCallable")}
              required
              value={bindingStr("callable")}
              onChange={(e) => setBinding("callable", e.currentTarget.value)}
              placeholder="my_pkg.udfs:transform"
              data-testid="command-binding-callable"
            />
          )}
          {form.implKind !== "source_procedure" && (
            <Switch
              label={t("commandFormFields.materialize")}
              description={t("commandFormFields.materializeHint")}
              checked={form.materialize}
              onChange={(e) => setForm({ ...form, materialize: e.currentTarget.checked })}
              data-testid="command-materialize-switch"
            />
          )}
          <Select
            label={t("commandFormFields.returnType")}
            data={[
              { value: "table", label: t("commandFormFields.registeredTable") },
              { value: "dataset", label: t("commandFormFields.datasetColumns") },
            ]}
            value={form.returnSchemaMode}
            onChange={(val) =>
              setForm({
                ...form,
                returnSchemaMode: (val ?? "table") as "table" | "dataset",
                returns: "",
              })
            }
            allowDeselect={false}
            data-testid="command-return-type-select"
          />
          {form.returnSchemaMode === "table" && (
            <Select
              label={t("commandFormFields.returnsTable")}
              placeholder={t("commandFormFields.selectTable")}
              data={physicalTableOptions(form.sourceId)}
              value={form.returns || null}
              onChange={(val) => setForm({ ...form, returns: val ?? "" })}
              data-testid="command-returns-table-select"
            />
          )}
          <TextInput
            label={t("commandFormFields.visibleTo")}
            value={form.visibleTo}
            onChange={(e) => setForm({ ...form, visibleTo: e.currentTarget.value })}
            placeholder={t("commandFormFields.visibleToPlaceholder")}
          />
          <TextInput
            label={t("commandFormFields.writableBy")}
            value={form.writablBy}
            onChange={(e) => setForm({ ...form, writablBy: e.currentTarget.value })}
            placeholder={t("commandFormFields.writableByPlaceholder")}
          />
        </>
      )}
      {form.actionType === "webhook" && (
        <>
          <TextInput
            label={t("commandFormFields.url")}
            required
            value={form.url}
            onChange={(e) => setForm({ ...form, url: e.currentTarget.value })}
            placeholder={t("commandFormFields.urlPlaceholder")}
            data-testid="command-url-input"
          />
          <Select
            label={t("commandFormFields.method")}
            data={["POST", "GET", "PUT", "PATCH"]}
            value={form.method}
            onChange={(val) => setForm({ ...form, method: val ?? "POST" })}
            allowDeselect={false}
            data-testid="command-method-select"
          />
          <NumberInput
            label={t("commandFormFields.timeoutMs")}
            min={100}
            value={form.timeoutMs}
            onChange={(val) => setForm({ ...form, timeoutMs: typeof val === "number" ? val : 0 })}
          />
          <Select
            label={t("commandFormFields.returnsTableOptional")}
            placeholder={t("commandFormFields.noneInlineType")}
            data={virtualTableOptions}
            value={form.returns || null}
            onChange={(val) => setForm({ ...form, returns: val ?? "" })}
          />
          <TextInput
            label={t("commandFormFields.visibleTo")}
            value={form.visibleTo}
            onChange={(e) => setForm({ ...form, visibleTo: e.currentTarget.value })}
            placeholder={t("commandFormFields.visibleToPlaceholder")}
          />
          {!form.returns && (
            <div style={{ gridColumn: "1 / -1" }}>
              <Title order={5} mb="xs">
                {t("commandFormFields.inlineReturnType")}
              </Title>
              {form.inlineReturnType.map((f, i) => (
                <Group key={i} gap="xs" mb="xs" align="center" wrap="nowrap">
                  <TextInput
                    value={f.name}
                    onChange={(e) => handleInlineFieldChange(i, "name", e.currentTarget.value)}
                    placeholder={t("commandFormFields.fieldNamePlaceholder")}
                    style={{ flex: 1, minWidth: 0 }}
                  />
                  <Select
                    value={f.type}
                    onChange={(val) => handleInlineFieldChange(i, "type", val ?? "String")}
                    data={GRAPHQL_TYPES}
                    allowDeselect={false}
                    w={120}
                  />
                  <ActionIcon
                    variant="subtle"
                    color="red"
                    aria-label={t("commandFormFields.removeField", { name: f.name || i + 1 })}
                    onClick={() => handleRemoveInlineField(i)}
                  >
                    <X size={14} />
                  </ActionIcon>
                </Group>
              ))}
              <Button variant="subtle" size="xs" onClick={handleAddInlineField}>
                {t("commandFormFields.addField")}
              </Button>
            </div>
          )}
        </>
      )}
      <Select
        label={t("commandFormFields.kind")}
        data={[
          { value: "mutation", label: t("commandFormFields.mutation") },
          { value: "query", label: t("commandFormFields.query") },
        ]}
        value={form.kind}
        onChange={(val) => setForm({ ...form, kind: val ?? "mutation" })}
        allowDeselect={false}
      />
      <Select
        label={t("commandFormFields.domain")}
        placeholder={t("commandFormFields.selectDomain")}
        data={domainHints}
        value={form.domainId || null}
        onChange={(val) => setForm({ ...form, domainId: val ?? "" })}
      />
      <TextInput
        label={t("commandFormFields.description")}
        value={form.description}
        onChange={(e) => setForm({ ...form, description: e.currentTarget.value })}
        placeholder={t("commandFormFields.descriptionPlaceholder")}
      />
      <div style={{ gridColumn: "1 / -1" }}>
        <Title order={5} mb="xs">
          {t("commandFormFields.arguments")}
        </Title>
        {form.arguments.map((arg, i) => (
          <div key={i}>
            <Group gap="xs" mb="xs" align="center" wrap="nowrap">
              <TextInput
                value={arg.name}
                onChange={(e) => handleArgChange(i, "name", e.currentTarget.value)}
                placeholder={t("commandFormFields.argNamePlaceholder")}
                style={{ flex: 1, minWidth: 0 }}
              />
              <Select
                value={arg.type}
                onChange={(val) => handleArgChange(i, "type", val ?? "String")}
                data={GRAPHQL_TYPES}
                allowDeselect={false}
                w={120}
              />
              {form.actionType === "function" && form.implKind !== "source_procedure" && (
                <Select
                  aria-label={t("commandFormFields.argKind")}
                  value={arg.argKind ?? "column_value"}
                  onChange={(val) => handleArgChange(i, "argKind", val ?? "column_value")}
                  data={ARG_KINDS}
                  allowDeselect={false}
                  w={200}
                  data-testid={`command-arg-kind-${i}`}
                />
              )}
              <ActionIcon
                variant="subtle"
                color="red"
                aria-label={t("commandFormFields.removeArgument", { name: arg.name || i + 1 })}
                onClick={() => handleRemoveArg(i)}
              >
                <X size={14} />
              </ActionIcon>
            </Group>
            {/* REQ-1159: a dataset arg (table_ref/result_set) carries an IR-typed column contract. */}
            {form.actionType === "function" &&
              DATASET_ARG_KINDS.has(arg.argKind ?? "column_value") && (
                <div style={{ marginLeft: 24, marginBottom: 12 }} data-testid={`dataset-columns-${i}`}>
                  <Title order={6} c="dimmed" mb={4}>
                    input dataset columns
                  </Title>
                  {(arg.columns ?? []).map((col, ci) => (
                    <Group key={ci} gap="xs" mb={4} align="center" wrap="nowrap">
                      <TextInput
                        value={col.name}
                        onChange={(e) => changeArgColumn(i, ci, "name", e.currentTarget.value)}
                        placeholder="column"
                        size="xs"
                        style={{ flex: 1, minWidth: 0 }}
                      />
                      <Select
                        value={col.type}
                        onChange={(val) => changeArgColumn(i, ci, "type", val ?? "text")}
                        data={IR_TYPES}
                        allowDeselect={false}
                        size="xs"
                        w={130}
                      />
                      <ActionIcon
                        variant="subtle"
                        color="red"
                        aria-label={`remove input column ${col.name || ci + 1}`}
                        onClick={() => removeArgColumn(i, ci)}
                      >
                        <X size={12} />
                      </ActionIcon>
                    </Group>
                  ))}
                  <Button variant="subtle" size="xs" onClick={() => addArgColumn(i)}>
                    add input column
                  </Button>
                </div>
              )}
          </div>
        ))}
        <Button variant="subtle" size="xs" onClick={handleAddArg}>
          {t("commandFormFields.addArgument")}
        </Button>
      </div>
      {/* REQ-1159: canonical IR-typed output dataset contract (returnSchema is its GraphQL projection). */}
      {form.actionType === "function" &&
        form.implKind !== "source_procedure" &&
        form.returnSchemaMode === "dataset" && (
        <div style={{ gridColumn: "1 / -1" }} data-testid="output-columns">
          <Title order={5} mb="xs">
            output dataset columns
          </Title>
          {form.outputColumns.map((col, ci) => (
            <Group key={ci} gap="xs" mb="xs" align="center" wrap="nowrap">
              <TextInput
                value={col.name}
                onChange={(e) => changeOutputColumn(ci, "name", e.currentTarget.value)}
                placeholder="column"
                style={{ flex: 1, minWidth: 0 }}
              />
              <Select
                value={col.type}
                onChange={(val) => changeOutputColumn(ci, "type", val ?? "text")}
                data={IR_TYPES}
                allowDeselect={false}
                w={130}
              />
              <ActionIcon
                variant="subtle"
                color="red"
                aria-label={`remove output column ${col.name || ci + 1}`}
                onClick={() => removeOutputColumn(ci)}
              >
                <X size={14} />
              </ActionIcon>
            </Group>
          ))}
          <Button variant="subtle" size="xs" onClick={addOutputColumn}>
            add output column
          </Button>
        </div>
      )}
    </>
  );
}
