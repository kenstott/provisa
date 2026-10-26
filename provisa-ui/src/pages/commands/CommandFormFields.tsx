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
  Autocomplete,
  Button,
  NumberInput,
  Select,
  Switch,
  Textarea,
  TextInput,
  Title,
} from "@mantine/core";
import { X } from "lucide-react";
import type { Source, RegisteredTable, DataProduct } from "../../types/admin";
import type { TableMetadata } from "../../api/admin";
import type { ActionArg, InlineField, DatasetColumn } from "../../api/actions";
import {
  GRAPHQL_TYPES,
  IR_TYPES,
  IMPL_KINDS,
  ARG_KINDS,
  DATASET_ARG_KINDS,
  EMPTY_ARG,
  EMPTY_INLINE,
  EMPTY_DATASET_COLUMN,
  OPERATION_SCHEMA,
  CODE_SCHEMA,
} from "./types";
import type { FormState } from "./types";
import { ColumnGovernanceFields } from "./ColumnGovernanceFields";
import { physicalRelationName } from "../../naming";
import {
  FieldGroup,
  FormChecks,
  FormEntry,
  FormField,
  FormLayout,
} from "../../components/form/FormLayout";
import { assignableDomainIds } from "../../lib/internalDomains";

interface CommandFormFieldsProps {
  /** The short step that creates a command: only what it needs in order to exist -- what it
   * calls. Everything else starts at its default and is set in the edit form the new command
   * then opens in. */
  essential?: boolean;
  form: FormState;
  setForm: React.Dispatch<React.SetStateAction<FormState>>;
  sources: Source[];
  tables: RegisteredTable[];
  domainHints: string[];
  availableFunctions: TableMetadata[];
  loadingFunctions: boolean;
  // REQ-1634: optional data-product membership picker, scoped to the command's domain.
  dataProducts: DataProduct[];
  // REQ-2059: the roles an output column's mask entries may name.
  roleOptions: { id: string; label: string }[];
}

export function CommandFormFields({
  essential = false,
  form,
  setForm,
  sources,
  tables,
  domainHints,
  availableFunctions,
  loadingFunctions,
  dataProducts,
  roleOptions,
}: CommandFormFieldsProps): React.ReactElement {
  const { t } = useTranslation();

  const physicalTableOptions = (sourceId: string) =>
    tables
      .filter((tbl) => tbl.sourceId === sourceId)
      .map((tbl) => ({
        value: `${tbl.schemaName}.${tbl.tableName}`,
        label: `${tbl.schemaName}.${tbl.tableName}${tbl.alias ? ` (${tbl.alias})` : ""}`,
      }));

  const virtualTableOptions = tables.map((tbl) => ({
    value: physicalRelationName(tbl),
    label: `${physicalRelationName(tbl)}${tbl.alias ? ` (${tbl.alias})` : ""}`,
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
    setArgColumns(
      idx,
      (form.arguments[idx].columns ?? []).filter((_, i) => i !== ci),
    );

  // REQ-1159: canonical IR-typed output dataset contract.
  const addOutputColumn = () =>
    setForm({ ...form, outputColumns: [...form.outputColumns, { ...EMPTY_DATASET_COLUMN }] });
  const changeOutputColumn = (ci: number, field: "name" | "type", value: string) => {
    const cols = [...form.outputColumns];
    cols[ci] = { ...cols[ci], [field]: value };
    setForm({ ...form, outputColumns: cols });
  };
  // REQ-1679: per-column governance patch on an output column.
  const patchOutputColumn = (ci: number, patch: Partial<DatasetColumn>) => {
    const cols = [...form.outputColumns];
    cols[ci] = { ...cols[ci], ...patch };
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
  const handleInlineFieldChange = (idx: number, field: "name" | "type", value: string) => {
    const fields = [...form.inlineReturnType];
    fields[idx] = { ...fields[idx], [field]: value };
    setForm({ ...form, inlineReturnType: fields });
  };
  // REQ-1679: per-field governance patch on an inline return field.
  const patchInlineField = (idx: number, patch: Partial<InlineField>) => {
    const fields = [...form.inlineReturnType];
    fields[idx] = { ...fields[idx], ...patch };
    setForm({ ...form, inlineReturnType: fields });
  };

  const selectedSource = sources.find((s) => s.id === form.sourceId);
  // REQ-1924: a remote source offers write operations to pick; one picked is called as it is,
  // so its kind, arguments and answer follow from the operation, not from this form.
  // REQ-2131: a code source offers its @columns functions the same way, as column transforms.
  const operationSchema = selectedSource
    ? (OPERATION_SCHEMA[selectedSource.type] ?? CODE_SCHEMA[selectedSource.type])
    : undefined;
  const isSourceOperation =
    form.implKind === "source_operation" || form.implKind === "code_columns";
  // REQ-2001: a statement command's statement declares its kind, its written table and its
  // output, so the form asks for none of them.
  const isStatement = form.implKind === "statement";
  // REQ-2168: a SQL expression function is a query answering one value, its template its whole
  // definition, so the form asks for its arguments and its template alone.
  const isTemplate = form.implKind === "sql_expression";
  const declaresItsShape = isStatement || isTemplate;

  return (
    <>
      <FormLayout>
        {form.actionType === "function" && (
          <>
            <FormLayout>
              <FormField size="medium">
                <Select
                  label={t("commandFormFields.implementation")}
                  data={IMPL_KINDS}
                  value={form.implKind}
                  onChange={(val) => setForm({ ...form, implKind: val ?? "source_procedure" })}
                  allowDeselect={false}
                  data-testid="command-impl-kind-select"
                />
              </FormField>
              {(form.implKind === "source_procedure" || isSourceOperation) && (
                <>
                  <FormLayout>
                    <FormField size="medium">
                      <Select
                        label={t("commandFormFields.source")}
                        placeholder={t("commandFormFields.selectSource")}
                        required
                        data={sources.map((s) => ({ value: s.id, label: `${s.id} (${s.type})` }))}
                        value={form.sourceId || null}
                        onChange={(val) => {
                          const selectedSrc = sources.find((s) => s.id === val);
                          const schema = selectedSrc
                            ? OPERATION_SCHEMA[selectedSrc.type]
                            : undefined;
                          const code = selectedSrc ? CODE_SCHEMA[selectedSrc.type] : undefined;
                          setForm({
                            ...form,
                            sourceId: val ?? "",
                            implKind: schema
                              ? "source_operation"
                              : code
                                ? "code_columns"
                                : "source_procedure",
                            schemaName: schema ?? code ?? form.schemaName,
                            functionName: "",
                            ...(schema ? { kind: "mutation", arguments: [], returns: "" } : {}),
                            ...(code ? { kind: "query", arguments: [], returns: "" } : {}),
                          });
                        }}
                        data-testid="command-source-select"
                      />
                    </FormField>
                    <FormField size="medium">
                      <TextInput
                        label={t("commandFormFields.schema")}
                        value={form.schemaName}
                        onChange={(e) => setForm({ ...form, schemaName: e.currentTarget.value })}
                        readOnly={operationSchema !== undefined}
                        data-testid="command-schema-input"
                      />
                    </FormField>
                    {operationSchema !== undefined ? (
                      <FormField size="medium">
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
                      </FormField>
                    ) : (
                      // REQ-887: the schema's routines are offered to pick from; picking one registers
                      // nothing until the command is saved.
                      <FormField
                        hint={
                          availableFunctions.length > 0
                            ? t("commandFormFields.routinesOffered", {
                                count: availableFunctions.length,
                              })
                            : undefined
                        }
                        size="medium"
                      >
                        <Autocomplete
                          label={t("commandFormFields.functionName")}
                          required
                          data={availableFunctions.map((f) => f.name)}
                          value={form.functionName}
                          onChange={(val) => setForm({ ...form, functionName: val })}
                          placeholder={t("commandFormFields.dbFunctionNamePlaceholder")}
                          data-testid="command-function-input"
                        />
                      </FormField>
                    )}
                  </FormLayout>
                </>
              )}
              {form.implKind === "script" && (
                <FormField size="long">
                  <TextInput
                    label={t("commandFormFields.bindingArgv")}
                    required
                    value={bindingStr("argv")}
                    onChange={(e) =>
                      setBinding("argv", e.currentTarget.value.split(/\s+/).filter(Boolean))
                    }
                    placeholder="/usr/local/bin/transform --json"
                    data-testid="command-binding-argv"
                  />
                </FormField>
              )}
              {form.implKind === "http" && (
                <>
                  <FormLayout>
                    <FormField size="long">
                      <TextInput
                        label={t("commandFormFields.bindingUrl")}
                        required
                        value={bindingStr("url")}
                        onChange={(e) => setBinding("url", e.currentTarget.value)}
                        placeholder="https://svc.internal/fn"
                        data-testid="command-binding-url"
                      />
                    </FormField>
                    <FormField size="medium">
                      <Select
                        label={t("commandFormFields.method")}
                        data={["POST", "GET", "PUT", "PATCH"]}
                        value={bindingStr("method") || "POST"}
                        onChange={(val) => setBinding("method", val ?? "POST")}
                        allowDeselect={false}
                      />
                    </FormField>
                  </FormLayout>
                </>
              )}
              {form.implKind === "grpc" && (
                <>
                  <FormLayout>
                    <FormField size="long">
                      <TextInput
                        label={t("commandFormFields.bindingTarget")}
                        required
                        value={bindingStr("target")}
                        onChange={(e) => setBinding("target", e.currentTarget.value)}
                        placeholder="svc.internal:50051"
                        data-testid="command-binding-target"
                      />
                    </FormField>
                    <FormField size="medium">
                      <TextInput
                        label={t("commandFormFields.bindingGrpcMethod")}
                        required
                        value={bindingStr("method")}
                        onChange={(e) => setBinding("method", e.currentTarget.value)}
                        placeholder="pkg.Service.Method"
                        data-testid="command-binding-grpc-method"
                      />
                    </FormField>
                    <FormChecks>
                      <FormField size="short">
                        <Switch
                          label={t("commandFormFields.bindingTls")}
                          checked={!!form.binding.tls}
                          onChange={(e) => setBinding("tls", e.currentTarget.checked)}
                        />
                      </FormField>
                    </FormChecks>
                  </FormLayout>
                </>
              )}
              {isStatement && (
                <FormField
                  help={t("commandFormFields.bindingSqlHint", { placeholder: "{{name}}" })}
                  size="long"
                >
                  <Textarea
                    label={t("commandFormFields.bindingSql")}
                    required
                    autosize
                    minRows={4}
                    value={bindingStr("sql")}
                    onChange={(e) => setBinding("sql", e.currentTarget.value)}
                    placeholder="INSERT INTO kb.docs (id, title) SELECT id, subject FROM support.tickets WHERE updated_at > {{since}}"
                    data-testid="command-binding-sql"
                  />
                </FormField>
              )}
              {isTemplate && (
                <FormField
                  help={t("commandFormFields.bindingTemplateHint", { placeholder: "{name}" })}
                  size="long"
                >
                  <Textarea
                    label={t("commandFormFields.bindingTemplate")}
                    required
                    autosize
                    minRows={2}
                    value={bindingStr("sql")}
                    onChange={(e) => setBinding("sql", e.currentTarget.value)}
                    placeholder="array_to_string({list}, {delimiter})"
                    data-testid="command-binding-template"
                  />
                </FormField>
              )}
              {form.implKind === "python" && (
                <FormField size="long">
                  <TextInput
                    label={t("commandFormFields.bindingCallable")}
                    required
                    value={bindingStr("callable")}
                    onChange={(e) => setBinding("callable", e.currentTarget.value)}
                    placeholder="my_pkg.udfs:transform"
                    data-testid="command-binding-callable"
                  />
                </FormField>
              )}
              {!essential && (
                <>
                  <FormChecks>
                    {form.implKind !== "source_procedure" && !isSourceOperation && !declaresItsShape && (
                      <FormField help={t("commandFormFields.materializeHint")} size="long">
                        <Switch
                          label={t("commandFormFields.materialize")}
                          checked={form.materialize}
                          onChange={(e) =>
                            setForm({ ...form, materialize: e.currentTarget.checked })
                          }
                          data-testid="command-materialize-switch"
                        />
                      </FormField>
                    )}
                  </FormChecks>
                  {!isSourceOperation && !declaresItsShape && (
                    <FormField size="medium">
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
                    </FormField>
                  )}
                  {!isSourceOperation && !declaresItsShape && form.returnSchemaMode === "table" && (
                    <FormField size="medium">
                      <Select
                        label={t("commandFormFields.returnsTable")}
                        placeholder={t("commandFormFields.selectTable")}
                        data={physicalTableOptions(form.sourceId)}
                        value={form.returns || null}
                        onChange={(val) => setForm({ ...form, returns: val ?? "" })}
                        data-testid="command-returns-table-select"
                      />
                    </FormField>
                  )}
                  <FormField size="medium">
                    <TextInput
                      label={t("commandFormFields.visibleTo")}
                      value={form.visibleTo}
                      onChange={(e) => setForm({ ...form, visibleTo: e.currentTarget.value })}
                      placeholder={t("commandFormFields.visibleToPlaceholder")}
                    />
                  </FormField>
                </>
              )}
            </FormLayout>
            {/* REQ-1924, REQ-871: the table a call changes; what is held of it is refreshed after. */}
            {!essential && (
              <>
                {!declaresItsShape && (
                  <FormLayout>
                    <FormField help={t("commandFormFields.writesTableHint")} size="medium">
                      <Select
                        label={t("commandFormFields.writesTable")}
                        placeholder={t("commandFormFields.selectTable")}
                        data={physicalTableOptions(form.sourceId)}
                        value={form.writesTable || null}
                        onChange={(val) => setForm({ ...form, writesTable: val ?? "" })}
                        clearable
                        data-testid="command-writes-table-select"
                      />
                    </FormField>
                  </FormLayout>
                )}
                {/* REQ-1924: a call runs only once the deployment's approval hook approves it. */}
                <FormLayout>
                  <FormChecks>
                    <FormField help={t("commandFormFields.requiresApprovalHint")} size="short">
                      <Switch
                        label={t("commandFormFields.requiresApproval")}
                        checked={form.requiresApproval}
                        onChange={(e) =>
                          setForm({ ...form, requiresApproval: e.currentTarget.checked })
                        }
                        data-testid="command-requires-approval-switch"
                      />
                    </FormField>
                  </FormChecks>
                </FormLayout>
              </>
            )}
          </>
        )}
        {form.actionType === "webhook" && (
          <>
            <FormLayout>
              <FormField size="long">
                <TextInput
                  label={t("commandFormFields.url")}
                  required
                  value={form.url}
                  onChange={(e) => setForm({ ...form, url: e.currentTarget.value })}
                  placeholder={t("commandFormFields.urlPlaceholder")}
                  data-testid="command-url-input"
                />
              </FormField>
              {!essential && (
                <>
                  <FormField size="medium">
                    <Select
                      label={t("commandFormFields.method")}
                      data={["POST", "GET", "PUT", "PATCH"]}
                      value={form.method}
                      onChange={(val) => setForm({ ...form, method: val ?? "POST" })}
                      allowDeselect={false}
                      data-testid="command-method-select"
                    />
                  </FormField>
                  <FormField size="short">
                    <NumberInput
                      label={t("commandFormFields.timeoutMs")}
                      min={100}
                      value={form.timeoutMs}
                      onChange={(val) =>
                        setForm({ ...form, timeoutMs: typeof val === "number" ? val : 0 })
                      }
                    />
                  </FormField>
                  <FormField size="medium">
                    <Select
                      label={t("commandFormFields.returnsTableOptional")}
                      placeholder={t("commandFormFields.noneInlineType")}
                      data={virtualTableOptions}
                      value={form.returns || null}
                      onChange={(val) => setForm({ ...form, returns: val ?? "" })}
                    />
                  </FormField>
                  <FormField size="medium">
                    <TextInput
                      label={t("commandFormFields.visibleTo")}
                      value={form.visibleTo}
                      onChange={(e) => setForm({ ...form, visibleTo: e.currentTarget.value })}
                      placeholder={t("commandFormFields.visibleToPlaceholder")}
                    />
                  </FormField>
                </>
              )}
            </FormLayout>
            {/* REQ-209: a call is made only once the deployment's approval hook approves it. */}
            {!essential && (
              <>
                <FormLayout>
                  <FormChecks>
                    <FormField help={t("commandFormFields.requiresApprovalHint")} size="short">
                      <Switch
                        label={t("commandFormFields.requiresApproval")}
                        checked={form.requiresApproval}
                        onChange={(e) =>
                          setForm({ ...form, requiresApproval: e.currentTarget.checked })
                        }
                        data-testid="webhook-requires-approval-switch"
                      />
                    </FormField>
                  </FormChecks>
                  {!form.returns && (
                    // The response's fields: each an entry of its row and who sees it and how it is
                    // masked, set apart from the next.
                    <FormLayout data-testid="inline-return-type">
                      <Title order={5}>{t("commandFormFields.inlineReturnType")}</Title>
                      {form.inlineReturnType.map((f, i) => (
                        <FormEntry key={i}>
                          <FieldGroup row className="form-field-group--bare">
                            <FormField size="medium">
                              <TextInput
                                value={f.name}
                                onChange={(e) =>
                                  handleInlineFieldChange(i, "name", e.currentTarget.value)
                                }
                                placeholder={t("commandFormFields.fieldNamePlaceholder")}
                              />
                            </FormField>
                            <FormField size="medium">
                              <Select
                                value={f.type}
                                onChange={(val) =>
                                  handleInlineFieldChange(i, "type", val ?? "String")
                                }
                                data={GRAPHQL_TYPES}
                                allowDeselect={false}
                              />
                            </FormField>
                            <ActionIcon
                              variant="subtle"
                              color="red"
                              aria-label={t("commandFormFields.removeField", {
                                name: f.name || i + 1,
                              })}
                              onClick={() => handleRemoveInlineField(i)}
                            >
                              <X size={14} />
                            </ActionIcon>
                          </FieldGroup>
                          {/* REQ-1679: the webhook's response is governed like a table's rows. */}
                          <ColumnGovernanceFields
                            col={f}
                            onChange={(patch) => patchInlineField(i, patch)}
                            testId={`inline-field-governance-${i}`}
                            roleOptions={roleOptions}
                          />
                        </FormEntry>
                      ))}
                      <FormField size="full">
                        <Button variant="subtle" size="xs" onClick={handleAddInlineField}>
                          {t("commandFormFields.addField")}
                        </Button>
                      </FormField>
                    </FormLayout>
                  )}
                </FormLayout>
              </>
            )}
          </>
        )}
        {/* A command needs its domain in order to exist, so the short step asks it too. */}
        <FormField size="medium">
          <Select
            label={t("commandFormFields.domain")}
            placeholder={t("commandFormFields.selectDomain")}
            data={assignableDomainIds(domainHints)}
            value={form.domainId || null}
            // REQ-1531: every command and webhook sits in a domain, so one that is chosen cannot
            // be cleared — only replaced. Saving with none is refused by the server.
            allowDeselect={false}
            withAsterisk
            onChange={(val) => setForm({ ...form, domainId: val ?? form.domainId })}
            data-testid="command-domain-select"
          />
        </FormField>
        {!essential && (
          <>
            {!isSourceOperation && !declaresItsShape && (
              <FormField size="medium">
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
              </FormField>
            )}
            {form.actionType === "function" && (
              <FormField size="medium">
                <Select
                  label={t("commandFormFields.dataProduct")}
                  placeholder={t("commandFormFields.dataProductPlaceholder")}
                  // REQ-1634: a command may only join a data product owned by its own domain.
                  data={dataProducts
                    .filter((dp) => dp.domainId === form.domainId)
                    .map((dp) => ({ value: dp.id, label: dp.name }))}
                  value={form.productId || null}
                  onChange={(val) => setForm({ ...form, productId: val ?? "" })}
                  disabled={!form.domainId}
                  clearable
                  searchable
                  data-testid="command-data-product-select"
                />
              </FormField>
            )}
            <FormField size="long">
              <TextInput
                label={t("commandFormFields.description")}
                value={form.description}
                onChange={(e) => setForm({ ...form, description: e.currentTarget.value })}
                placeholder={t("commandFormFields.descriptionPlaceholder")}
              />
            </FormField>
            {!isSourceOperation && (
              // The arguments: one row each (its boxes carry no label, so the remove sits level
              // with them); an argument that takes a dataset is an entry of its row and the
              // dataset's columns, set apart from the next.
              <FormLayout data-testid="command-arguments">
                <Title order={5}>{t("commandFormFields.arguments")}</Title>
                {form.arguments.map((arg, i) => {
                  const row = (
                    <FieldGroup row className="form-field-group--bare">
                      <FormField size="medium">
                        <TextInput
                          value={arg.name}
                          onChange={(e) => handleArgChange(i, "name", e.currentTarget.value)}
                          placeholder={t("commandFormFields.argNamePlaceholder")}
                        />
                      </FormField>
                      <FormField size="medium">
                        <Select
                          value={arg.type}
                          onChange={(val) => handleArgChange(i, "type", val ?? "String")}
                          data={GRAPHQL_TYPES}
                          allowDeselect={false}
                        />
                      </FormField>
                      {form.actionType === "function" &&
                        form.implKind !== "source_procedure" &&
                        !declaresItsShape && (
                          <FormField size="medium">
                            <Select
                              aria-label={t("commandFormFields.argKind")}
                              value={arg.argKind ?? "column_value"}
                              onChange={(val) =>
                                handleArgChange(i, "argKind", val ?? "column_value")
                              }
                              data={ARG_KINDS}
                              allowDeselect={false}
                              data-testid={`command-arg-kind-${i}`}
                            />
                          </FormField>
                        )}
                      <ActionIcon
                        variant="subtle"
                        color="red"
                        aria-label={t("commandFormFields.removeArgument", {
                          name: arg.name || i + 1,
                        })}
                        onClick={() => handleRemoveArg(i)}
                      >
                        <X size={14} />
                      </ActionIcon>
                    </FieldGroup>
                  );
                  // REQ-1159: a dataset arg (table_ref/result_set) carries an IR-typed column contract.
                  if (
                    !(
                      form.actionType === "function" &&
                      DATASET_ARG_KINDS.has(arg.argKind ?? "column_value")
                    )
                  )
                    return <React.Fragment key={i}>{row}</React.Fragment>;
                  return (
                    <FormEntry key={i}>
                      {row}
                      <FormLayout
                        style={{ paddingInlineStart: "var(--mantine-spacing-md)" }}
                        data-testid={`dataset-columns-${i}`}
                      >
                        <Title order={6}>{t("commandFormFields.inputColumnsTitle")}</Title>
                        {(arg.columns ?? []).map((col, ci) => (
                          <FieldGroup row className="form-field-group--bare" key={ci}>
                            <FormField size="medium">
                              <TextInput
                                value={col.name}
                                onChange={(e) =>
                                  changeArgColumn(i, ci, "name", e.currentTarget.value)
                                }
                                placeholder={t("commandFormFields.datasetColumnPlaceholder")}
                              />
                            </FormField>
                            <FormField size="medium">
                              <Select
                                value={col.type}
                                onChange={(val) => changeArgColumn(i, ci, "type", val ?? "text")}
                                data={IR_TYPES}
                                allowDeselect={false}
                              />
                            </FormField>
                            <ActionIcon
                              variant="subtle"
                              color="red"
                              aria-label={t("commandFormFields.removeInputColumn", {
                                name: col.name || ci + 1,
                              })}
                              onClick={() => removeArgColumn(i, ci)}
                            >
                              <X size={14} />
                            </ActionIcon>
                          </FieldGroup>
                        ))}
                        <FormField size="full">
                          <Button variant="subtle" size="xs" onClick={() => addArgColumn(i)}>
                            {t("commandFormFields.addInputColumn")}
                          </Button>
                        </FormField>
                      </FormLayout>
                    </FormEntry>
                  );
                })}
                <FormField size="full">
                  <Button variant="subtle" size="xs" onClick={handleAddArg}>
                    {t("commandFormFields.addArgument")}
                  </Button>
                </FormField>
              </FormLayout>
            )}
          </>
        )}
      </FormLayout>
      {/* REQ-1159: canonical IR-typed output dataset contract (returnSchema is its GraphQL projection). */}
      {!essential && (
        <>
          <FormLayout>
            {form.actionType === "function" &&
              form.implKind !== "source_procedure" &&
              !isSourceOperation &&
              !declaresItsShape &&
              form.returnSchemaMode === "dataset" && (
                // The output dataset's columns: each an entry of its row and who sees it and how it
                // is masked, set apart from the next.
                <FormLayout data-testid="output-columns">
                  <Title order={5}>{t("commandFormFields.outputColumnsTitle")}</Title>
                  {form.outputColumns.map((col, ci) => (
                    <FormEntry key={ci} data-testid={`output-column-${ci}`}>
                      {/* Its boxes carry no label, so the remove sits level with them. */}
                      <FieldGroup row className="form-field-group--bare">
                        <FormField size="medium">
                          <TextInput
                            value={col.name}
                            onChange={(e) => changeOutputColumn(ci, "name", e.currentTarget.value)}
                            placeholder={t("commandFormFields.datasetColumnPlaceholder")}
                          />
                        </FormField>
                        <FormField size="medium">
                          <Select
                            value={col.type}
                            onChange={(val) => changeOutputColumn(ci, "type", val ?? "text")}
                            data={IR_TYPES}
                            allowDeselect={false}
                          />
                        </FormField>
                        <ActionIcon
                          variant="subtle"
                          color="red"
                          aria-label={t("commandFormFields.removeOutputColumn", {
                            name: col.name || ci + 1,
                          })}
                          onClick={() => removeOutputColumn(ci)}
                        >
                          <X size={14} />
                        </ActionIcon>
                      </FieldGroup>
                      {/* REQ-1679: the function's response is governed like a table's rows. */}
                      <ColumnGovernanceFields
                        col={col}
                        onChange={(patch) => patchOutputColumn(ci, patch)}
                        testId={`output-column-governance-${ci}`}
                        roleOptions={roleOptions}
                      />
                    </FormEntry>
                  ))}
                  <FormField size="full">
                    <Button variant="subtle" size="xs" onClick={addOutputColumn}>
                      {t("commandFormFields.addOutputColumn")}
                    </Button>
                  </FormField>
                </FormLayout>
              )}
          </FormLayout>
        </>
      )}
    </>
  );
}
