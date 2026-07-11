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
import type { Source, RegisteredTable } from "../../types/admin";
import type { TableMetadata } from "../../api/admin";
import type { ActionArg, InlineField } from "../../api/actions";
import {
  GRAPHQL_TYPES,
  EMPTY_ARG,
  EMPTY_INLINE,
  inferJsonSchema,
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
  const physicalTableOptions = (sourceId: string) =>
    tables
      .filter((t) => t.sourceId === sourceId)
      .map((t) => ({
        value: `${t.schemaName}.${t.tableName}`,
        label: `${t.schemaName}.${t.tableName}${t.alias ? ` (${t.alias})` : ""}`,
      }));

  const normalizePart = (s: string) => s.replace(/[^a-zA-Z0-9]/g, "_").replace(/^_+|_+$/g, "");
  const virtualTableOptions = tables.map((t) => ({
    value: `${normalizePart(t.sourceId)}.${normalizePart(t.schemaName)}.${t.tableName}`,
    label: `${normalizePart(t.sourceId)}.${normalizePart(t.schemaName)}.${t.tableName}${t.alias ? ` (${t.alias})` : ""}`,
  }));

  const handleAddArg = () => setForm({ ...form, arguments: [...form.arguments, { ...EMPTY_ARG }] });
  const handleRemoveArg = (idx: number) =>
    setForm({ ...form, arguments: form.arguments.filter((_, i) => i !== idx) });
  const handleArgChange = (idx: number, field: keyof ActionArg, value: string) => {
    const args = [...form.arguments];
    args[idx] = { ...args[idx], [field]: value };
    setForm({ ...form, arguments: args });
  };
  const handleAddInlineField = () =>
    setForm({ ...form, inlineReturnType: [...form.inlineReturnType, { ...EMPTY_INLINE }] });
  const handleRemoveInlineField = (idx: number) =>
    setForm({ ...form, inlineReturnType: form.inlineReturnType.filter((_, i) => i !== idx) });
  const handleInlineFieldChange = (idx: number, field: keyof InlineField, value: string) => {
    const fields = [...form.inlineReturnType];
    fields[idx] = { ...fields[idx], [field]: value };
    setForm({ ...form, inlineReturnType: fields });
  };

  return (
    <>
      {form.actionType === "function" && (
        <>
          <label>
            Source
            <select
              required
              value={form.sourceId}
              onChange={(e) => {
                const selectedSrc = sources.find((s) => s.id === e.target.value);
                setForm({
                  ...form,
                  sourceId: e.target.value,
                  schemaName: selectedSrc?.type === "openapi" ? "openapi" : form.schemaName,
                  functionName: "",
                });
              }}
            >
              <option value="">Select source...</option>
              {sources.map((s) => (
                <option key={s.id} value={s.id}>
                  {s.id} ({s.type})
                </option>
              ))}
            </select>
          </label>
          <label>
            Schema
            <input
              value={form.schemaName}
              onChange={(e) => setForm({ ...form, schemaName: e.target.value })}
              readOnly={sources.find((s) => s.id === form.sourceId)?.type === "openapi"}
            />
          </label>
          <label>
            Function Name
            {sources.find((s) => s.id === form.sourceId)?.type === "openapi" ? (
              <select
                required
                value={form.functionName}
                onChange={(e) => setForm({ ...form, functionName: e.target.value })}
                disabled={loadingFunctions}
              >
                <option value="">{loadingFunctions ? "Loading..." : "Select operation..."}</option>
                {availableFunctions.map((f) => (
                  <option key={f.name} value={f.name} title={f.comment ?? undefined}>
                    {f.name}
                    {f.comment ? ` — ${f.comment}` : ""}
                  </option>
                ))}
              </select>
            ) : (
              <input
                required
                value={form.functionName}
                onChange={(e) => setForm({ ...form, functionName: e.target.value })}
                placeholder="DB function name"
              />
            )}
          </label>
          <label>
            Return Type
            <select
              value={form.returnSchemaMode}
              onChange={(e) =>
                setForm({
                  ...form,
                  returnSchemaMode: e.target.value as "table" | "custom",
                  returns: "",
                  returnSchemaStr: "",
                  sampleJson: "",
                })
              }
            >
              <option value="table">Registered Table</option>
              <option value="custom">Custom Schema</option>
            </select>
          </label>
          {form.returnSchemaMode === "table" ? (
            <label>
              Returns (table)
              <select
                value={form.returns}
                onChange={(e) => setForm({ ...form, returns: e.target.value })}
              >
                <option value="">Select table...</option>
                {physicalTableOptions(form.sourceId).map((t) => (
                  <option key={t.value} value={t.value}>
                    {t.label}
                  </option>
                ))}
              </select>
            </label>
          ) : (
            <div style={{ gridColumn: "1 / -1" }}>
              <label>
                Sample JSON (paste a sample row or array to infer schema)
                <textarea
                  rows={4}
                  value={form.sampleJson}
                  onChange={(e) => {
                    const inferred = inferJsonSchema(e.target.value);
                    setForm({
                      ...form,
                      sampleJson: e.target.value,
                      returnSchemaStr: inferred || form.returnSchemaStr,
                    });
                  }}
                  placeholder={'[{"id": 1, "name": "foo"}]'}
                  style={{ fontFamily: "monospace", fontSize: "0.85rem", resize: "vertical" }}
                />
              </label>
              <label>
                JSON Schema (edit as needed)
                <textarea
                  rows={8}
                  value={form.returnSchemaStr}
                  onChange={(e) => setForm({ ...form, returnSchemaStr: e.target.value })}
                  placeholder='{"type":"array","items":{"type":"object","properties":{"id":{"type":"integer"}}}}'
                  style={{ fontFamily: "monospace", fontSize: "0.85rem", resize: "vertical" }}
                />
              </label>
            </div>
          )}
          <label>
            Visible To (roles, comma-separated)
            <input
              value={form.visibleTo}
              onChange={(e) => setForm({ ...form, visibleTo: e.target.value })}
              placeholder="admin, analyst"
            />
          </label>
          <label>
            Writable By (roles, comma-separated)
            <input
              value={form.writablBy}
              onChange={(e) => setForm({ ...form, writablBy: e.target.value })}
              placeholder="admin"
            />
          </label>
        </>
      )}
      {form.actionType === "webhook" && (
        <>
          <label>
            URL
            <input
              required
              value={form.url}
              onChange={(e) => setForm({ ...form, url: e.target.value })}
              placeholder="https://api.example.com/action"
            />
          </label>
          <label>
            Method
            <select
              value={form.method}
              onChange={(e) => setForm({ ...form, method: e.target.value })}
            >
              <option value="POST">POST</option>
              <option value="GET">GET</option>
              <option value="PUT">PUT</option>
              <option value="PATCH">PATCH</option>
            </select>
          </label>
          <label>
            Timeout (ms)
            <input
              type="number"
              min={100}
              value={form.timeoutMs}
              onChange={(e) => setForm({ ...form, timeoutMs: +e.target.value })}
            />
          </label>
          <label>
            Returns (table, optional)
            <select
              value={form.returns}
              onChange={(e) => setForm({ ...form, returns: e.target.value })}
            >
              <option value="">None (use inline type)</option>
              {virtualTableOptions.map((t) => (
                <option key={t.value} value={t.value}>
                  {t.label}
                </option>
              ))}
            </select>
          </label>
          <label>
            Visible To (roles, comma-separated)
            <input
              value={form.visibleTo}
              onChange={(e) => setForm({ ...form, visibleTo: e.target.value })}
              placeholder="admin, analyst"
            />
          </label>
          {!form.returns && (
            <div style={{ gridColumn: "1 / -1" }}>
              <h4 style={{ marginBottom: "0.5rem" }}>Inline Return Type</h4>
              {form.inlineReturnType.map((f, i) => (
                <div
                  key={i}
                  style={{
                    display: "flex",
                    gap: "0.5rem",
                    marginBottom: "0.25rem",
                    alignItems: "center",
                  }}
                >
                  <input
                    value={f.name}
                    onChange={(e) => handleInlineFieldChange(i, "name", e.target.value)}
                    placeholder="Field name"
                    style={{ flex: 1, minWidth: 0 }}
                  />
                  <select
                    value={f.type}
                    onChange={(e) => handleInlineFieldChange(i, "type", e.target.value)}
                    style={{ flex: "0 0 auto", width: "120px" }}
                  >
                    {GRAPHQL_TYPES.map((t) => (
                      <option key={t} value={t}>
                        {t}
                      </option>
                    ))}
                  </select>
                  <button
                    type="button"
                    className="destructive"
                    onClick={() => handleRemoveInlineField(i)}
                    style={{ padding: "0.25rem 0.5rem" }}
                  >
                    X
                  </button>
                </div>
              ))}
              <button
                type="button"
                onClick={handleAddInlineField}
                style={{ fontSize: "0.85rem", marginTop: "0.25rem" }}
              >
                + Add Field
              </button>
            </div>
          )}
        </>
      )}
      <label>
        Kind
        <select value={form.kind} onChange={(e) => setForm({ ...form, kind: e.target.value })}>
          <option value="mutation">Mutation</option>
          <option value="query">Query</option>
        </select>
      </label>
      <label>
        Domain
        <select
          value={form.domainId}
          onChange={(e) => setForm({ ...form, domainId: e.target.value })}
        >
          <option value="">Select domain...</option>
          {domainHints.map((d) => (
            <option key={d} value={d}>
              {d}
            </option>
          ))}
        </select>
      </label>
      <label>
        Description
        <input
          value={form.description}
          onChange={(e) => setForm({ ...form, description: e.target.value })}
          placeholder="optional"
        />
      </label>
      <div style={{ gridColumn: "1 / -1" }}>
        <h4 style={{ marginBottom: "0.5rem" }}>Arguments</h4>
        {form.arguments.map((arg, i) => (
          <div
            key={i}
            style={{
              display: "flex",
              gap: "0.5rem",
              marginBottom: "0.25rem",
              alignItems: "center",
            }}
          >
            <input
              value={arg.name}
              onChange={(e) => handleArgChange(i, "name", e.target.value)}
              placeholder="Arg name"
              style={{ flex: 1, minWidth: 0 }}
            />
            <select
              value={arg.type}
              onChange={(e) => handleArgChange(i, "type", e.target.value)}
              style={{ flex: "0 0 auto", width: "120px" }}
            >
              {GRAPHQL_TYPES.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
            <button
              type="button"
              className="destructive"
              onClick={() => handleRemoveArg(i)}
              style={{ padding: "0.25rem 0.5rem" }}
            >
              X
            </button>
          </div>
        ))}
        <button
          type="button"
          onClick={handleAddArg}
          style={{ fontSize: "0.85rem", marginTop: "0.25rem" }}
        >
          + Add Argument
        </button>
      </div>
    </>
  );
}
