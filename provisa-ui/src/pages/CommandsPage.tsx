// Copyright (c) 2026 Kenneth Stott
// Canary: b12f5409-a0a1-4db6-b8de-245a25bd1768
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState, useEffect, useCallback } from "react";
import {
  fetchActions,
  saveFunction,
  saveWebhook,
  deleteFunction,
  deleteWebhook,
  testAction,
} from "../api/actions";
import type { TrackedFunction, TrackedWebhook, ActionArg, InlineField } from "../api/actions";
import { fetchSources, fetchTables, fetchDomains, fetchAvailableFunctions } from "../api/admin";
import type { TableMetadata } from "../api/admin";
import type { Source, RegisteredTable } from "../types/admin";
import { ConfirmDialog } from "../components/ConfirmDialog";

const GRAPHQL_TYPES = ["String", "Int", "Float", "Boolean", "DateTime", "Date", "BigInt", "JSON"];

const EMPTY_ARG: ActionArg = { name: "", type: "String" };
const EMPTY_INLINE: InlineField = { name: "", type: "String" };

type ActionType = "function" | "webhook";

interface FormState {
  actionType: ActionType;
  name: string;
  sourceId: string;
  schemaName: string;
  functionName: string;
  returns: string;
  visibleTo: string;
  writablBy: string;
  domainId: string;
  description: string;
  arguments: ActionArg[];
  url: string;
  method: string;
  timeoutMs: number;
  inlineReturnType: InlineField[];
  kind: string;
  returnSchemaMode: "table" | "custom";
  sampleJson: string;
  returnSchemaStr: string;
}

const EMPTY_FORM: FormState = {
  actionType: "function",
  name: "",
  sourceId: "",
  schemaName: "public",
  functionName: "",
  returns: "",
  visibleTo: "",
  writablBy: "",
  domainId: "",
  description: "",
  arguments: [],
  url: "",
  method: "POST",
  timeoutMs: 5000,
  inlineReturnType: [],
  kind: "mutation",
  returnSchemaMode: "table",
  sampleJson: "",
  returnSchemaStr: "",
};

function inferJsonSchema(jsonStr: string): string {
  try {
    const obj = JSON.parse(jsonStr);
    const sample = Array.isArray(obj) ? obj[0] : obj;
    if (!sample || typeof sample !== "object") return "";
    const props: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(sample)) {
      const t = typeof v;
      props[k] = { type: t === "number" ? (Number.isInteger(v as number) ? "integer" : "number") : t === "boolean" ? "boolean" : "string" };
    }
    return JSON.stringify({
      type: Array.isArray(obj) ? "array" : "object",
      ...(Array.isArray(obj) ? { items: { type: "object", properties: props } } : { properties: props }),
    }, null, 2);
  } catch {
    return "";
  }
}

export function CommandsPage() {
  const [functions, setFunctions] = useState<TrackedFunction[]>([]);
  const [webhooks, setWebhooks] = useState<TrackedWebhook[]>([]);
  const [sources, setSources] = useState<Source[]>([]);
  const [tables, setTables] = useState<RegisteredTable[]>([]);
  const [domainHints, setDomainHints] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [msg, setMsg] = useState("");
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState<FormState>({ ...EMPTY_FORM });
  const [editingName, setEditingName] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [testResult, setTestResult] = useState<{ name: string; data: unknown } | null>(null);
  const [testing, setTesting] = useState<string | null>(null);
  const [expandedFn, setExpandedFn] = useState<string | null>(null);
  const [expandedWh, setExpandedWh] = useState<string | null>(null);
  const [availableFunctions, setAvailableFunctions] = useState<TableMetadata[]>([]);
  const [loadingFunctions, setLoadingFunctions] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [fns, whs, srcs, tbls, doms] = await Promise.all([
        fetchActions().then((a) => a.functions),
        fetchActions().then((a) => a.webhooks),
        fetchSources(),
        fetchTables(),
        fetchDomains(),
      ]);
      setFunctions(fns);
      setWebhooks(whs);
      setSources(srcs);
      setTables(tbls);
      setDomainHints(doms.map((d) => d.id));
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  useEffect(() => {
    setAvailableFunctions([]);
    const src = sources.find((s) => s.id === form.sourceId);
    if (!src || src.type !== "openapi") return;
    setLoadingFunctions(true);
    fetchAvailableFunctions(form.sourceId)
      .then(setAvailableFunctions)
      .catch(() => setAvailableFunctions([]))
      .finally(() => setLoadingFunctions(false));
  }, [form.sourceId, sources]);

  // Physical options: schema.table — used for DB functions (returns within the source)
  const physicalTableOptions = (sourceId: string) =>
    tables
      .filter((t) => t.sourceId === sourceId)
      .map((t) => ({
        value: `${t.schemaName}.${t.tableName}`,
        label: `${t.schemaName}.${t.tableName}${t.alias ? ` (${t.alias})` : ""}`,
      }));

  // Virtual options: normalized_source.schema.table — used for webhooks
  const normalizePart = (s: string) => s.replace(/[^a-zA-Z0-9]/g, "_").replace(/^_+|_+$/g, "");
  const virtualTableOptions = tables.map((t) => ({
    value: `${normalizePart(t.sourceId)}.${normalizePart(t.schemaName)}.${t.tableName}`,
    label: `${normalizePart(t.sourceId)}.${normalizePart(t.schemaName)}.${t.tableName}${t.alias ? ` (${t.alias})` : ""}`,
  }));

  const handleAddArg = () => setForm({ ...form, arguments: [...form.arguments, { ...EMPTY_ARG }] });
  const handleRemoveArg = (idx: number) => setForm({ ...form, arguments: form.arguments.filter((_, i) => i !== idx) });
  const handleArgChange = (idx: number, field: keyof ActionArg, value: string) => {
    const args = [...form.arguments];
    args[idx] = { ...args[idx], [field]: value };
    setForm({ ...form, arguments: args });
  };
  const handleAddInlineField = () => setForm({ ...form, inlineReturnType: [...form.inlineReturnType, { ...EMPTY_INLINE }] });
  const handleRemoveInlineField = (idx: number) => setForm({ ...form, inlineReturnType: form.inlineReturnType.filter((_, i) => i !== idx) });
  const handleInlineFieldChange = (idx: number, field: keyof InlineField, value: string) => {
    const fields = [...form.inlineReturnType];
    fields[idx] = { ...fields[idx], [field]: value };
    setForm({ ...form, inlineReturnType: fields });
  };

  const handleEdit = (actionType: ActionType, name: string) => {
    if (actionType === "function") {
      const fn = functions.find((f) => f.name === name);
      if (!fn) return;
      const hasCustomSchema = !!fn.returnSchema && !fn.returns;
      setForm({
        actionType: "function",
        name: fn.name,
        sourceId: fn.sourceId,
        schemaName: fn.schemaName,
        functionName: fn.functionName,
        returns: fn.returns,
        visibleTo: fn.visibleTo.join(", "),
        writablBy: fn.writableBy.join(", "),
        domainId: fn.domainId,
        description: fn.description ?? "",
        arguments: fn.arguments.length > 0 ? fn.arguments : [],
        url: "",
        method: "POST",
        timeoutMs: 5000,
        inlineReturnType: [],
        kind: fn.kind ?? "mutation",
        returnSchemaMode: hasCustomSchema ? "custom" : "table",
        sampleJson: "",
        returnSchemaStr: hasCustomSchema ? JSON.stringify(fn.returnSchema, null, 2) : "",
      });
      setExpandedFn(name);
    } else {
      const wh = webhooks.find((w) => w.name === name);
      if (!wh) return;
      setForm({
        actionType: "webhook",
        name: wh.name,
        sourceId: "",
        schemaName: "public",
        functionName: "",
        returns: wh.returns ?? "",
        visibleTo: wh.visibleTo.join(", "),
        writablBy: "",
        domainId: wh.domainId,
        description: wh.description ?? "",
        arguments: wh.arguments.length > 0 ? wh.arguments : [],
        url: wh.url,
        method: wh.method,
        timeoutMs: wh.timeoutMs,
        inlineReturnType: wh.inlineReturnType.length > 0 ? wh.inlineReturnType : [],
        kind: wh.kind ?? "mutation",
      });
      setExpandedWh(name);
    }
    setEditingName(name);
    setShowForm(false);
  };

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    setError("");
    setMsg("");
    try {
      const visibleTo = form.visibleTo.split(",").map((s) => s.trim()).filter(Boolean);
      if (form.actionType === "function") {
        const writableBy = form.writablBy.split(",").map((s) => s.trim()).filter(Boolean);
        let returnSchema: Record<string, unknown> | null = null;
        if (form.returnSchemaMode === "custom" && form.returnSchemaStr) {
          try { returnSchema = JSON.parse(form.returnSchemaStr); } catch { /* leave null */ }
        }
        await saveFunction({
          name: form.name,
          sourceId: form.sourceId,
          schemaName: form.schemaName,
          functionName: form.functionName,
          returns: form.returnSchemaMode === "custom" ? "" : form.returns,
          arguments: form.arguments,
          visibleTo,
          writableBy,
          domainId: form.domainId,
          description: form.description || undefined,
          kind: form.kind,
          returnSchema,
        });
      } else {
        await saveWebhook({
          name: form.name,
          url: form.url,
          method: form.method,
          timeoutMs: form.timeoutMs,
          returns: form.returns || undefined,
          inlineReturnType: form.inlineReturnType,
          arguments: form.arguments,
          visibleTo,
          domainId: form.domainId,
          description: form.description || undefined,
          kind: form.kind,
        });
      }
      setMsg(`Saved ${form.actionType} "${form.name}"`);
      setShowForm(false);
      setForm({ ...EMPTY_FORM });
      setEditingName(null);
      load();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  };

  const handleTest = async (actionType: ActionType, name: string) => {
    setTesting(name);
    setTestResult(null);
    setError("");
    try {
      const result = await testAction(actionType, name);
      setTestResult({ name, data: result });
    } catch (e: any) {
      setError(`Test failed: ${e.message}`);
    } finally {
      setTesting(null);
    }
  };

  const handleCancel = () => {
    setShowForm(false);
    setForm({ ...EMPTY_FORM });
    setEditingName(null);
  };

  const renderFormFields = () => (
    <>
      {form.actionType === "function" && (
        <>
          <label>Source
            <select required value={form.sourceId} onChange={(e) => {
              const selectedSrc = sources.find((s) => s.id === e.target.value);
              setForm({ ...form, sourceId: e.target.value, schemaName: selectedSrc?.type === "openapi" ? "openapi" : form.schemaName, functionName: "" });
            }}>
              <option value="">Select source...</option>
              {sources.map((s) => <option key={s.id} value={s.id}>{s.id} ({s.type})</option>)}
            </select>
          </label>
          <label>Schema
            <input
              value={form.schemaName}
              onChange={(e) => setForm({ ...form, schemaName: e.target.value })}
              readOnly={sources.find((s) => s.id === form.sourceId)?.type === "openapi"}
            />
          </label>
          <label>Function Name
            {sources.find((s) => s.id === form.sourceId)?.type === "openapi" ? (
              <select
                required
                value={form.functionName}
                onChange={(e) => setForm({ ...form, functionName: e.target.value })}
                disabled={loadingFunctions}
              >
                <option value="">{loadingFunctions ? "Loading..." : "Select operation..."}</option>
                {availableFunctions.map((f) => (
                  <option key={f.name} value={f.name} title={f.comment ?? undefined}>{f.name}{f.comment ? ` — ${f.comment}` : ""}</option>
                ))}
              </select>
            ) : (
              <input required value={form.functionName} onChange={(e) => setForm({ ...form, functionName: e.target.value })} placeholder="DB function name" />
            )}
          </label>
          <label>Return Type
            <select value={form.returnSchemaMode} onChange={(e) => setForm({ ...form, returnSchemaMode: e.target.value as "table" | "custom", returns: "", returnSchemaStr: "", sampleJson: "" })}>
              <option value="table">Registered Table</option>
              <option value="custom">Custom Schema</option>
            </select>
          </label>
          {form.returnSchemaMode === "table" ? (
            <label>Returns (table)
              <select value={form.returns} onChange={(e) => setForm({ ...form, returns: e.target.value })}>
                <option value="">Select table...</option>
                {physicalTableOptions(form.sourceId).map((t) => <option key={t.value} value={t.value}>{t.label}</option>)}
              </select>
            </label>
          ) : (
            <div style={{ gridColumn: "1 / -1" }}>
              <label>Sample JSON (paste a sample row or array to infer schema)
                <textarea
                  rows={4}
                  value={form.sampleJson}
                  onChange={(e) => {
                    const inferred = inferJsonSchema(e.target.value);
                    setForm({ ...form, sampleJson: e.target.value, returnSchemaStr: inferred || form.returnSchemaStr });
                  }}
                  placeholder={'[{"id": 1, "name": "foo"}]'}
                  style={{ fontFamily: "monospace", fontSize: "0.85rem", resize: "vertical" }}
                />
              </label>
              <label>JSON Schema (edit as needed)
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
          <label>Visible To (roles, comma-separated)
            <input value={form.visibleTo} onChange={(e) => setForm({ ...form, visibleTo: e.target.value })} placeholder="admin, analyst" />
          </label>
          <label>Writable By (roles, comma-separated)
            <input value={form.writablBy} onChange={(e) => setForm({ ...form, writablBy: e.target.value })} placeholder="admin" />
          </label>
        </>
      )}
      {form.actionType === "webhook" && (
        <>
          <label>URL
            <input required value={form.url} onChange={(e) => setForm({ ...form, url: e.target.value })} placeholder="https://api.example.com/action" />
          </label>
          <label>Method
            <select value={form.method} onChange={(e) => setForm({ ...form, method: e.target.value })}>
              <option value="POST">POST</option>
              <option value="GET">GET</option>
              <option value="PUT">PUT</option>
              <option value="PATCH">PATCH</option>
            </select>
          </label>
          <label>Timeout (ms)
            <input type="number" min={100} value={form.timeoutMs} onChange={(e) => setForm({ ...form, timeoutMs: +e.target.value })} />
          </label>
          <label>Returns (table, optional)
            <select value={form.returns} onChange={(e) => setForm({ ...form, returns: e.target.value })}>
              <option value="">None (use inline type)</option>
              {virtualTableOptions.map((t) => <option key={t.value} value={t.value}>{t.label}</option>)}
            </select>
          </label>
          <label>Visible To (roles, comma-separated)
            <input value={form.visibleTo} onChange={(e) => setForm({ ...form, visibleTo: e.target.value })} placeholder="admin, analyst" />
          </label>
          {!form.returns && (
            <div style={{ gridColumn: "1 / -1" }}>
              <h4 style={{ marginBottom: "0.5rem" }}>Inline Return Type</h4>
              {form.inlineReturnType.map((f, i) => (
                <div key={i} style={{ display: "flex", gap: "0.5rem", marginBottom: "0.25rem", alignItems: "center" }}>
                  <input value={f.name} onChange={(e) => handleInlineFieldChange(i, "name", e.target.value)} placeholder="Field name" style={{ flex: 1, minWidth: 0 }} />
                  <select value={f.type} onChange={(e) => handleInlineFieldChange(i, "type", e.target.value)} style={{ flex: "0 0 auto", width: "120px" }}>
                    {GRAPHQL_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
                  </select>
                  <button type="button" className="destructive" onClick={() => handleRemoveInlineField(i)} style={{ padding: "0.25rem 0.5rem" }}>X</button>
                </div>
              ))}
              <button type="button" onClick={handleAddInlineField} style={{ fontSize: "0.85rem", marginTop: "0.25rem" }}>+ Add Field</button>
            </div>
          )}
        </>
      )}
      <label>Kind
        <select value={form.kind} onChange={(e) => setForm({ ...form, kind: e.target.value })}>
          <option value="mutation">Mutation</option>
          <option value="query">Query</option>
        </select>
      </label>
      <label>Domain
        <select value={form.domainId} onChange={(e) => setForm({ ...form, domainId: e.target.value })}>
          <option value="">Select domain...</option>
          {domainHints.map((d) => <option key={d} value={d}>{d}</option>)}
        </select>
      </label>
      <label>Description
        <input value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} placeholder="optional" />
      </label>
      <div style={{ gridColumn: "1 / -1" }}>
        <h4 style={{ marginBottom: "0.5rem" }}>Arguments</h4>
        {form.arguments.map((arg, i) => (
          <div key={i} style={{ display: "flex", gap: "0.5rem", marginBottom: "0.25rem", alignItems: "center" }}>
            <input value={arg.name} onChange={(e) => handleArgChange(i, "name", e.target.value)} placeholder="Arg name" style={{ flex: 1, minWidth: 0 }} />
            <select value={arg.type} onChange={(e) => handleArgChange(i, "type", e.target.value)} style={{ flex: "0 0 auto", width: "120px" }}>
              {GRAPHQL_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
            </select>
            <button type="button" className="destructive" onClick={() => handleRemoveArg(i)} style={{ padding: "0.25rem 0.5rem" }}>X</button>
          </div>
        ))}
        <button type="button" onClick={handleAddArg} style={{ fontSize: "0.85rem", marginTop: "0.25rem" }}>+ Add Argument</button>
      </div>
    </>
  );

  if (loading) return <div className="page">Loading commands...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Commands</h2>
        {!editingName && (
          <button onClick={() => { setShowForm(!showForm); if (showForm) handleCancel(); }}>
            {showForm ? "Cancel" : "Add Command"}
          </button>
        )}
      </div>

      {error && <div className="error">{error}</div>}
      {msg && <div className="success">{msg}</div>}

      {showForm && !editingName && (
        <form className="form-card" onSubmit={handleSave}>
          <label>Type
            <select value={form.actionType} onChange={(e) => setForm({ ...EMPTY_FORM, actionType: e.target.value as ActionType })}>
              <option value="function">DB Function</option>
              <option value="webhook">Webhook</option>
            </select>
          </label>
          <label>Name
            <input required value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} placeholder="e.g. process_order" />
          </label>
          {renderFormFields()}
          <button type="submit" disabled={saving}>{saving ? "Saving..." : "Create"}</button>
        </form>
      )}

      <h3 style={{ marginTop: "1.5rem", marginBottom: "0.5rem" }}>DB Functions</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Source</th>
            <th>Function</th>
            <th>Returns</th>
            <th>Args</th>
            <th>Visible To</th>
          </tr>
        </thead>
        <tbody>
          {functions.length === 0 && (
            <tr><td colSpan={6} style={{ color: "var(--text-muted)", textAlign: "center" }}>No functions registered</td></tr>
          )}
          {functions.map((fn) => {
            const isExpanded = expandedFn === fn.name;
            const isEditing = editingName === fn.name;
            return (
              <React.Fragment key={fn.name}>
                <tr
                  onClick={() => {
                    setExpandedFn(isExpanded ? null : fn.name);
                    if (isEditing) setEditingName(null);
                  }}
                  style={{ cursor: "pointer", background: isExpanded ? "var(--surface)" : undefined }}
                >
                  <td>{fn.name}</td>
                  <td>{fn.sourceId}</td>
                  <td>{fn.schemaName}.{fn.functionName}</td>
                  <td>{fn.returns || (fn.returnSchema ? "custom schema" : "—")}</td>
                  <td>{fn.arguments.length}</td>
                  <td>{fn.visibleTo.join(", ") || "all"}</td>
                </tr>
                {isExpanded && (
                  <tr>
                    <td colSpan={6} style={{ padding: "0.75rem 1rem", background: "var(--bg)", borderTop: "1px solid var(--border)" }}>
                      {isEditing ? (
                        <form className="form-card" onSubmit={handleSave} style={{ margin: 0 }}>
                          {renderFormFields()}
                          <div style={{ gridColumn: "1 / -1", display: "flex", gap: "0.5rem", justifyContent: "flex-end" }}>
                            <button type="button" className="btn-secondary" onClick={handleCancel}>Cancel</button>
                            <button type="submit" className="btn-primary" disabled={saving}>{saving ? "Saving..." : "Update"}</button>
                          </div>
                        </form>
                      ) : (
                        <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                          <dl style={{ display: "grid", gridTemplateColumns: "max-content 1fr", gap: "0.25rem 1rem", margin: 0, color: "var(--text)" }}>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Name</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{fn.name}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Kind</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{fn.kind ?? "mutation"}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Source</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{fn.sourceId}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Schema</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{fn.schemaName}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Function</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{fn.functionName}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Returns</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{fn.returns || (fn.returnSchema ? "custom schema" : "—")}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Visible To</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{fn.visibleTo.join(", ") || "all"}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Writable By</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{fn.writableBy.join(", ") || "all"}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Domain</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{fn.domainId || "—"}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Description</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{fn.description || "—"}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Arguments</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{fn.arguments.length === 0 ? "none" : fn.arguments.map((a) => `${a.name}: ${a.type}`).join(", ")}</dd>
                          </dl>
                          <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.25rem" }}>
                            <button
                              className="btn-secondary"
                              onClick={(e) => { e.stopPropagation(); handleEdit("function", fn.name); }}
                            >Edit</button>
                            <button
                              className="btn-secondary"
                              onClick={(e) => { e.stopPropagation(); handleTest("function", fn.name); }}
                              disabled={testing === fn.name}
                            >{testing === fn.name ? "Testing..." : "Test"}</button>
                            <ConfirmDialog
                              title={`Delete function "${fn.name}"?`}
                              consequence="This will remove the function from the schema."
                              onConfirm={async () => { await deleteFunction(fn.name); setExpandedFn(null); load(); }}
                            >
                              {(open) => (
                                <button className="btn-danger" onClick={(e) => { e.stopPropagation(); open(); }}>Delete</button>
                              )}
                            </ConfirmDialog>
                          </div>
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </React.Fragment>
            );
          })}
        </tbody>
      </table>

      <h3 style={{ marginTop: "1.5rem", marginBottom: "0.5rem" }}>Webhooks</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>URL</th>
            <th>Method</th>
            <th>Timeout</th>
            <th>Returns</th>
            <th>Args</th>
            <th>Visible To</th>
          </tr>
        </thead>
        <tbody>
          {webhooks.length === 0 && (
            <tr><td colSpan={7} style={{ color: "var(--text-muted)", textAlign: "center" }}>No webhooks registered</td></tr>
          )}
          {webhooks.map((wh) => {
            const isExpanded = expandedWh === wh.name;
            const isEditing = editingName === wh.name;
            return (
              <React.Fragment key={wh.name}>
                <tr
                  onClick={() => {
                    setExpandedWh(isExpanded ? null : wh.name);
                    if (isEditing) setEditingName(null);
                  }}
                  style={{ cursor: "pointer", background: isExpanded ? "var(--surface)" : undefined }}
                >
                  <td>{wh.name}</td>
                  <td style={{ maxWidth: "200px", overflow: "hidden", textOverflow: "ellipsis" }}>{wh.url}</td>
                  <td>{wh.method}</td>
                  <td>{wh.timeoutMs}ms</td>
                  <td>{wh.returns || `inline (${wh.inlineReturnType.length} fields)`}</td>
                  <td>{wh.arguments.length}</td>
                  <td>{wh.visibleTo.join(", ") || "all"}</td>
                </tr>
                {isExpanded && (
                  <tr>
                    <td colSpan={7} style={{ padding: "0.75rem 1rem", background: "var(--bg)", borderTop: "1px solid var(--border)" }}>
                      {isEditing ? (
                        <form className="form-card" onSubmit={handleSave} style={{ margin: 0 }}>
                          {renderFormFields()}
                          <div style={{ gridColumn: "1 / -1", display: "flex", gap: "0.5rem", justifyContent: "flex-end" }}>
                            <button type="button" className="btn-secondary" onClick={handleCancel}>Cancel</button>
                            <button type="submit" className="btn-primary" disabled={saving}>{saving ? "Saving..." : "Update"}</button>
                          </div>
                        </form>
                      ) : (
                        <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                          <dl style={{ display: "grid", gridTemplateColumns: "max-content 1fr", gap: "0.25rem 1rem", margin: 0, color: "var(--text)" }}>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Name</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{wh.name}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Kind</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{wh.kind ?? "mutation"}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>URL</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{wh.url}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Method</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{wh.method}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Timeout</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{wh.timeoutMs}ms</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Returns</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{wh.returns || `inline (${wh.inlineReturnType.length} fields)`}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Visible To</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{wh.visibleTo.join(", ") || "all"}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Domain</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{wh.domainId || "—"}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Description</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{wh.description || "—"}</dd>
                            <dt style={{ color: "var(--text-muted)" }}><strong>Arguments</strong></dt><dd style={{ color: "var(--text)", margin: 0 }}>{wh.arguments.length === 0 ? "none" : wh.arguments.map((a) => `${a.name}: ${a.type}`).join(", ")}</dd>
                            {wh.inlineReturnType.length > 0 && (
                              <>
                                <dt><strong>Inline Fields</strong></dt><dd>{wh.inlineReturnType.map((f) => `${f.name}: ${f.type}`).join(", ")}</dd>
                              </>
                            )}
                          </dl>
                          <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.25rem" }}>
                            <button
                              className="btn-secondary"
                              onClick={(e) => { e.stopPropagation(); handleEdit("webhook", wh.name); }}
                            >Edit</button>
                            <button
                              className="btn-secondary"
                              onClick={(e) => { e.stopPropagation(); handleTest("webhook", wh.name); }}
                              disabled={testing === wh.name}
                            >{testing === wh.name ? "Testing..." : "Test"}</button>
                            <ConfirmDialog
                              title={`Delete webhook "${wh.name}"?`}
                              consequence="This will remove the webhook from the schema."
                              onConfirm={async () => { await deleteWebhook(wh.name); setExpandedWh(null); load(); }}
                            >
                              {(open) => (
                                <button className="btn-danger" onClick={(e) => { e.stopPropagation(); open(); }}>Delete</button>
                              )}
                            </ConfirmDialog>
                          </div>
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </React.Fragment>
            );
          })}
        </tbody>
      </table>

      {testResult && (
        <div style={{ marginTop: "1rem", padding: "1rem", background: "var(--surface)", border: "1px solid var(--border)", borderRadius: "4px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "0.5rem" }}>
            <h4>Test Result: {testResult.name}</h4>
            <button onClick={() => setTestResult(null)} style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}>Close</button>
          </div>
          <pre style={{ fontSize: "0.85rem", overflow: "auto", maxHeight: "300px" }}>
            {JSON.stringify(testResult.data, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}
