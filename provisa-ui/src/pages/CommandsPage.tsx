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
import { useAuth } from "../context/AuthContext";
import { Trash2, Pencil, Check, X } from "lucide-react";
import { FilterInput } from "../components/admin/FilterInput";
import {
  fetchActions,
  saveFunction,
  saveWebhook,
  deleteFunction,
  deleteWebhook,
  testAction,
} from "../api/actions";
import type { TrackedFunction, TrackedWebhook } from "../api/actions";
import {
  useSources,
  useTables,
  useDomains,
  useAvailableFunctionsLazy,
} from "../hooks/useAdminQueries";
import type { TableMetadata } from "../api/admin";
import { fetchOrgRoles } from "../api/admin";
import type { Role } from "../types/auth";
import { ConfirmDialog } from "../components/ConfirmDialog";
import type { ActionType, FormState } from "./commands/types";
import { EMPTY_FORM } from "./commands/types";
import { CommandFormFields } from "./commands/CommandFormFields";

export function CommandsPage() {
  const { sources } = useSources();
  const { tables } = useTables();
  const { domains } = useDomains();
  const getAvailableFunctions = useAvailableFunctionsLazy();
  const { activeOrgId } = useAuth();
  const orgId = activeOrgId ?? "root";
  const domainHints = domains.map((d) => d.id);
  const [functions, setFunctions] = useState<TrackedFunction[]>([]);
  const [webhooks, setWebhooks] = useState<TrackedWebhook[]>([]);
  const [roles, setRoles] = useState<Role[]>([]);
  const [testRoleId, setTestRoleId] = useState<string>("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [msg, setMsg] = useState("");
  const [cmdSearch, setCmdSearch] = useState("");
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState<FormState>({ ...EMPTY_FORM });
  const [editingName, setEditingName] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [testResult, setTestResult] = useState<{ name: string; data: unknown } | null>(null);
  const [testing, setTesting] = useState<string | null>(null);
  const [expandedFn, setExpandedFn] = useState<string | null>(null);
  const [expandedWh, setExpandedWh] = useState<string | null>(null);
  const [fnPage, setFnPage] = useState(0);
  const [whPage, setWhPage] = useState(0);
  const PAGE_SIZE = 50;
  const [availableFunctions, setAvailableFunctions] = useState<TableMetadata[]>([]);
  const [loadingFunctions, setLoadingFunctions] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const actions = await fetchActions();
      setFunctions(actions.functions);
      setWebhooks(actions.webhooks);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  /* eslint-disable react-hooks/set-state-in-effect -- mount data-fetch: load() sets loading state synchronously by design */
  useEffect(() => {
    load();
  }, [load]);
  /* eslint-enable react-hooks/set-state-in-effect */

  useEffect(() => {
    fetchOrgRoles(orgId).then(setRoles).catch(() => {});
  }, [orgId]);

  useEffect(() => {
    /* eslint-disable-next-line react-hooks/set-state-in-effect --
       resets the available-functions list synchronously when the selected source changes, before refetching */
    setAvailableFunctions([]);
    const src = sources.find((s) => s.id === form.sourceId);
    if (!src || src.type !== "openapi") return;
    setLoadingFunctions(true);
    getAvailableFunctions(form.sourceId)
      .then(setAvailableFunctions)
      .catch(() => setAvailableFunctions([]))
      .finally(() => setLoadingFunctions(false));
  }, [form.sourceId, sources, getAvailableFunctions]);

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
        returnSchemaMode: "table",
        sampleJson: "",
        returnSchemaStr: "",
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
      const visibleTo = form.visibleTo
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      if (form.actionType === "function") {
        const writableBy = form.writablBy
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);
        let returnSchema: Record<string, unknown> | null = null;
        if (form.returnSchemaMode === "custom" && form.returnSchemaStr) {
          try {
            returnSchema = JSON.parse(form.returnSchemaStr);
          } catch {
            /* leave null */
          }
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
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const handleTest = async (actionType: ActionType, name: string) => {
    setTesting(name);
    setTestResult(null);
    setError("");
    try {
      const result = await testAction(actionType, name, testRoleId || undefined);
      setTestResult({ name, data: result });
    } catch (e) {
      setError(`Test failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setTesting(null);
    }
  };

  const handleCancel = () => {
    setShowForm(false);
    setForm({ ...EMPTY_FORM });
    setEditingName(null);
  };

  const formFieldsProps = {
    form,
    setForm,
    sources,
    tables,
    domainHints,
    availableFunctions,
    loadingFunctions,
  };

  if (loading) return <div className="page">Loading commands...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Commands</h2>
        <FilterInput
          value={cmdSearch}
          onChange={(v) => {
            setCmdSearch(v);
            setFnPage(0);
            setWhPage(0);
          }}
          placeholder="Filter by name…"
        />
        <div className="page-actions">
          {!editingName && (
            <button
              onClick={() => {
                setShowForm(!showForm);
                if (showForm) handleCancel();
              }}
            >
              {showForm ? "✕" : "+ Command"}
            </button>
          )}
        </div>
      </div>

      {error && <div className="error">{error}</div>}
      {msg && <div className="success">{msg}</div>}

      {showForm && !editingName && (
        <form className="form-card" onSubmit={handleSave}>
          <label>
            Type
            <select
              value={form.actionType}
              onChange={(e) => setForm({ ...EMPTY_FORM, actionType: e.target.value as ActionType })}
            >
              <option value="function">DB Function</option>
              <option value="webhook">Webhook</option>
            </select>
          </label>
          <label>
            Name
            <input
              required
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              placeholder="e.g. process_order"
            />
          </label>
          <CommandFormFields {...formFieldsProps} />
          <button type="submit" disabled={saving}>
            {saving ? "Saving..." : "Create"}
          </button>
        </form>
      )}

      <h3 style={{ marginTop: "1.5rem", marginBottom: "0.5rem" }}>DB Functions</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Source</th>
            <th>Domain</th>
            <th>Function</th>
            <th>Returns</th>
            <th>Args</th>
            <th>Visible To</th>
          </tr>
        </thead>
        <tbody>
          {functions.length === 0 && (
            <tr>
              <td colSpan={7} style={{ color: "var(--text-muted)", textAlign: "center" }}>
                No functions registered
              </td>
            </tr>
          )}
          {(() => {
            const filtered = functions.filter(
              (fn) => !cmdSearch.trim() || fn.name.toLowerCase().includes(cmdSearch.toLowerCase()),
            );
            const paged = filtered.slice(fnPage * PAGE_SIZE, (fnPage + 1) * PAGE_SIZE);
            return paged.map((fn) => {
              const isExpanded = expandedFn === fn.name;
              const isEditing = editingName === fn.name;
              return (
                <React.Fragment key={fn.name}>
                  <tr
                    onClick={() => {
                      setExpandedFn(isExpanded ? null : fn.name);
                      if (isEditing) setEditingName(null);
                    }}
                    style={{
                      cursor: "pointer",
                      background: isExpanded ? "var(--surface)" : undefined,
                    }}
                  >
                    <td>{fn.name}</td>
                    <td>{fn.sourceId}</td>
                    <td>{fn.domainId || "—"}</td>
                    <td>
                      {fn.schemaName}.{fn.functionName}
                    </td>
                    <td>{fn.returns || (fn.returnSchema ? "custom schema" : "—")}</td>
                    <td>{fn.arguments.length}</td>
                    <td>{fn.visibleTo.join(", ") || "all"}</td>
                  </tr>
                  {isExpanded && (
                    <tr>
                      <td
                        colSpan={7}
                        style={{
                          padding: "0.75rem 1rem",
                          background: "var(--bg)",
                          borderTop: "1px solid var(--border)",
                        }}
                      >
                        {isEditing ? (
                          <form className="form-card" onSubmit={handleSave} style={{ margin: 0 }}>
                            <CommandFormFields {...formFieldsProps} />
                            <div
                              style={{
                                gridColumn: "1 / -1",
                                display: "flex",
                                gap: "0.5rem",
                                justifyContent: "flex-end",
                              }}
                            >
                              <button
                                type="button"
                                className="btn-icon"
                                title="Cancel"
                                onClick={handleCancel}
                              >
                                <X size={14} />
                              </button>
                              <button
                                type="submit"
                                className="btn-icon-primary"
                                title="Save"
                                disabled={saving}
                              >
                                <Check size={14} />
                              </button>
                            </div>
                          </form>
                        ) : (
                          <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                            <dl
                              style={{
                                display: "grid",
                                gridTemplateColumns: "max-content 1fr",
                                gap: "0.25rem 1rem",
                                margin: 0,
                                color: "var(--text)",
                              }}
                            >
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Name</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>{fn.name}</dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Kind</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>
                                {fn.kind ?? "mutation"}
                              </dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Source</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>{fn.sourceId}</dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Schema</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>{fn.schemaName}</dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Function</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>{fn.functionName}</dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Returns</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>
                                {fn.returns || (fn.returnSchema ? "custom schema" : "—")}
                              </dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Visible To</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>
                                {fn.visibleTo.join(", ") || "all"}
                              </dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Writable By</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>
                                {fn.writableBy.join(", ") || "all"}
                              </dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Domain</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>
                                {fn.domainId || "—"}
                              </dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Description</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>
                                {fn.description || "—"}
                              </dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Arguments</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>
                                {fn.arguments.length === 0
                                  ? "none"
                                  : fn.arguments.map((a) => `${a.name}: ${a.type}`).join(", ")}
                              </dd>
                            </dl>
                            <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.25rem" }}>
                              <button
                                className="btn-icon"
                                title="Edit"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleEdit("function", fn.name);
                                }}
                              >
                                <Pencil size={14} />
                              </button>
                              <button
                                className="btn-secondary"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleTest("function", fn.name);
                                }}
                                disabled={testing === fn.name}
                              >
                                {testing === fn.name ? "Testing..." : "Test"}
                              </button>
                              <ConfirmDialog
                                title={`Delete function "${fn.name}"?`}
                                consequence="This will remove the function from the schema."
                                onConfirm={async () => {
                                  await deleteFunction(fn.name);
                                  setExpandedFn(null);
                                  load();
                                }}
                              >
                                {(open) => (
                                  <button
                                    className="btn-icon-danger"
                                    title="Delete"
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      open();
                                    }}
                                  >
                                    <Trash2 size={14} />
                                  </button>
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
            });
          })()}
        </tbody>
      </table>
      {(() => {
        const filtered = functions.filter(
          (fn) => !cmdSearch.trim() || fn.name.toLowerCase().includes(cmdSearch.toLowerCase()),
        );
        const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
        if (totalPages === 1) return null;
        return (
          <div
            style={{
              display: "flex",
              gap: "0.5rem",
              alignItems: "center",
              justifyContent: "flex-end",
              padding: "0.5rem 0",
            }}
          >
            <button onClick={() => setFnPage(0)} disabled={fnPage === 0}>
              «
            </button>
            <button onClick={() => setFnPage((p) => p - 1)} disabled={fnPage === 0}>
              ‹
            </button>
            <span>
              Page {fnPage + 1} / {totalPages}
            </span>
            <button onClick={() => setFnPage((p) => p + 1)} disabled={fnPage >= totalPages - 1}>
              ›
            </button>
            <button onClick={() => setFnPage(totalPages - 1)} disabled={fnPage >= totalPages - 1}>
              »
            </button>
          </div>
        );
      })()}

      <h3 style={{ marginTop: "1.5rem", marginBottom: "0.5rem" }}>Webhooks</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Domain</th>
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
            <tr>
              <td colSpan={8} style={{ color: "var(--text-muted)", textAlign: "center" }}>
                No webhooks registered
              </td>
            </tr>
          )}
          {(() => {
            const filtered = webhooks.filter(
              (wh) => !cmdSearch.trim() || wh.name.toLowerCase().includes(cmdSearch.toLowerCase()),
            );
            const paged = filtered.slice(whPage * PAGE_SIZE, (whPage + 1) * PAGE_SIZE);
            return paged.map((wh) => {
              const isExpanded = expandedWh === wh.name;
              const isEditing = editingName === wh.name;
              return (
                <React.Fragment key={wh.name}>
                  <tr
                    onClick={() => {
                      setExpandedWh(isExpanded ? null : wh.name);
                      if (isEditing) setEditingName(null);
                    }}
                    style={{
                      cursor: "pointer",
                      background: isExpanded ? "var(--surface)" : undefined,
                    }}
                  >
                    <td>{wh.name}</td>
                    <td>{wh.domainId || "—"}</td>
                    <td style={{ maxWidth: "200px", overflow: "hidden", textOverflow: "ellipsis" }}>
                      {wh.url}
                    </td>
                    <td>{wh.method}</td>
                    <td>{wh.timeoutMs}ms</td>
                    <td>{wh.returns || `inline (${wh.inlineReturnType.length} fields)`}</td>
                    <td>{wh.arguments.length}</td>
                    <td>{wh.visibleTo.join(", ") || "all"}</td>
                  </tr>
                  {isExpanded && (
                    <tr>
                      <td
                        colSpan={8}
                        style={{
                          padding: "0.75rem 1rem",
                          background: "var(--bg)",
                          borderTop: "1px solid var(--border)",
                        }}
                      >
                        {isEditing ? (
                          <form className="form-card" onSubmit={handleSave} style={{ margin: 0 }}>
                            <CommandFormFields {...formFieldsProps} />
                            <div
                              style={{
                                gridColumn: "1 / -1",
                                display: "flex",
                                gap: "0.5rem",
                                justifyContent: "flex-end",
                              }}
                            >
                              <button
                                type="button"
                                className="btn-icon"
                                title="Cancel"
                                onClick={handleCancel}
                              >
                                <X size={14} />
                              </button>
                              <button
                                type="submit"
                                className="btn-icon-primary"
                                title="Save"
                                disabled={saving}
                              >
                                <Check size={14} />
                              </button>
                            </div>
                          </form>
                        ) : (
                          <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                            <dl
                              style={{
                                display: "grid",
                                gridTemplateColumns: "max-content 1fr",
                                gap: "0.25rem 1rem",
                                margin: 0,
                                color: "var(--text)",
                              }}
                            >
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Name</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>{wh.name}</dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Kind</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>
                                {wh.kind ?? "mutation"}
                              </dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>URL</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>{wh.url}</dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Method</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>{wh.method}</dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Timeout</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>{wh.timeoutMs}ms</dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Returns</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>
                                {wh.returns || `inline (${wh.inlineReturnType.length} fields)`}
                              </dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Visible To</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>
                                {wh.visibleTo.join(", ") || "all"}
                              </dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Domain</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>
                                {wh.domainId || "—"}
                              </dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Description</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>
                                {wh.description || "—"}
                              </dd>
                              <dt style={{ color: "var(--text-muted)" }}>
                                <strong>Arguments</strong>
                              </dt>
                              <dd style={{ color: "var(--text)", margin: 0 }}>
                                {wh.arguments.length === 0
                                  ? "none"
                                  : wh.arguments.map((a) => `${a.name}: ${a.type}`).join(", ")}
                              </dd>
                              {wh.inlineReturnType.length > 0 && (
                                <>
                                  <dt>
                                    <strong>Inline Fields</strong>
                                  </dt>
                                  <dd>
                                    {wh.inlineReturnType
                                      .map((f) => `${f.name}: ${f.type}`)
                                      .join(", ")}
                                  </dd>
                                </>
                              )}
                            </dl>
                            <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.25rem" }}>
                              <button
                                className="btn-icon"
                                title="Edit"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleEdit("webhook", wh.name);
                                }}
                              >
                                <Pencil size={14} />
                              </button>
                              <button
                                className="btn-secondary"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleTest("webhook", wh.name);
                                }}
                                disabled={testing === wh.name}
                              >
                                {testing === wh.name ? "Testing..." : "Test"}
                              </button>
                              <ConfirmDialog
                                title={`Delete webhook "${wh.name}"?`}
                                consequence="This will remove the webhook from the schema."
                                onConfirm={async () => {
                                  await deleteWebhook(wh.name);
                                  setExpandedWh(null);
                                  load();
                                }}
                              >
                                {(open) => (
                                  <button
                                    className="btn-icon-danger"
                                    title="Delete"
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      open();
                                    }}
                                  >
                                    <Trash2 size={14} />
                                  </button>
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
            });
          })()}
        </tbody>
      </table>
      {(() => {
        const filtered = webhooks.filter(
          (wh) => !cmdSearch.trim() || wh.name.toLowerCase().includes(cmdSearch.toLowerCase()),
        );
        const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
        if (totalPages === 1) return null;
        return (
          <div
            style={{
              display: "flex",
              gap: "0.5rem",
              alignItems: "center",
              justifyContent: "flex-end",
              padding: "0.5rem 0",
            }}
          >
            <button onClick={() => setWhPage(0)} disabled={whPage === 0}>
              «
            </button>
            <button onClick={() => setWhPage((p) => p - 1)} disabled={whPage === 0}>
              ‹
            </button>
            <span>
              Page {whPage + 1} / {totalPages}
            </span>
            <button onClick={() => setWhPage((p) => p + 1)} disabled={whPage >= totalPages - 1}>
              ›
            </button>
            <button onClick={() => setWhPage(totalPages - 1)} disabled={whPage >= totalPages - 1}>
              »
            </button>
          </div>
        );
      })()}

      <div style={{ marginTop: "1rem", display: "inline-flex", alignItems: "center", gap: "0.5rem" }}>
        <span style={{ fontSize: "0.85rem", color: "var(--text-muted)" }}>Test as role:</span>
        <select
          value={testRoleId}
          onChange={(e) => setTestRoleId(e.target.value)}
          style={{
            background: "var(--bg-card)",
            color: "var(--text)",
            border: "1px solid var(--border)",
            borderRadius: "4px",
            padding: "0.2rem 0.5rem",
            fontSize: "0.85rem",
          }}
        >
          <option value="">(no governance)</option>
          {roles.map((r) => (
            <option key={r.id} value={r.id}>{r.id}</option>
          ))}
        </select>
      </div>

      {testResult && (
        <div
          style={{
            marginTop: "1rem",
            padding: "1rem",
            background: "var(--surface)",
            border: "1px solid var(--border)",
            borderRadius: "4px",
          }}
        >
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              marginBottom: "0.5rem",
            }}
          >
            <h4>Test Result: {testResult.name}</h4>
            <button
              onClick={() => setTestResult(null)}
              style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}
            >
              Close
            </button>
          </div>
          {!!(testResult.data && typeof testResult.data === "object" && "enforcement" in testResult.data) && (
            <div
              style={{
                marginBottom: "0.75rem",
                padding: "0.5rem 0.75rem",
                background: "hsl(var(--color-info) / 0.08)",
                border: "1px solid hsl(var(--color-info) / 0.3)",
                borderRadius: "4px",
                fontSize: "0.8rem",
              }}
            >
              <strong>Governance applied</strong>
              <pre style={{ margin: "0.25rem 0 0", fontSize: "0.78rem" }}>
                {JSON.stringify((testResult.data as Record<string, unknown>).enforcement, null, 2)}
              </pre>
            </div>
          )}
          <pre style={{ fontSize: "0.85rem", overflow: "auto", maxHeight: "300px" }}>
            {JSON.stringify(
              testResult.data && typeof testResult.data === "object" && "rows" in testResult.data
                ? (testResult.data as Record<string, unknown>).rows
                : testResult.data,
              null,
              2,
            )}
          </pre>
        </div>
      )}
    </div>
  );
}
