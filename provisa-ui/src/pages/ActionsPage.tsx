import { useState, useEffect, useCallback } from "react";
import {
  fetchActions,
  saveFunction,
  saveWebhook,
  deleteFunction,
  deleteWebhook,
  testAction,
} from "../api/actions";
import type { TrackedFunction, TrackedWebhook, ActionArg, InlineField } from "../api/actions";
import { fetchSources, fetchTables } from "../api/admin";
import type { Source, RegisteredTable } from "../types/admin";
import { ConfirmDialog } from "../components/ConfirmDialog";

const GRAPHQL_TYPES = ["String", "Int", "Float", "Boolean", "DateTime", "Date", "BigInt", "JSON"];

const EMPTY_ARG: ActionArg = { name: "", type: "String" };
const EMPTY_INLINE: InlineField = { name: "", type: "String" };

type ActionType = "function" | "webhook";

interface FormState {
  actionType: ActionType;
  name: string;
  // Function fields
  sourceId: string;
  schemaName: string;
  functionName: string;
  returns: string;
  visibleTo: string;
  writablBy: string;
  domainId: string;
  description: string;
  arguments: ActionArg[];
  // Webhook fields
  url: string;
  method: string;
  timeoutMs: number;
  inlineReturnType: InlineField[];
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
};

export function ActionsPage() {
  const [functions, setFunctions] = useState<TrackedFunction[]>([]);
  const [webhooks, setWebhooks] = useState<TrackedWebhook[]>([]);
  const [sources, setSources] = useState<Source[]>([]);
  const [tables, setTables] = useState<RegisteredTable[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [msg, setMsg] = useState("");
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState<FormState>({ ...EMPTY_FORM });
  const [editingName, setEditingName] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [testResult, setTestResult] = useState<{ name: string; data: unknown } | null>(null);
  const [testing, setTesting] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [fns, whs, srcs, tbls] = await Promise.all([
        fetchActions().then((a) => a.functions),
        fetchActions().then((a) => a.webhooks),
        fetchSources(),
        fetchTables(),
      ]);
      setFunctions(fns);
      setWebhooks(whs);
      setSources(srcs);
      setTables(tbls);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const tableOptions = tables.map((t) => ({
    value: `${t.sourceId}.${t.schemaName}.${t.tableName}`,
    label: `${t.sourceId}.${t.schemaName}.${t.tableName}${t.alias ? ` (${t.alias})` : ""}`,
  }));

  const handleAddArg = () => {
    setForm({ ...form, arguments: [...form.arguments, { ...EMPTY_ARG }] });
  };

  const handleRemoveArg = (idx: number) => {
    setForm({ ...form, arguments: form.arguments.filter((_, i) => i !== idx) });
  };

  const handleArgChange = (idx: number, field: keyof ActionArg, value: string) => {
    const args = [...form.arguments];
    args[idx] = { ...args[idx], [field]: value };
    setForm({ ...form, arguments: args });
  };

  const handleAddInlineField = () => {
    setForm({ ...form, inlineReturnType: [...form.inlineReturnType, { ...EMPTY_INLINE }] });
  };

  const handleRemoveInlineField = (idx: number) => {
    setForm({ ...form, inlineReturnType: form.inlineReturnType.filter((_, i) => i !== idx) });
  };

  const handleInlineFieldChange = (idx: number, field: keyof InlineField, value: string) => {
    const fields = [...form.inlineReturnType];
    fields[idx] = { ...fields[idx], [field]: value };
    setForm({ ...form, inlineReturnType: fields });
  };

  const handleEdit = (actionType: ActionType, name: string) => {
    if (actionType === "function") {
      const fn = functions.find((f) => f.name === name);
      if (!fn) return;
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
      });
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
      });
    }
    setEditingName(name);
    setShowForm(true);
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
        await saveFunction({
          name: form.name,
          sourceId: form.sourceId,
          schemaName: form.schemaName,
          functionName: form.functionName,
          returns: form.returns,
          arguments: form.arguments,
          visibleTo,
          writableBy,
          domainId: form.domainId,
          description: form.description || undefined,
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

  if (loading) return <div className="page">Loading actions...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Actions</h2>
        <button onClick={() => { setShowForm(!showForm); if (showForm) handleCancel(); }}>
          {showForm ? "Cancel" : "Add Action"}
        </button>
      </div>

      {error && <div className="error">{error}</div>}
      {msg && <div className="success">{msg}</div>}

      {showForm && (
        <form className="form-card" onSubmit={handleSave}>
          <label>Type
            <select
              value={form.actionType}
              onChange={(e) => setForm({ ...EMPTY_FORM, actionType: e.target.value as ActionType })}
              disabled={editingName !== null}
            >
              <option value="function">DB Function</option>
              <option value="webhook">Webhook</option>
            </select>
          </label>

          <label>Name
            <input
              required
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              placeholder="e.g. process_order"
              disabled={editingName !== null}
            />
          </label>

          {form.actionType === "function" && (
            <>
              <label>Source
                <select
                  required
                  value={form.sourceId}
                  onChange={(e) => setForm({ ...form, sourceId: e.target.value })}
                >
                  <option value="">Select source...</option>
                  {sources.map((s) => (
                    <option key={s.id} value={s.id}>{s.id} ({s.type})</option>
                  ))}
                </select>
              </label>
              <label>Schema
                <input
                  value={form.schemaName}
                  onChange={(e) => setForm({ ...form, schemaName: e.target.value })}
                />
              </label>
              <label>Function Name
                <input
                  required
                  value={form.functionName}
                  onChange={(e) => setForm({ ...form, functionName: e.target.value })}
                  placeholder="DB function name"
                />
              </label>
              <label>Returns (table)
                <select
                  required
                  value={form.returns}
                  onChange={(e) => setForm({ ...form, returns: e.target.value })}
                >
                  <option value="">Select table...</option>
                  {tableOptions.map((t) => (
                    <option key={t.value} value={t.value}>{t.label}</option>
                  ))}
                </select>
              </label>
              <label>Visible To (roles, comma-separated)
                <input
                  value={form.visibleTo}
                  onChange={(e) => setForm({ ...form, visibleTo: e.target.value })}
                  placeholder="admin, analyst"
                />
              </label>
              <label>Writable By (roles, comma-separated)
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
              <label>URL
                <input
                  required
                  value={form.url}
                  onChange={(e) => setForm({ ...form, url: e.target.value })}
                  placeholder="https://api.example.com/action"
                />
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
                <input
                  type="number"
                  min={100}
                  value={form.timeoutMs}
                  onChange={(e) => setForm({ ...form, timeoutMs: +e.target.value })}
                />
              </label>
              <label>Returns (table, optional)
                <select
                  value={form.returns}
                  onChange={(e) => setForm({ ...form, returns: e.target.value })}
                >
                  <option value="">None (use inline type)</option>
                  {tableOptions.map((t) => (
                    <option key={t.value} value={t.value}>{t.label}</option>
                  ))}
                </select>
              </label>
              <label>Visible To (roles, comma-separated)
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
                    <div key={i} style={{ display: "flex", gap: "0.5rem", marginBottom: "0.25rem", alignItems: "center" }}>
                      <input
                        value={f.name}
                        onChange={(e) => handleInlineFieldChange(i, "name", e.target.value)}
                        placeholder="Field name"
                        style={{ flex: 1 }}
                      />
                      <select
                        value={f.type}
                        onChange={(e) => handleInlineFieldChange(i, "type", e.target.value)}
                      >
                        {GRAPHQL_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
                      </select>
                      <button type="button" className="destructive" onClick={() => handleRemoveInlineField(i)} style={{ padding: "0.25rem 0.5rem" }}>
                        X
                      </button>
                    </div>
                  ))}
                  <button type="button" onClick={handleAddInlineField} style={{ fontSize: "0.85rem", marginTop: "0.25rem" }}>
                    + Add Field
                  </button>
                </div>
              )}
            </>
          )}

          <label>Domain
            <input
              value={form.domainId}
              onChange={(e) => setForm({ ...form, domainId: e.target.value })}
              placeholder="optional"
            />
          </label>
          <label>Description
            <input
              value={form.description}
              onChange={(e) => setForm({ ...form, description: e.target.value })}
              placeholder="optional"
            />
          </label>

          <div style={{ gridColumn: "1 / -1" }}>
            <h4 style={{ marginBottom: "0.5rem" }}>Arguments</h4>
            {form.arguments.map((arg, i) => (
              <div key={i} style={{ display: "flex", gap: "0.5rem", marginBottom: "0.25rem", alignItems: "center" }}>
                <input
                  value={arg.name}
                  onChange={(e) => handleArgChange(i, "name", e.target.value)}
                  placeholder="Arg name"
                  style={{ flex: 1 }}
                />
                <select
                  value={arg.type}
                  onChange={(e) => handleArgChange(i, "type", e.target.value)}
                >
                  {GRAPHQL_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
                </select>
                <button type="button" className="destructive" onClick={() => handleRemoveArg(i)} style={{ padding: "0.25rem 0.5rem" }}>
                  X
                </button>
              </div>
            ))}
            <button type="button" onClick={handleAddArg} style={{ fontSize: "0.85rem", marginTop: "0.25rem" }}>
              + Add Argument
            </button>
          </div>

          <button type="submit" disabled={saving}>{saving ? "Saving..." : editingName ? "Update" : "Create"}</button>
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
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {functions.length === 0 && (
            <tr><td colSpan={7} style={{ color: "var(--text-muted)", textAlign: "center" }}>No functions registered</td></tr>
          )}
          {functions.map((fn) => (
            <tr key={fn.name}>
              <td>{fn.name}</td>
              <td>{fn.sourceId}</td>
              <td>{fn.schemaName}.{fn.functionName}</td>
              <td>{fn.returns}</td>
              <td>{fn.arguments.length}</td>
              <td>{fn.visibleTo.join(", ") || "all"}</td>
              <td style={{ display: "flex", gap: "0.25rem" }}>
                <button onClick={() => handleEdit("function", fn.name)} style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}>
                  Edit
                </button>
                <button
                  onClick={() => handleTest("function", fn.name)}
                  disabled={testing === fn.name}
                  style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}
                >
                  {testing === fn.name ? "Testing..." : "Test"}
                </button>
                <ConfirmDialog
                  title={`Delete function "${fn.name}"?`}
                  consequence="This will remove the function from the schema."
                  onConfirm={async () => { await deleteFunction(fn.name); load(); }}
                >
                  {(open) => <button className="destructive" onClick={open}>Delete</button>}
                </ConfirmDialog>
              </td>
            </tr>
          ))}
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
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {webhooks.length === 0 && (
            <tr><td colSpan={8} style={{ color: "var(--text-muted)", textAlign: "center" }}>No webhooks registered</td></tr>
          )}
          {webhooks.map((wh) => (
            <tr key={wh.name}>
              <td>{wh.name}</td>
              <td style={{ maxWidth: "200px", overflow: "hidden", textOverflow: "ellipsis" }}>{wh.url}</td>
              <td>{wh.method}</td>
              <td>{wh.timeoutMs}ms</td>
              <td>{wh.returns || `inline (${wh.inlineReturnType.length} fields)`}</td>
              <td>{wh.arguments.length}</td>
              <td>{wh.visibleTo.join(", ") || "all"}</td>
              <td style={{ display: "flex", gap: "0.25rem" }}>
                <button onClick={() => handleEdit("webhook", wh.name)} style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}>
                  Edit
                </button>
                <button
                  onClick={() => handleTest("webhook", wh.name)}
                  disabled={testing === wh.name}
                  style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}
                >
                  {testing === wh.name ? "Testing..." : "Test"}
                </button>
                <ConfirmDialog
                  title={`Delete webhook "${wh.name}"?`}
                  consequence="This will remove the webhook from the schema."
                  onConfirm={async () => { await deleteWebhook(wh.name); load(); }}
                >
                  {(open) => <button className="destructive" onClick={open}>Delete</button>}
                </ConfirmDialog>
              </td>
            </tr>
          ))}
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
