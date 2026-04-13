// Copyright (c) 2026 Kenneth Stott
// Canary: b8b1d1a3-e713-464e-8e0a-c8bc5b43544d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useRef, useCallback } from "react";
import { Trash2 } from "lucide-react";
import {
  fetchSources,
  fetchDomains,
  fetchTables,
  fetchRelationships,
  fetchRlsRules,
  fetchRoles,
  downloadConfig,
  uploadConfig,
  fetchSettings,
  updateSettings,
  createDomain,
  deleteDomain,
} from "../api/admin";
import type { PlatformSettings } from "../api/admin";
import type { Domain } from "../types/admin";
import { domainGqlAlias } from "../types/admin";
import { MVManager } from "../components/admin/MVManager";
import { CacheManager } from "../components/admin/CacheManager";
import { SystemHealth } from "../components/admin/SystemHealth";
import { ScheduledTasks } from "../components/admin/ScheduledTasks";

const FORMAT_OPTIONS = ["parquet", "orc", "json", "ndjson", "csv", "arrow"];
const TABS = ["Overview", "Domains", "Materialized Views", "Cache", "Scheduled Tasks", "System Health", "Observability"] as const;
type Tab = typeof TABS[number];

/** Admin overview page — dashboard, config management, platform settings. */
export function AdminPage() {
  const [activeTab, setActiveTab] = useState<Tab>("Overview");
  const [stats, setStats] = useState<Record<string, number>>({});
  const [domains, setDomains] = useState<Domain[]>([]);
  const [newDomainId, setNewDomainId] = useState("");
  const [newDomainDesc, setNewDomainDesc] = useState("");
  const [newDomainAlias, setNewDomainAlias] = useState("");
  const [domainMsg, setDomainMsg] = useState("");
  const [loading, setLoading] = useState(true);
  const [configYaml, setConfigYaml] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadMsg, setUploadMsg] = useState("");
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [settingsMsg, setSettingsMsg] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    Promise.all([
      fetchSources(),
      fetchDomains(),
      fetchTables(),
      fetchRelationships(),
      fetchRoles(),
      fetchRlsRules(),
      fetchSettings(),
    ])
      .then(([sources, doms, tables, rels, roles, rls, s]) => {
        setStats({
          Sources: sources.length,
          Domains: doms.length,
          Tables: tables.length,
          Relationships: rels.length,
          Roles: roles.length,
          "RLS Rules": rls.length,
        });
        setDomains(doms);
        setSettings(s);
      })
      .finally(() => setLoading(false));
  }, []);

  const handleDownload = async () => {
    const yaml = await downloadConfig();
    const blob = new Blob([yaml], { type: "application/x-yaml" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "provisa.yaml";
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleViewConfig = async () => {
    if (configYaml !== null) {
      setConfigYaml(null);
      return;
    }
    const yaml = await downloadConfig();
    setConfigYaml(yaml);
  };

  const handleUploadClick = () => fileInputRef.current?.click();

  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploading(true);
    setUploadMsg("");
    const text = await file.text();
    const result = await uploadConfig(text);
    setUploadMsg(result.message);
    setUploading(false);
    if (fileInputRef.current) fileInputRef.current.value = "";
  };

  const saveSettings = useCallback(async () => {
    if (!settings) return;
    setSettingsSaving(true);
    setSettingsMsg("");
    const result = await updateSettings(settings);
    setSettingsMsg(`Updated: ${result.updated.join(", ")}`);
    setSettingsSaving(false);
  }, [settings]);

  const updateRedirect = (key: string, value: unknown) => {
    if (!settings) return;
    setSettings({
      ...settings,
      redirect: { ...settings.redirect, [key]: value },
    });
  };

  const handleAddDomain = async () => {
    if (!newDomainId.trim()) return;
    await createDomain(newDomainId.trim(), newDomainDesc.trim(), newDomainAlias.trim() || null);
    const updated = await fetchDomains();
    setDomains(updated);
    setStats((s) => ({ ...s, Domains: updated.length }));
    setNewDomainId("");
    setNewDomainDesc("");
    setNewDomainAlias("");
    setDomainMsg(`Added "${newDomainId.trim()}"`);
  };

  const handleDeleteDomain = async (id: string) => {
    await deleteDomain(id);
    const updated = await fetchDomains();
    setDomains(updated);
    setStats((s) => ({ ...s, Domains: updated.length }));
    setDomainMsg(`Deleted "${id}"`);
  };

  if (loading) return <div className="page">Loading admin dashboard...</div>;

  return (
    <div className="page">
      <h2>Admin Dashboard</h2>

      <div className="admin-tabs">
        {TABS.map((tab) => (
          <button
            key={tab}
            className={`admin-tab${activeTab === tab ? " active" : ""}`}
            onClick={() => setActiveTab(tab)}
          >
            {tab}
          </button>
        ))}
      </div>

      <div className="admin-tab-content">
      {activeTab === "Overview" && (
        <>
          <div className="stats-grid">
            {Object.entries(stats).map(([label, count]) => (
              <div key={label} className="stat-card">
                <div className="stat-count">{count}</div>
                <div className="stat-label">{label}</div>
              </div>
            ))}
          </div>

          <h3>Platform Settings</h3>
          {settings && (
            <div className="settings-grid">
              <div className="settings-section">
                <h4>Redirect</h4>
                <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", whiteSpace: "nowrap" }}>
                  <input
                    type="checkbox"
                    checked={settings.redirect.enabled}
                    onChange={(e) => updateRedirect("enabled", e.target.checked)}
                    style={{ width: "auto" }}
                  />
                  Enabled
                </label>
                <label>
                  Default Threshold (rows)
                  <input
                    type="number"
                    value={settings.redirect.threshold}
                    onChange={(e) =>
                      updateRedirect("threshold", parseInt(e.target.value) || 0)
                    }
                  />
                </label>
                <label>
                  Default Format
                  <select
                    value={settings.redirect.default_format}
                    onChange={(e) =>
                      updateRedirect("default_format", e.target.value)
                    }
                  >
                    {FORMAT_OPTIONS.map((f) => (
                      <option key={f} value={f}>
                        {f}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  Presigned URL TTL (seconds)
                  <input
                    type="number"
                    value={settings.redirect.ttl}
                    onChange={(e) =>
                      updateRedirect("ttl", parseInt(e.target.value) || 0)
                    }
                  />
                </label>
              </div>
              <div className="settings-section">
                <h4>Naming</h4>
                <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", whiteSpace: "nowrap" }}>
                  <input
                    type="checkbox"
                    checked={settings.naming.domain_prefix}
                    onChange={(e) =>
                      setSettings({
                        ...settings,
                        naming: { domain_prefix: e.target.checked },
                      })
                    }
                    style={{ width: "auto" }}
                  />
                  Domain prefix (domain_id__ prepended to all names)
                </label>
                <label>
                  Naming Convention
                  <select
                    value={settings.naming.convention || "snake_case"}
                    onChange={(e) =>
                      setSettings({
                        ...settings,
                        naming: {
                          ...settings.naming,
                          convention: e.target.value,
                        },
                      })
                    }
                  >
                    <option value="none">None (raw DB names)</option>
                    <option value="snake_case">snake_case</option>
                    <option value="camelCase">camelCase</option>
                    <option value="PascalCase">PascalCase</option>
                  </select>
                </label>
              </div>
              <div className="settings-section">
                <h4>Sampling</h4>
                <label>
                  Default Sample Size
                  <input
                    type="number"
                    value={settings.sampling.default_sample_size}
                    onChange={(e) =>
                      setSettings({
                        ...settings,
                        sampling: {
                          default_sample_size: parseInt(e.target.value) || 0,
                        },
                      })
                    }
                  />
                </label>
              </div>
              <div className="settings-section">
                <h4>Cache</h4>
                <label>
                  Default TTL (seconds)
                  <input
                    type="number"
                    value={settings.cache.default_ttl}
                    onChange={(e) =>
                      setSettings({
                        ...settings,
                        cache: {
                          default_ttl: parseInt(e.target.value) || 0,
                        },
                      })
                    }
                  />
                </label>
              </div>
              <div className="settings-actions">
                <button
                  className="btn-primary"
                  onClick={saveSettings}
                  disabled={settingsSaving}
                >
                  {settingsSaving ? "Saving..." : "Save Settings"}
                </button>
                {settingsMsg && (
                  <span className="upload-msg">{settingsMsg}</span>
                )}
              </div>
            </div>
          )}

          <h3>Configuration File</h3>
          <div className="config-actions">
            <button className="btn-secondary" onClick={handleDownload}>
              Download
            </button>
            <button className="btn-secondary" onClick={handleViewConfig}>
              {configYaml !== null ? "Hide" : "View"}
            </button>
            <button
              className="btn-primary"
              onClick={handleUploadClick}
              disabled={uploading}
            >
              {uploading ? "Uploading..." : "Upload"}
            </button>
            <input
              ref={fileInputRef}
              type="file"
              accept=".yaml,.yml"
              style={{ display: "none" }}
              onChange={handleFileChange}
            />
            {uploadMsg && <span className="upload-msg">{uploadMsg}</span>}
          </div>

          {configYaml !== null && (
            <pre className="config-preview">{configYaml}</pre>
          )}
        </>
      )}

      {activeTab === "Domains" && (
        <>
          {domainMsg && <div className="success" style={{ marginBottom: "0.5rem" }}>{domainMsg}</div>}
          <table className="data-table" style={{ marginBottom: "1rem" }}>
            <thead><tr><th>ID</th><th>Description</th><th>GQL Alias</th><th></th></tr></thead>
            <tbody>
              {domains.length === 0 && (
                <tr><td colSpan={4} style={{ color: "var(--text-muted)", textAlign: "center" }}>No domains defined</td></tr>
              )}
              {domains.map((d) => (
                <tr key={d.id}>
                  <td>{d.id}</td>
                  <td>{d.description || "—"}</td>
                  <td style={{ color: "var(--text-muted)", fontFamily: "monospace" }}>{domainGqlAlias(d)}</td>
                  <td>
                    <button className="btn-icon-danger" title="Delete" onClick={() => handleDeleteDomain(d.id)}><Trash2 size={14} /></button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{ display: "flex", gap: "0.5rem", alignItems: "center" }}>
            <input value={newDomainId} onChange={(e) => setNewDomainId(e.target.value)} placeholder="domain-id" style={{ width: "160px", background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.5rem", borderRadius: "4px" }} />
            <input value={newDomainDesc} onChange={(e) => setNewDomainDesc(e.target.value)} placeholder="description (optional)" style={{ flex: 1, background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.5rem", borderRadius: "4px" }} />
            <input value={newDomainAlias} onChange={(e) => setNewDomainAlias(e.target.value)} placeholder={newDomainId.trim() ? `alias (default: ${domainGqlAlias({ id: newDomainId.trim(), description: "" })})` : "gql alias (optional)"} style={{ width: "180px", background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.5rem", borderRadius: "4px" }} />
            <button className="btn-primary" onClick={handleAddDomain} disabled={!newDomainId.trim()}>Add Domain</button>
          </div>
        </>
      )}
      {activeTab === "Materialized Views" && <MVManager />}
      {activeTab === "Cache" && <CacheManager />}
      {activeTab === "Scheduled Tasks" && <ScheduledTasks />}
      {activeTab === "System Health" && <SystemHealth />}
      {activeTab === "Observability" && settings && (
        <ObservabilityTab settings={settings} setSettings={setSettings} />
      )}
      </div>
    </div>
  );
}

interface ObsTabProps {
  settings: import("../api/admin").PlatformSettings;
  setSettings: (s: import("../api/admin").PlatformSettings) => void;
}

interface TraceEntry {
  ts: number;
  trace_id: string;
  span_id: string;
  name: string;
  status: string;
  duration_ms: number | null;
  attrs: Record<string, unknown>;
}

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8001";

function TraceFeed() {
  const [traces, setTraces] = useState<TraceEntry[]>([]);
  const [paused, setPaused] = useState(false);
  const [selected, setSelected] = useState<TraceEntry | null>(null);
  const listRef = useRef<HTMLDivElement>(null);
  const pausedRef = useRef(paused);
  pausedRef.current = paused;

  useEffect(() => {
    let alive = true;
    const poll = async () => {
      if (!alive || pausedRef.current) return;
      try {
        const resp = await fetch(`${API_BASE}/admin/traces/recent?limit=50`);
        const json = await resp.json();
        if (alive && !pausedRef.current) setTraces(json.traces ?? []);
      } catch { /* ignore */ }
    };
    poll();
    const id = setInterval(poll, 2000);
    return () => { alive = false; clearInterval(id); };
  }, []);

  const statusColor = (s: string) =>
    s === "OK" ? "var(--success)" : s === "ERROR" ? "var(--error, #e55)" : "var(--text-muted)";

  return (
    <div className="trace-feed">
      <div className="trace-feed-header">
        <span>Live Traces</span>
        <button
          className="trace-pause-btn"
          onClick={() => setPaused((p) => !p)}
          title={paused ? "Resume" : "Pause"}
        >
          {paused ? "▶ Resume" : "⏸ Pause"}
        </button>
      </div>
      <div className="trace-feed-list" ref={listRef}>
        {traces.length === 0 ? (
          <div className="trace-empty">No spans yet — make a request to see traces.</div>
        ) : (
          traces.map((t) => (
            <div
              key={t.span_id}
              className={`trace-row${selected?.span_id === t.span_id ? " trace-row--selected" : ""}`}
              onClick={() => setSelected(selected?.span_id === t.span_id ? null : t)}
            >
              <span className="trace-name">{t.name}</span>
              <span className="trace-status" style={{ color: statusColor(t.status) }}>{t.status}</span>
              <span className="trace-dur">{t.duration_ms != null ? `${t.duration_ms}ms` : "—"}</span>
              {selected?.span_id === t.span_id && (
                <div className="trace-detail" onClick={(e) => e.stopPropagation()}>
                  <div><strong>Trace:</strong> <code>{t.trace_id}</code></div>
                  <div><strong>Span:</strong> <code>{t.span_id}</code></div>
                  <div><strong>Time:</strong> {new Date(t.ts * 1000).toLocaleTimeString()}</div>
                  {Object.keys(t.attrs).length > 0 && (
                    <pre className="trace-attrs">{JSON.stringify(t.attrs, null, 2)}</pre>
                  )}
                </div>
              )}
            </div>
          ))
        )}
      </div>
    </div>
  );
}

function ObservabilityTab({ settings, setSettings }: ObsTabProps) {
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");

  const update = (key: keyof typeof settings.otel, value: unknown) =>
    setSettings({ ...settings, otel: { ...settings.otel, [key]: value } });

  const save = async () => {
    setSaving(true);
    setMsg("");
    try {
      const result = await updateSettings({ otel: settings.otel });
      setMsg(`Saved: ${result.updated.join(", ")}`);
    } catch (e: unknown) {
      setMsg(e instanceof Error ? e.message : "Save failed");
    } finally {
      setSaving(false);
    }
  };

  const active = Boolean(settings.otel.endpoint);

  return (
    <div className="observability-layout">
      <div className="settings-section">
        <h4>OpenTelemetry Tracing</h4>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem", marginBottom: "1rem" }}>
          Status: <strong style={{ color: active ? "var(--success)" : "var(--text-muted)" }}>
            {active ? `Exporting → ${settings.otel.endpoint}` : "Active (spans dropped — no collector configured)"}
          </strong>
        </p>
        <label>
          OTLP Collector Endpoint
          <input
            type="text"
            value={settings.otel.endpoint}
            onChange={(e) => update("endpoint", e.target.value)}
            placeholder="http://otel-collector:4317"
          />
          <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
            Leave empty to generate traces without exporting. Override with OTEL_EXPORTER_OTLP_ENDPOINT env var.
          </span>
        </label>
        <label>
          Service Name
          <input
            type="text"
            value={settings.otel.service_name}
            onChange={(e) => update("service_name", e.target.value)}
            placeholder="provisa"
          />
          <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
            Override with OTEL_SERVICE_NAME env var.
          </span>
        </label>
        <label>
          Sample Rate
          <input
            type="number"
            min={0}
            max={1}
            step={0.01}
            value={settings.otel.sample_rate}
            onChange={(e) => update("sample_rate", parseFloat(e.target.value) || 0)}
          />
          <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
            0.0–1.0. 1.0 = 100% of traces sampled.
          </span>
        </label>
        <div style={{ marginTop: "1rem", display: "flex", gap: "0.75rem", alignItems: "center" }}>
          <button className="btn-primary" onClick={save} disabled={saving}>
            {saving ? "Saving..." : "Save"}
          </button>
          {msg && <span className="upload-msg">{msg}</span>}
        </div>
        <p style={{ marginTop: "1.5rem", fontSize: "0.8rem", color: "var(--text-muted)" }}>
          Note: endpoint and service_name changes take effect on next restart. Sample rate is applied immediately.
        </p>
      </div>
      <TraceFeed />
    </div>
  );
}
