// Copyright (c) 2026 Kenneth Stott
// Canary: 11512393-9871-4333-a5e8-19346666ab3c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useRef, type RefObject } from "react";
import {
  updateSettings,
  reloadQueryEngineCatalog,
  restartQueryEngine,
  recomputeSchemaClusters,
} from "../../api/admin";
import type { PlatformSettings } from "../../api/admin";

interface ObsTabProps {
  settings: PlatformSettings;
  setSettings: (s: PlatformSettings) => void;
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

const API_BASE = import.meta.env.VITE_API_BASE || "";

function TraceList({
  traces,
  filter,
  selected,
  setSelected,
  listRef,
}: {
  traces: TraceEntry[];
  filter: string;
  selected: TraceEntry | null;
  setSelected: (t: TraceEntry | null) => void;
  listRef?: RefObject<HTMLDivElement>;
}) {
  const statusColor = (s: string) =>
    s === "OK" ? "var(--success)" : s === "ERROR" ? "var(--error, #e55)" : "var(--text-muted)";

  const needle = filter.toLowerCase();
  const visible = needle
    ? traces.filter(
        (t) =>
          t.name.toLowerCase().includes(needle) ||
          t.status.toLowerCase().includes(needle) ||
          t.trace_id.toLowerCase().includes(needle),
      )
    : traces;

  return (
    <div className="trace-feed-list" ref={listRef}>
      {visible.length === 0 ? (
        <div className="trace-empty">
          {traces.length === 0 ? "No spans yet — make a request to see traces." : "No spans match filter."}
        </div>
      ) : (
        visible.map((t) => (
          <div
            key={t.span_id}
            className={`trace-row${selected?.span_id === t.span_id ? " trace-row--selected" : ""}`}
            onClick={() => setSelected(selected?.span_id === t.span_id ? null : t)}
          >
            <span className="trace-ts">{new Date(t.ts * 1000).toLocaleTimeString()}</span>
            <span className="trace-name">{t.name}</span>
            <span className="trace-status" style={{ color: statusColor(t.status) }}>
              {t.status}
            </span>
            <span className="trace-dur">
              {t.duration_ms != null ? `${t.duration_ms}ms` : "—"}
            </span>
            {selected?.span_id === t.span_id && (
              <div className="trace-detail" onClick={(e) => e.stopPropagation()}>
                <div>
                  <strong>Trace:</strong> <code>{t.trace_id}</code>
                </div>
                <div>
                  <strong>Span:</strong> <code>{t.span_id}</code>
                </div>
                <div>
                  <strong>Time:</strong> {new Date(t.ts * 1000).toLocaleTimeString()}
                </div>
                {Object.keys(t.attrs).length > 0 && (
                  <pre className="trace-attrs">{JSON.stringify(t.attrs, null, 2)}</pre>
                )}
              </div>
            )}
          </div>
        ))
      )}
    </div>
  );
}

function TraceFeed() {
  const [traces, setTraces] = useState<TraceEntry[]>([]);
  const [paused, setPaused] = useState(false);
  const [filter, setFilter] = useState("");
  const [selected, setSelected] = useState<TraceEntry | null>(null);
  const [expanded, setExpanded] = useState(false);
  const listRef = useRef<HTMLDivElement>(null);
  const pausedRef = useRef(paused);
  /* eslint-disable-next-line react-hooks/refs --
     latest-value ref mirrors `paused` for the async poll callback (standard ref pattern) */
  pausedRef.current = paused;

  useEffect(() => {
    let alive = true;
    const poll = async () => {
      if (!alive || pausedRef.current) return;
      try {
        const resp = await fetch(`${API_BASE}/admin/traces/recent?limit=50`);
        const json = await resp.json();
        if (alive && !pausedRef.current) setTraces(json.traces ?? []);
      } catch {
        /* ignore */
      }
    };
    poll();
    const id = setInterval(poll, 2000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  const headerContent = (
    <>
      <span style={{ flex: 1 }}>Live Traces</span>
      <input
        className="trace-filter"
        placeholder="Filter…"
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        onClick={(e) => e.stopPropagation()}
      />
      <div style={{ flex: 1, display: "flex", justifyContent: "flex-end", alignItems: "center", gap: "0.4rem" }}>
        <button
          className="trace-pause-btn"
          onClick={() => setPaused((p) => !p)}
          title={paused ? "Resume" : "Pause"}
        >
          {paused ? "▶ Resume" : "⏸ Pause"}
        </button>
        <button
          className="trace-expand-btn"
          onClick={(e) => { e.stopPropagation(); setExpanded((v) => !v); }}
          title={expanded ? "Collapse" : "Expand"}
        >
          {expanded ? "✕" : "⤢"}
        </button>
      </div>
    </>
  );

  return (
    <>
      <div className="trace-feed">
        <div className="trace-feed-header">
          {headerContent}
        </div>
        <TraceList traces={traces} filter={filter} selected={selected} setSelected={setSelected} listRef={listRef} />
      </div>
      {expanded && (
        <div
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.6)",
            zIndex: 1000,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
          }}
          onClick={() => setExpanded(false)}
        >
          <div
            style={{
              width: "90vw",
              height: "90vh",
              background: "var(--bg-surface, #1e1e2e)",
              borderRadius: "8px",
              display: "flex",
              flexDirection: "column",
              overflow: "hidden",
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div className="trace-feed-header">
              {headerContent}
            </div>
            <div style={{ flex: 1, overflow: "auto" }}>
              <TraceList traces={traces} filter={filter} selected={selected} setSelected={setSelected} />
            </div>
          </div>
        </div>
      )}
    </>
  );
}

export function ObservabilityTab({ settings, setSettings }: ObsTabProps) {
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
          Status:{" "}
          <strong style={{ color: active ? "var(--success)" : "var(--text-muted)" }}>
            {active
              ? `Exporting → ${settings.otel.endpoint}`
              : "Active (spans dropped — no collector configured)"}
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
            Leave empty to generate traces without exporting. Override with
            OTEL_EXPORTER_OTLP_ENDPOINT env var.
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
          Note: endpoint and service_name changes take effect on next restart. Sample rate is
          applied immediately.
        </p>

        <h4 style={{ marginTop: "1.5rem" }}>Support Telemetry</h4>
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem", marginBottom: "1rem" }}>
          Optionally forward telemetry to Provisa support. SQL literals are stripped by default.
        </p>
        <label>
          Support OTLP Endpoint
          <input
            type="text"
            value={settings.otel.support_endpoint}
            onChange={(e) => update("support_endpoint", e.target.value)}
            placeholder="https://otel.provisa.io:4318"
          />
          <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
            Leave empty to disable support telemetry. Override with PROVISA_SUPPORT_OTLP_ENDPOINT
            env var.
          </span>
        </label>
        <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem" }}>
          <input
            type="checkbox"
            checked={settings.otel.support_redact_sql_literals}
            onChange={(e) => update("support_redact_sql_literals", e.target.checked)}
          />
          Redact SQL literals before forwarding to support
        </label>
        <label>
          Redact Attributes (comma-separated)
          <input
            type="text"
            value={(settings.otel.support_redact_attributes ?? []).join(", ")}
            onChange={(e) =>
              update(
                "support_redact_attributes",
                e.target.value
                  .split(",")
                  .map((s) => s.trim())
                  .filter(Boolean),
              )
            }
            placeholder="user.id, db.user, ..."
          />
          <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
            Attribute keys to drop entirely before sending to support.
          </span>
        </label>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
        <TraceFeed />
        <QueryEngineActions />
      </div>
    </div>
  );
}

function QueryEngineActions() {
  const [reloadStatus, setReloadStatus] = useState<string>("");
  const [restartStatus, setRestartStatus] = useState<string>("");
  const [clusterStatus, setClusterStatus] = useState<string>("");
  const [reloading, setReloading] = useState(false);
  const [restarting, setRestarting] = useState(false);
  const [reclustering, setReclustering] = useState(false);

  const handleReload = async () => {
    setReloading(true);
    setReloadStatus("");
    try {
      const result = await reloadQueryEngineCatalog();
      setReloadStatus(result.success ? "Catalog reloaded." : `Errors: ${result.errors.join("; ")}`);
    } catch (e: unknown) {
      setReloadStatus(e instanceof Error ? e.message : "Failed");
    } finally {
      setReloading(false);
    }
  };

  const handleRestart = async () => {
    setRestarting(true);
    setRestartStatus("");
    try {
      const result = await restartQueryEngine();
      setRestartStatus(`Restarted: ${result.container}`);
    } catch (e: unknown) {
      setRestartStatus(e instanceof Error ? e.message : "Failed");
    } finally {
      setRestarting(false);
    }
  };

  const handleRecluster = async () => {
    setReclustering(true);
    setClusterStatus("");
    try {
      const result = await recomputeSchemaClusters();
      setClusterStatus(`Clustered ${result.tables_clustered} tables.`);
    } catch (e: unknown) {
      setClusterStatus(e instanceof Error ? e.message : "Failed");
    } finally {
      setReclustering(false);
    }
  };

  return (
    <div className="settings-section">
      <h4>Query Engine</h4>
      <div style={{ display: "flex", gap: "0.75rem", alignItems: "center", flexWrap: "wrap" }}>
        <button className="btn-primary" onClick={handleReload} disabled={reloading}>
          {reloading ? "Reloading..." : "Reload Catalog"}
        </button>
        <button className="btn-warning" onClick={handleRestart} disabled={restarting}>
          {restarting ? "Restarting..." : "Restart Engine"}
        </button>
        <button className="btn-secondary" onClick={handleRecluster} disabled={reclustering}>
          {reclustering ? "Clustering..." : "Recompute Schema Clusters"}
        </button>
      </div>
      {reloadStatus && (
        <span className="upload-msg" style={{ marginTop: "0.5rem", display: "block" }}>
          {reloadStatus}
        </span>
      )}
      {restartStatus && (
        <span className="upload-msg" style={{ marginTop: "0.5rem", display: "block" }}>
          {restartStatus}
        </span>
      )}
      {clusterStatus && (
        <span className="upload-msg" style={{ marginTop: "0.5rem", display: "block" }}>
          {clusterStatus}
        </span>
      )}
    </div>
  );
}
