// Copyright (c) 2026 Kenneth Stott
// Canary: f4a2c9b7-3e1d-4f5a-8b2e-7c6d1a9f4e3b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useCallback, useRef, useEffect } from "react";
import {
  GraphFrame,
  labelColor,
  extractElements,
  type FrameData,
} from "./GraphFrame";
import "./GraphPage.css";

interface SchemaNodeLabel {
  domainLabel: string | null;
  tableLabel: string;
  properties: string[];
}
interface SchemaRel {
  type: string;
  source: string;
  target: string;
}

// ── Sidebar: schema summary ───────────────────────────────────────────────────
interface SidebarProps {
  schemaNodeLabels: SchemaNodeLabel[];
  schemaRels: SchemaRel[];
  schemaLoading: boolean;
  history: string[];
  onHistorySelect: (q: string) => void;
  onLabelClick: (label: string) => void;
  onRelClick: (type: string) => void;
  width: number;
  onWidthChange: (w: number) => void;
}

function Sidebar({ schemaNodeLabels, schemaRels, schemaLoading, history, onHistorySelect, onLabelClick, onRelClick, width, onWidthChange }: SidebarProps) {
  const [section, setSection] = useState<"db" | "history">("db");
  const dragging = useRef(false);

  const onMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    dragging.current = true;
    const startX = e.clientX;
    const startW = width;
    const onMove = (ev: MouseEvent) => {
      if (!dragging.current) return;
      const next = Math.max(160, Math.min(480, startW + ev.clientX - startX));
      onWidthChange(next);
    };
    const onUp = () => { dragging.current = false; window.removeEventListener("mousemove", onMove); window.removeEventListener("mouseup", onUp); };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, [width, onWidthChange]);

  return (
    <aside className="graph-sidebar" style={{ width }}>
      <div className="graph-sidebar-tabs">
        <button
          className={`graph-sidebar-tab ${section === "db" ? "active" : ""}`}
          onClick={() => setSection("db")}
          title="Database"
        >
          ◉
        </button>
        <button
          className={`graph-sidebar-tab ${section === "history" ? "active" : ""}`}
          onClick={() => setSection("history")}
          title="History"
        >
          ⏱
        </button>
      </div>

      <div className="graph-sidebar-body">
        {section === "db" && (
          <>
            {(() => {
              const domainLabels = schemaLoading ? [] : [...new Set(
                schemaNodeLabels.filter(n => n.domainLabel).map(n => n.domainLabel!)
              )].sort();
              const tableLabels = schemaLoading ? [] : [...new Set(
                schemaNodeLabels.map(n => n.tableLabel)
              )].sort();
              return (
                <>
                  {domainLabels.length > 0 && (
                    <div className="graph-schema-section">
                      <div className="graph-schema-heading">Domain Labels</div>
                      <div className="graph-label-list">
                        {domainLabels.map((lbl) => (
                          <div key={lbl} className="graph-label-item">
                            <span
                              className="graph-label-pill"
                              style={{ background: labelColor(lbl) }}
                              onClick={() => onLabelClick(lbl)}
                              title={`MATCH (n:${lbl}) RETURN n LIMIT 25`}
                            >
                              {lbl}
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                  <div className="graph-schema-section">
                    <div className="graph-schema-heading">Node Labels</div>
                    {schemaLoading ? (
                      <div className="graph-schema-empty">Loading…</div>
                    ) : tableLabels.length === 0 ? (
                      <div className="graph-schema-empty">No labels found</div>
                    ) : (
                      <div className="graph-label-list">
                        {tableLabels.map((lbl) => (
                          <div key={lbl} className="graph-label-item">
                            <span
                              className="graph-label-pill"
                              style={{ background: labelColor(lbl) }}
                              onClick={() => onLabelClick(lbl)}
                              title={`MATCH (n:${lbl}) RETURN n LIMIT 25`}
                            >
                              {lbl}
                            </span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                </>
              );
            })()}

            <div className="graph-schema-section">
              <div className="graph-schema-heading">Relationship Types</div>
              {schemaLoading ? (
                <div className="graph-schema-empty">Loading…</div>
              ) : schemaRels.length === 0 ? (
                <div className="graph-schema-empty">No relationship types found</div>
              ) : (
                <div className="graph-rel-list">
                  {schemaRels.map(({ type, source, target }) => (
                    <div
                      key={type}
                      className="graph-rel-item graph-rel-item--clickable"
                      title={`MATCH ()-[r:${type}]->() RETURN r LIMIT 25`}
                      onClick={() => onRelClick(type)}
                    >
                      <span className="graph-rel-arrow">–</span>
                      <span className="graph-rel-type">{type}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </>
        )}

        {section === "history" && (
          <div className="graph-schema-section">
            <div className="graph-schema-heading">History</div>
            {history.length === 0 ? (
              <div className="graph-schema-empty">No history yet</div>
            ) : (
              <div className="graph-history-list">
                {history.map((q, i) => (
                  <div
                    key={i}
                    className="graph-history-item"
                    onClick={() => onHistorySelect(q)}
                    title={q}
                  >
                    {q}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
      <div className="graph-sidebar-resizer" onMouseDown={onMouseDown} />
    </aside>
  );
}

// ── Query bar ─────────────────────────────────────────────────────────────────
interface QueryBarProps {
  onRun: (query: string) => void;
  initialQuery?: string;
}

function QueryBar({ onRun, initialQuery }: QueryBarProps) {
  const [query, setQuery] = useState(initialQuery ?? "MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 50");
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // Auto-grow textarea
  useEffect(() => {
    const el = textareaRef.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${Math.min(el.scrollHeight, 160)}px`;
  }, [query]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
        e.preventDefault();
        onRun(query.trim());
      }
    },
    [query, onRun],
  );

  return (
    <div className="graph-query-bar">
      <div className="graph-query-prompt">$</div>
      <textarea
        ref={textareaRef}
        className="graph-query-input"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onKeyDown={handleKeyDown}
        spellCheck={false}
        placeholder="MATCH (n) RETURN n LIMIT 25"
        rows={1}
      />
      <button
        className="graph-run-btn"
        onClick={() => onRun(query.trim())}
        title="Run query (⌘↵)"
      >
        ▶
      </button>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────
export function GraphPage() {
  const [frames, setFrames] = useState<FrameData[]>([]);
  const [history, setHistory] = useState<string[]>([]);
  const [historyQuery, setHistoryQuery] = useState<string | null>(null);
  const [schemaNodeLabels, setSchemaNodeLabels] = useState<SchemaNodeLabel[]>([]);
  const [schemaRels, setSchemaRels] = useState<SchemaRel[]>([]);
  const [schemaLoading, setSchemaLoading] = useState(true);
  const [sidebarWidth, setSidebarWidth] = useState(240);
  const frameIdRef = useRef(0);

  // Fetch schema on mount via dedicated graph-schema endpoint
  useEffect(() => {
    fetch("/data/graph-schema")
      .then((r) => r.json())
      .then((data) => {
        const nodeLabels: SchemaNodeLabel[] = (data.node_labels ?? []).map(
          (n: { domain_label: string | null; table_label: string; properties: string[] }) => ({
            domainLabel: n.domain_label ?? null,
            tableLabel: n.table_label,
            properties: n.properties ?? [],
          })
        );
        const rels: SchemaRel[] = (data.relationship_types ?? []).map((r: { type: string; source: string; target: string }) => ({
          type: r.type,
          source: r.source ?? "",
          target: r.target ?? "",
        }));
        setSchemaNodeLabels(nodeLabels);
        setSchemaRels(rels.sort((a, b) => a.type.localeCompare(b.type)));
      })
      .catch(() => {})
      .finally(() => setSchemaLoading(false));
  }, []);

  const runQuery = useCallback(async (query: string) => {
    if (!query) return;
    const id = String(++frameIdRef.current);
    const start = Date.now();
    setFrames((f) => [
      { id, query, status: "loading", nodes: new Map(), edges: new Map(), rows: [], columns: [] },
      ...f,
    ]);
    setHistory((h) => [query, ...h.filter((q) => q !== query).slice(0, 49)]);
    try {
      const res = await fetch("/data/cypher", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query, params: {} }),
      });
      const elapsed = Date.now() - start;
      if (!res.ok) {
        const text = await res.text();
        let msg: string;
        try { msg = (JSON.parse(text) as { error?: string }).error ?? text; } catch { msg = text; }
        setFrames((f) => f.map((fr) => fr.id === id ? { ...fr, status: "error", error: msg } : fr));
        return;
      }
      const data = await res.json();
      const rows: Record<string, unknown>[] = data.rows ?? [];
      const columns: string[] = data.columns ?? [];
      const { nodes, edges } = extractElements(rows);
      setFrames((f) => f.map((fr) => fr.id === id ? { ...fr, status: "done", nodes, edges, rows, columns, elapsed } : fr));
    } catch (err) {
      setFrames((f) => f.map((fr) => fr.id === id ? { ...fr, status: "error", error: String(err) } : fr));
    }
  }, []);

  const closeFrame = useCallback((id: string) => {
    setFrames((f) => f.filter((fr) => fr.id !== id));
  }, []);

  const rerunFrame = useCallback(async (id: string, query: string) => {
    if (!query) return;
    const start = Date.now();
    setFrames((f) => f.map((fr) =>
      fr.id === id
        ? { ...fr, query, status: "loading", nodes: new Map(), edges: new Map(), rows: [], columns: [], elapsed: undefined, error: undefined }
        : fr
    ));
    try {
      const res = await fetch("/data/cypher", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query, params: {} }),
      });
      const elapsed = Date.now() - start;
      if (!res.ok) {
        const text = await res.text();
        let msg: string;
        try { msg = (JSON.parse(text) as { error?: string }).error ?? text; } catch { msg = text; }
        setFrames((f) => f.map((fr) => fr.id === id ? { ...fr, status: "error", error: msg } : fr));
        return;
      }
      const data = await res.json();
      const rows: Record<string, unknown>[] = data.rows ?? [];
      const columns: string[] = data.columns ?? [];
      const { nodes, edges } = extractElements(rows);
      setFrames((f) => f.map((fr) => fr.id === id ? { ...fr, status: "done", nodes, edges, rows, columns, elapsed } : fr));
    } catch (err) {
      setFrames((f) => f.map((fr) => fr.id === id ? { ...fr, status: "error", error: String(err) } : fr));
    }
  }, []);

  const handleHistorySelect = useCallback((q: string) => setHistoryQuery(q), []);
  const handleLabelClick = useCallback((label: string) => {
    runQuery(`MATCH (n:${label}) RETURN n LIMIT 25`);
  }, [runQuery]);

  const handleRelClick = useCallback((type: string) => {
    runQuery(`MATCH ()-[r:${type}]->() RETURN r LIMIT 25`);
  }, [runQuery]);

  return (
    <div className="graph-page">
      <Sidebar
        schemaNodeLabels={schemaNodeLabels}
        schemaRels={schemaRels}
        schemaLoading={schemaLoading}
        history={history}
        onHistorySelect={handleHistorySelect}
        onLabelClick={handleLabelClick}
        onRelClick={handleRelClick}
        width={sidebarWidth}
        onWidthChange={setSidebarWidth}
      />

      <div className="graph-content">
        <QueryBar
          onRun={runQuery}
          initialQuery={historyQuery ?? undefined}
          key={historyQuery ?? "initial"}
        />

        <div className="graph-stream">
          {frames.length === 0 && (
            <div className="graph-stream-empty">
              <div className="graph-stream-empty-icon">⬡</div>
              <div>Run a Cypher query to explore the graph</div>
              <div className="graph-stream-hint">⌘↵ to run</div>
            </div>
          )}
          {frames.map((frame) => (
            <GraphFrame key={frame.id} frame={frame} onClose={closeFrame} onRerun={rerunFrame} />
          ))}
        </div>
      </div>
    </div>
  );
}
