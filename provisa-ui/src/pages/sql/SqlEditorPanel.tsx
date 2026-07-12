// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { Play, Copy, Check, X, Sparkles } from "lucide-react";
import { format as formatSql } from "sql-formatter";
import CodeMirror from "@uiw/react-codemirror";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView } from "@codemirror/view";
import type { Extension } from "@codemirror/state";
import { nlToSql } from "../../api/admin";
import type { SqlTab } from "./types";
import type { RegisteredTable } from "../../types/admin";

interface SqlEditorPanelProps {
  // Tab strip
  tabs: SqlTab[];
  activeTabId: string;
  editingTabId: string | null;
  editingTitle: string;
  setEditingTabId: React.Dispatch<React.SetStateAction<string | null>>;
  setEditingTitle: React.Dispatch<React.SetStateAction<string>>;
  switchTab: (id: string) => void;
  addTab: () => void;
  closeTab: (id: string) => void;
  renameTab: (id: string, title: string) => void;
  // NL strip
  nlText: string;
  setNlText: React.Dispatch<React.SetStateAction<string>>;
  nlLoading: boolean;
  setNlLoading: React.Dispatch<React.SetStateAction<boolean>>;
  nlError: string;
  setNlError: React.Dispatch<React.SetStateAction<string>>;
  setSqlText: React.Dispatch<React.SetStateAction<string>>;
  role: string;
  // Editor
  sqlText: string;
  sqlExtensions: Extension[];
  editorViewRef: React.MutableRefObject<EditorView | null>;
  copied: boolean;
  handleCopy: () => void;
  // Toolbar
  running: boolean;
  handleRun: () => void;
  sampleMode: "first" | "last" | "random";
  setSampleMode: React.Dispatch<React.SetStateAction<"first" | "last" | "random">>;
  sampleSize: number;
  setSampleSize: React.Dispatch<React.SetStateAction<number>>;
  roles: string[];
  setRole: React.Dispatch<React.SetStateAction<string>>;
  viewTable: RegisteredTable | null;
  viewSaving: boolean;
  setViewSaving: React.Dispatch<React.SetStateAction<boolean>>;
  updateTable: (input: Record<string, unknown>) => Promise<{ success: boolean; message: string }>;
  canCreateView: boolean;
  canRequestView: boolean;
  onOpenViewModal: () => void;
}

export function SqlEditorPanel({
  tabs,
  activeTabId,
  editingTabId,
  editingTitle,
  setEditingTabId,
  setEditingTitle,
  switchTab,
  addTab,
  closeTab,
  renameTab,
  nlText,
  setNlText,
  nlLoading,
  setNlLoading,
  nlError,
  setNlError,
  setSqlText,
  role,
  sqlText,
  sqlExtensions,
  editorViewRef,
  copied,
  handleCopy,
  running,
  handleRun,
  sampleMode,
  setSampleMode,
  sampleSize,
  setSampleSize,
  roles,
  setRole,
  viewTable,
  viewSaving,
  setViewSaving,
  updateTable,
  canCreateView,
  canRequestView,
  onOpenViewModal,
}: SqlEditorPanelProps) {
  return (
    <>
      {/* Query tab strip */}
      <div
        style={{
          display: "flex",
          alignItems: "stretch",
          gap: "2px",
          padding: "0.25rem 0.5rem 0",
          borderBottom: "1px solid var(--border)",
          background: "var(--surface)",
          flexShrink: 0,
          overflowX: "auto",
        }}
      >
        {tabs.map((t) => {
          const isActive = t.id === activeTabId;
          return (
            <div
              key={t.id}
              onClick={() => switchTab(t.id)}
              onDoubleClick={() => {
                setEditingTabId(t.id);
                setEditingTitle(t.title);
              }}
              title="Double-click to rename"
              style={{
                display: "flex",
                alignItems: "center",
                gap: "0.35rem",
                padding: "0.25rem 0.5rem",
                fontSize: "0.75rem",
                cursor: "pointer",
                whiteSpace: "nowrap",
                borderTopLeftRadius: "5px",
                borderTopRightRadius: "5px",
                border: "1px solid var(--border)",
                borderBottom: isActive ? "1px solid var(--bg)" : "1px solid var(--border)",
                marginBottom: "-1px",
                background: isActive ? "var(--bg)" : "transparent",
                color: isActive ? "var(--text)" : "var(--text-muted)",
                fontWeight: isActive ? 600 : 400,
              }}
            >
              {editingTabId === t.id ? (
                <input
                  autoFocus
                  value={editingTitle}
                  onClick={(e) => e.stopPropagation()}
                  onChange={(e) => setEditingTitle(e.target.value)}
                  onBlur={() => {
                    const v = editingTitle.trim();
                    if (v) renameTab(t.id, v);
                    setEditingTabId(null);
                  }}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      const v = editingTitle.trim();
                      if (v) renameTab(t.id, v);
                      setEditingTabId(null);
                    } else if (e.key === "Escape") {
                      setEditingTabId(null);
                    }
                  }}
                  style={{
                    width: "80px",
                    fontSize: "0.75rem",
                    padding: "0 0.2rem",
                    border: "1px solid var(--primary)",
                    borderRadius: "3px",
                    background: "var(--bg)",
                    color: "var(--text)",
                    outline: "none",
                  }}
                />
              ) : (
                <span>{t.title}</span>
              )}
              <span
                role="button"
                aria-label={`Close ${t.title}`}
                onClick={(e) => {
                  e.stopPropagation();
                  closeTab(t.id);
                }}
                style={{
                  display: "flex",
                  alignItems: "center",
                  opacity: 0.6,
                  borderRadius: "3px",
                }}
              >
                <X size={11} />
              </span>
            </div>
          );
        })}
        <button
          onClick={addTab}
          aria-label="New query tab"
          title="New query tab"
          style={{
            padding: "0.25rem 0.5rem",
            fontSize: "0.85rem",
            lineHeight: 1,
            background: "none",
            border: "none",
            color: "var(--text-muted)",
            cursor: "pointer",
          }}
        >
          +
        </button>
      </div>

      {/* NL-to-SQL strip */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "0.5rem",
          padding: "0.35rem 0.75rem",
          borderBottom: "1px solid var(--border)",
          background: "var(--surface)",
          flexShrink: 0,
        }}
      >
        <div
          className="nl-input-wrap"
          style={{ flex: 1, position: "relative", display: "flex", alignItems: "center" }}
        >
          <input
            type="text"
            placeholder="Ask in plain English — generates SQL…"
            value={nlText}
            onChange={(e) => {
              setNlText(e.target.value);
              setNlError("");
            }}
            onKeyDown={async (e) => {
              if (e.key === "Enter" && nlText.trim() && !nlLoading) {
                setNlLoading(true);
                setNlError("");
                const result = await nlToSql(nlText.trim(), role);
                setNlLoading(false);
                if (result.error) {
                  setNlError(result.error);
                } else {
                  setSqlText(result.sql);
                }
              }
            }}
            style={{
              width: "100%",
              fontSize: "0.8rem",
              padding: "0.25rem 1.6rem 0.25rem 0.5rem",
              borderRadius: "4px",
              border: "1px solid var(--border)",
              background: "var(--bg)",
              color: "var(--text)",
              outline: "none",
            }}
          />
          {nlText && (
            <button
              onClick={() => {
                setNlText("");
                setNlError("");
              }}
              title="Clear"
              className="nl-clear-btn"
              style={{
                position: "absolute",
                right: "0.3rem",
                background: "none",
                border: "none",
                padding: "0.1rem",
                cursor: "pointer",
                color: "var(--text-muted)",
                display: "flex",
                alignItems: "center",
                lineHeight: 1,
              }}
            >
              <X size={12} />
            </button>
          )}
        </div>
        <button
          className="btn-primary"
          disabled={nlLoading || !nlText.trim()}
          onClick={async () => {
            setNlLoading(true);
            setNlError("");
            const result = await nlToSql(nlText.trim(), role);
            setNlLoading(false);
            if (result.error) {
              setNlError(result.error);
            } else {
              setSqlText(result.sql);
            }
          }}
          style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem", whiteSpace: "nowrap" }}
        >
          {nlLoading ? "Generating…" : "Generate SQL"}
        </button>
        {nlError && (
          <span
            style={{
              fontSize: "0.75rem",
              color: "var(--error)",
              maxWidth: "300px",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
            title={nlError}
          >
            {nlError}
          </span>
        )}
      </div>

      {/* Editor */}
      <div
        style={{
          flex: "0 0 220px",
          overflow: "hidden",
          borderBottom: "1px solid var(--border)",
          position: "relative",
        }}
        onMouseEnter={(e) => {
          e.currentTarget
            .querySelectorAll<HTMLElement>(".hover-sql-btn")
            .forEach((btn) => (btn.style.opacity = "1"));
        }}
        onMouseLeave={(e) => {
          e.currentTarget
            .querySelectorAll<HTMLElement>(".hover-sql-btn")
            .forEach((btn) => (btn.style.opacity = "0"));
        }}
      >
        {nlLoading && (
          <div
            style={{
              position: "absolute",
              inset: 0,
              zIndex: 10,
              background: "rgba(0,0,0,0.45)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              gap: "0.5rem",
              color: "var(--text-muted)",
              fontSize: "0.8rem",
              pointerEvents: "none",
            }}
          >
            <span
              style={{
                display: "inline-block",
                width: "14px",
                height: "14px",
                border: "2px solid var(--text-muted)",
                borderTopColor: "transparent",
                borderRadius: "50%",
                animation: "spin 0.7s linear infinite",
              }}
            />
            Generating SQL…
          </div>
        )}
        <CodeMirror
          value={sqlText}
          height="220px"
          theme={oneDark}
          extensions={sqlExtensions}
          onChange={(v) => {
            setSqlText(v);
          }}
          onCreateEditor={(view) => {
            editorViewRef.current = view;
          }}
          style={{ fontSize: "0.8rem" }}
        />
        <button
          className="hover-sql-btn"
          onClick={() => {
            try {
              setSqlText(formatSql(sqlText, { language: "postgresql" }));
            } catch {
              /* leave SQL unchanged if it can't be parsed */
            }
          }}
          title="Prettify SQL"
          disabled={!sqlText.trim()}
          style={{
            position: "absolute",
            top: "0.4rem",
            right: "4.2rem",
            opacity: 0,
            transition: "opacity 0.15s",
            background: "rgba(30,30,40,0.85)",
            border: "1px solid var(--border)",
            borderRadius: "4px",
            color: "var(--text-muted)",
            cursor: "pointer",
            padding: "0.2rem 0.35rem",
            display: "flex",
            alignItems: "center",
            gap: "0.25rem",
            fontSize: "0.72rem",
          }}
        >
          <Sparkles size={11} />
          Prettify
        </button>
        <button
          className="hover-sql-btn"
          onClick={handleCopy}
          title="Copy SQL"
          style={{
            position: "absolute",
            top: "0.4rem",
            right: "0.4rem",
            opacity: 0,
            transition: "opacity 0.15s",
            background: "rgba(30,30,40,0.85)",
            border: "1px solid var(--border)",
            borderRadius: "4px",
            color: "var(--text-muted)",
            cursor: "pointer",
            padding: "0.2rem 0.35rem",
            display: "flex",
            alignItems: "center",
            gap: "0.25rem",
            fontSize: "0.72rem",
          }}
        >
          {copied ? (
            <Check size={11} style={{ color: "var(--approve)" }} />
          ) : (
            <Copy size={11} />
          )}
          {copied ? "Copied" : "Copy"}
        </button>
      </div>

      {/* Toolbar */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "0.5rem",
          padding: "0.4rem 0.75rem",
          borderBottom: "1px solid var(--border)",
          flexShrink: 0,
          background: "var(--surface)",
        }}
      >
        <button
          className="btn-primary"
          style={{
            display: "flex",
            alignItems: "center",
            gap: "0.3rem",
            fontSize: "0.8rem",
            padding: "0.25rem 0.6rem",
          }}
          onClick={handleRun}
          disabled={running || !sqlText.trim()}
        >
          <Play size={11} />
          {running ? "Running…" : "Sample >"}
        </button>
        {viewTable && (
          <button
            className="btn-primary"
            style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
            title={`Save current SQL as the definition for view "${viewTable.tableName}"`}
            disabled={viewSaving || !sqlText.trim()}
            onClick={async () => {
              setViewSaving(true);
              await updateTable({
                sourceId: viewTable.sourceId,
                domainId: viewTable.domainId ?? "",
                schemaName: viewTable.schemaName ?? "",
                tableName: viewTable.tableName,
                alias: viewTable.alias ?? undefined,
                description: viewTable.description ?? undefined,
                watermarkColumn: viewTable.watermarkColumn ?? null,
                viewSql: sqlText.trim(),
                dataProduct: viewTable.dataProduct ?? false,
                columns: viewTable.columns.map((c) => ({
                  name: c.columnName,
                  visibleTo: c.visibleTo ?? [],
                  writableBy: c.writableBy,
                  unmaskedTo: c.unmaskedTo,
                  maskType: c.maskType ?? undefined,
                  maskPattern: c.maskPattern ?? undefined,
                  maskReplace: c.maskReplace ?? undefined,
                  maskValue: c.maskValue ?? undefined,
                  maskPrecision: c.maskPrecision ?? undefined,
                  alias: c.alias ?? undefined,
                  description: c.description ?? undefined,
                  nativeFilterType: c.nativeFilterType ?? undefined,
                  isPrimaryKey: c.isPrimaryKey ?? undefined,
                  isForeignKey: c.isForeignKey ?? undefined,
                  isAlternateKey: c.isAlternateKey ?? undefined,
                  scope: c.scope ?? "domain",
                })),
              });
              setViewSaving(false);
            }}
          >
            {viewSaving ? "Saving…" : `Update "${viewTable.tableName}"`}
          </button>
        )}
        <select
          value={sampleMode}
          onChange={(e) => setSampleMode(e.target.value as "first" | "last" | "random")}
          className="toolbar-select"
        >
          <option value="first">First</option>
          <option value="last">Last</option>
          <option value="random">Random</option>
        </select>
        <input
          type="number"
          value={sampleSize}
          min={1}
          max={10000}
          onChange={(e) => setSampleSize(Math.max(1, parseInt(e.target.value) || 100))}
          className="toolbar-input"
          style={{ width: "90px" }}
          title="Row count"
        />
        <select
          value={role}
          onChange={(e) => setRole(e.target.value)}
          className="toolbar-select"
        >
          {roles.map((r) => (
            <option key={r} value={r}>
              {r}
            </option>
          ))}
        </select>
        <div style={{ flex: 1 }} />
        {(canCreateView || canRequestView) && sqlText.trim() && (
          <button
            className="btn-secondary"
            style={{ fontSize: "0.78rem", padding: "0.25rem 0.6rem" }}
            onClick={onOpenViewModal}
          >
            {canCreateView ? "+ View" : "Request View"}
          </button>
        )}
      </div>
    </>
  );
}
