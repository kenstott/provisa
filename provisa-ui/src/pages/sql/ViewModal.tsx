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
import { X, Loader2 } from "lucide-react";
import CodeMirror from "@uiw/react-codemirror";
import { oneDark } from "@codemirror/theme-one-dark";
import type { Extension } from "@codemirror/state";
import type { ViewColumnConfig } from "./types";
import type { Domain } from "../../types/admin";

interface ViewModalProps {
  viewModal: boolean;
  setViewModal: React.Dispatch<React.SetStateAction<boolean>>;
  viewMsg: string;
  canCreateView: boolean;
  handleSaveView: () => void;
  viewSaving: boolean;
  viewId: string;
  setViewId: React.Dispatch<React.SetStateAction<string>>;
  viewDomainId: string;
  setViewDomainId: React.Dispatch<React.SetStateAction<string>>;
  viewHasParams: boolean;
  viewDescription: string;
  setViewDescription: React.Dispatch<React.SetStateAction<string>>;
  viewSqlNormalized: string;
  viewSqlExtensions: Extension[];
  domainMap: Record<string, Domain>;
  savedViewId: number | null;
  setSavedViewId: React.Dispatch<React.SetStateAction<number | null>>;
  setViewColumns: React.Dispatch<React.SetStateAction<ViewColumnConfig[]>>;
  onNavigateToViews: () => void;
  onCloseConfirmation: () => void;
}

export function ViewModal({
  viewModal,
  setViewModal,
  viewMsg,
  canCreateView,
  handleSaveView,
  viewSaving,
  viewId,
  setViewId,
  viewDomainId,
  setViewDomainId,
  viewHasParams,
  viewDescription,
  setViewDescription,
  viewSqlNormalized,
  viewSqlExtensions,
  domainMap,
  savedViewId,
  setSavedViewId,
  setViewColumns,
  onNavigateToViews,
  onCloseConfirmation,
}: ViewModalProps) {
  return (
    <>
      {viewModal && (
        <div className="modal-overlay" onClick={() => setViewModal(false)}>
          <div
            className="modal"
            style={{
              width: "90vw",
              maxWidth: "90vw",
              maxHeight: "90vh",
              display: "flex",
              flexDirection: "column",
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                marginBottom: "1rem",
                flexShrink: 0,
              }}
            >
              <span style={{ fontWeight: 600, fontSize: "0.9rem" }}>
                {canCreateView ? "+ View" : "Request View"}
              </span>
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                {viewMsg && (
                  <span
                    style={{
                      fontSize: "0.78rem",
                      color: viewMsg.startsWith("Error") ? "var(--destructive)" : "var(--approve)",
                    }}
                  >
                    {viewMsg}
                  </span>
                )}
                <button
                  className="btn-primary"
                  onClick={handleSaveView}
                  disabled={viewSaving || !viewId.trim() || !viewDomainId.trim() || viewHasParams}
                  style={{ fontSize: "0.8rem", padding: "0.3rem 0.75rem" }}
                >
                  {viewSaving ? (
                    <>
                      <Loader2
                        size={12}
                        style={{ animation: "spin 1s linear infinite", marginRight: 4 }}
                      />
                      Saving…
                    </>
                  ) : canCreateView ? (
                    "Create"
                  ) : (
                    "Submit Request"
                  )}
                </button>
                <button className="modal-close" onClick={() => setViewModal(false)}>
                  <X size={14} />
                </button>
              </div>
            </div>
            {!canCreateView && (
              <p
                style={{
                  fontSize: "0.78rem",
                  color: "var(--text-muted)",
                  marginBottom: "0.75rem",
                  flexShrink: 0,
                }}
              >
                You do not have <code>create_view</code>. This will be submitted as a suggested view
                pending approval.
              </p>
            )}
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                gap: "0.6rem",
                overflow: "auto",
                flex: 1,
                paddingRight: "1rem",
                paddingLeft: "2px",
              }}
            >
              <div style={{ display: "flex", gap: "0.75rem", flexShrink: 0 }}>
                <label
                  style={{
                    display: "flex",
                    flexDirection: "column",
                    gap: "0.25rem",
                    fontSize: "0.875rem",
                    color: "var(--text-muted)",
                    flex: 1,
                    minWidth: 0,
                  }}
                >
                  <span style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}>
                    Alias <span style={{ color: "var(--destructive)" }}>*</span>
                  </span>
                  <input
                    value={viewId}
                    onChange={(e) => setViewId(e.target.value)}
                    placeholder="e.g. my_view"
                    style={{
                      background: "var(--bg)",
                      color: "var(--text)",
                      border: "1px solid var(--border)",
                      padding: "0.5rem",
                      borderRadius: 4,
                      fontSize: "0.875rem",
                      width: "100%",
                      boxSizing: "border-box",
                    }}
                  />
                </label>
                <label
                  style={{
                    display: "flex",
                    flexDirection: "column",
                    gap: "0.25rem",
                    fontSize: "0.875rem",
                    color: "var(--text-muted)",
                    flex: 1,
                    minWidth: 0,
                  }}
                >
                  <span style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}>
                    Domain <span style={{ color: "var(--destructive)" }}>*</span>
                  </span>
                  <select
                    value={viewDomainId}
                    onChange={(e) => setViewDomainId(e.target.value)}
                    style={{
                      background: "var(--bg)",
                      color: "var(--text)",
                      border: "1px solid var(--border)",
                      padding: "0.5rem",
                      borderRadius: 4,
                      fontSize: "0.875rem",
                      width: "100%",
                      boxSizing: "border-box",
                    }}
                  >
                    <option value="">— select domain —</option>
                    {Object.values(domainMap)
                      .filter((d) => d.id && d.id !== "meta" && d.id !== "ops")
                      .map((d) => (
                        <option key={d.id} value={d.id}>
                          {d.id}
                          {d.description ? ` — ${d.description}` : ""}
                        </option>
                      ))}
                  </select>
                </label>
              </div>
              <label
                style={{
                  display: "flex",
                  flexDirection: "column",
                  gap: "0.25rem",
                  fontSize: "0.875rem",
                  color: "var(--text-muted)",
                  flexShrink: 0,
                }}
              >
                Description
                <textarea
                  value={viewDescription}
                  onChange={(e) => setViewDescription(e.target.value)}
                  placeholder="Optional"
                  rows={2}
                  style={{
                    resize: "vertical",
                    background: "var(--bg)",
                    color: "var(--text)",
                    border: "1px solid var(--border)",
                    padding: "0.5rem",
                    borderRadius: 4,
                    fontSize: "0.875rem",
                    width: "100%",
                    boxSizing: "border-box",
                  }}
                />
              </label>
              {viewHasParams && (
                <p
                  style={{
                    fontSize: "0.78rem",
                    color: "var(--destructive)",
                    margin: 0,
                    flexShrink: 0,
                  }}
                >
                  SQL contains unresolved parameter placeholders ($1, $2, …). Edit the SQL to
                  replace them with literal values.
                </p>
              )}
              <div
                style={{
                  resize: "vertical",
                  overflow: "auto",
                  minHeight: 80,
                  height: 120,
                  flexShrink: 0,
                }}
              >
                <CodeMirror
                  value={viewSqlNormalized}
                  extensions={viewSqlExtensions}
                  theme={oneDark}
                  editable={false}
                  height="100%"
                  basicSetup={{ lineNumbers: false, foldGutter: false }}
                />
              </div>
            </div>
          </div>
        </div>
      )}
      {savedViewId !== null && (
        <div
          className="modal-overlay"
          onClick={() => {
            setSavedViewId(null);
            setViewModal(false);
            setViewColumns([]);
            onCloseConfirmation();
          }}
        >
          <div
            className="modal"
            style={{ width: "400px", padding: "2rem" }}
            onClick={(e) => e.stopPropagation()}
          >
            <h3 style={{ marginTop: 0, marginBottom: "1rem" }}>View Saved</h3>
            <p style={{ marginBottom: "1.5rem", color: "var(--text-muted)" }}>
              {canCreateView
                ? `View "${viewId}" has been created and registered.`
                : `View "${viewId}" has been submitted for approval.`}
            </p>
            <div style={{ display: "flex", gap: "0.75rem", justifyContent: "flex-end" }}>
              <button
                className="btn-secondary"
                onClick={() => {
                  setSavedViewId(null);
                  setViewModal(false);
                  setViewColumns([]);
                  onCloseConfirmation();
                }}
                style={{ fontSize: "0.875rem", padding: "0.4rem 1rem" }}
              >
                Close
              </button>
              <button
                className="btn-primary"
                onClick={() => {
                  setSavedViewId(null);
                  setViewModal(false);
                  setViewColumns([]);
                  onCloseConfirmation();
                  onNavigateToViews();
                }}
                style={{ fontSize: "0.875rem", padding: "0.4rem 1rem" }}
              >
                Edit in Model/Views
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
