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
import { ChevronRight, ChevronDown, Table2, Columns3 } from "lucide-react";
import { normalizeDomain } from "./sqlHelpers";
import { DOMAIN_PAGE_SIZE } from "./types";
import type { TopTab } from "./types";
import type { Domain } from "../../types/admin";
import type { RegisteredTable } from "../../types/admin";

interface SchemaBrowserProps {
  sidebarOpen: boolean;
  setSidebarOpen: React.Dispatch<React.SetStateAction<boolean>>;
  domainGroups: Record<string, RegisteredTable[]>;
  domainMap: Record<string, Domain>;
  expandedDomains: Set<string>;
  expandedTables: Set<string>;
  domainPages: Record<string, number>;
  topTab: TopTab;
  insertAtCursor: (text: string) => void;
  toggleDomain: (d: string) => void;
  toggleTable: (t: string) => void;
  setDomainPages: React.Dispatch<React.SetStateAction<Record<string, number>>>;
}

export function SchemaBrowser({
  sidebarOpen,
  setSidebarOpen,
  domainGroups,
  domainMap,
  expandedDomains,
  expandedTables,
  domainPages,
  topTab,
  insertAtCursor,
  toggleDomain,
  toggleTable,
  setDomainPages,
}: SchemaBrowserProps) {
  return (
    <div
      style={{
        display: "flex",
        flexShrink: 0,
        borderRight: "1px solid var(--border)",
        position: "relative",
      }}
    >
      <button
        onClick={() => setSidebarOpen((v) => !v)}
        title={sidebarOpen ? "Collapse schema panel" : "Expand schema panel"}
        style={{
          position: "absolute",
          right: -13,
          top: "50%",
          transform: "translateY(-50%)",
          zIndex: 30,
          width: 13,
          height: 40,
          background: "var(--surface)",
          border: "1px solid var(--border)",
          borderLeft: "none",
          borderRadius: "0 4px 4px 0",
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: 0,
          color: "var(--text-muted)",
          fontSize: "0.55rem",
        }}
      >
        {sidebarOpen ? "‹" : "›"}
      </button>

      <div
        style={{
          width: sidebarOpen ? 210 : 0,
          overflow: "hidden",
          transition: "width 0.18s ease",
          background: "var(--bg)",
        }}
      >
        <div style={{ width: 210, overflow: "auto", height: "100%", padding: "0.5rem 0" }}>
          <div
            style={{
              padding: "0 0.75rem 0.4rem",
              fontSize: "0.65rem",
              fontWeight: 700,
              letterSpacing: "0.08em",
              textTransform: "uppercase",
              color: "var(--text-muted)",
            }}
          >
            Schema
          </div>
          {Object.entries(domainGroups).map(([domain, domainTables]) => {
            const domainOpen = expandedDomains.has(domain);
            return (
              <div key={domain}>
                <button
                  onClick={() => toggleDomain(domain)}
                  title={domainMap[domain]?.description || undefined}
                  style={{
                    width: "100%",
                    textAlign: "left",
                    background: "none",
                    border: "none",
                    cursor: "pointer",
                    padding: "0.2rem 0.75rem",
                    display: "flex",
                    alignItems: "center",
                    gap: "0.25rem",
                    color: "var(--text-muted)",
                    fontSize: "0.75rem",
                    fontWeight: 600,
                  }}
                >
                  {domainOpen ? <ChevronDown size={10} /> : <ChevronRight size={10} />}
                  <span
                    style={{
                      flex: 1,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {domain}
                  </span>
                  {domainMap[domain]?.description && (
                    <span
                      style={{
                        flexShrink: 0,
                        color: "var(--text-muted)",
                        opacity: 0.5,
                        fontSize: "0.65rem",
                        lineHeight: 1,
                      }}
                    >
                      ⓘ
                    </span>
                  )}
                </button>
                {domainOpen &&
                  (() => {
                    const dp = domainPages[domain] ?? 0;
                    const totalDomainPages = Math.ceil(domainTables.length / DOMAIN_PAGE_SIZE);
                    const paged = domainTables.slice(
                      dp * DOMAIN_PAGE_SIZE,
                      (dp + 1) * DOMAIN_PAGE_SIZE,
                    );
                    return (
                      <>
                        {paged.map((t) => {
                          const tOpen = expandedTables.has(t.tableName);
                          return (
                            <div key={t.tableName}>
                              <div style={{ display: "flex", alignItems: "center" }}>
                                <button
                                  onClick={() => toggleTable(t.tableName)}
                                  draggable={topTab === "canvas"}
                                  onDragStart={
                                    topTab === "canvas"
                                      ? (e) => e.dataTransfer.setData("tableName", t.tableName)
                                      : undefined
                                  }
                                  style={{
                                    flex: 1,
                                    minWidth: 0,
                                    textAlign: "left",
                                    background: "none",
                                    border: "none",
                                    cursor: topTab === "canvas" ? "grab" : "pointer",
                                    padding: "0.18rem 0 0.18rem 1.5rem",
                                    display: "flex",
                                    alignItems: "center",
                                    gap: "0.3rem",
                                    color: "var(--text)",
                                    fontSize: "0.75rem",
                                  }}
                                  title={
                                    topTab === "canvas"
                                      ? "Drag to canvas"
                                      : "Double-click to insert qualified name"
                                  }
                                  onDoubleClick={
                                    topTab === "sql"
                                      ? () =>
                                          insertAtCursor(
                                            `"${normalizeDomain(t.domainId || t.schemaName)}"."${t.alias || t.tableName}"`,
                                          )
                                      : undefined
                                  }
                                >
                                  {tOpen ? <ChevronDown size={9} /> : <ChevronRight size={9} />}
                                  <Table2
                                    size={9}
                                    style={{ flexShrink: 0, color: "var(--primary)" }}
                                  />
                                  <span
                                    style={{
                                      overflow: "hidden",
                                      textOverflow: "ellipsis",
                                      whiteSpace: "nowrap",
                                    }}
                                  >
                                    {t.alias || t.tableName}
                                  </span>
                                </button>
                                <button
                                  onClick={() =>
                                    insertAtCursor(
                                      `"${normalizeDomain(t.domainId || t.schemaName)}"."${t.alias || t.tableName}"`,
                                    )
                                  }
                                  title="Insert table reference in SQL editor"
                                  style={{
                                    flexShrink: 0,
                                    background: "none",
                                    border: "none",
                                    cursor: "pointer",
                                    padding: "0 0.35rem 0 0.1rem",
                                    color: "var(--primary)",
                                    fontSize: "0.7rem",
                                    opacity: 0.6,
                                    lineHeight: 1,
                                  }}
                                >
                                  →
                                </button>
                                {t.description && (
                                  <span
                                    title={t.description}
                                    style={{
                                      flexShrink: 0,
                                      paddingRight: "0.35rem",
                                      color: "var(--primary)",
                                      opacity: 0.7,
                                      fontSize: "0.65rem",
                                      cursor: "default",
                                      lineHeight: 1,
                                    }}
                                  >
                                    ⓘ
                                  </span>
                                )}
                              </div>
                              {tOpen &&
                                [
                                  ...t.columns.map((col) => ({
                                    columnName: col.computedSqlAlias,
                                    dataType: col.dataType,
                                    description: col.description,
                                    virtual: false,
                                  })),
                                  {
                                    columnName: "_name_",
                                    dataType: "text",
                                    description: "Table alias/name",
                                    virtual: true,
                                  },
                                  {
                                    columnName: "_domain_",
                                    dataType: "text",
                                    description: "Domain ID",
                                    virtual: true,
                                  },
                                ].map((col) => (
                                  <div
                                    key={col.columnName}
                                    style={{ display: "flex", alignItems: "center" }}
                                  >
                                    <button
                                      onClick={() =>
                                        topTab === "sql"
                                          ? insertAtCursor(
                                              `"${t.alias || t.tableName}"."${col.columnName}"`,
                                            )
                                          : undefined
                                      }
                                      style={{
                                        flex: 1,
                                        minWidth: 0,
                                        textAlign: "left",
                                        background: "none",
                                        border: "none",
                                        cursor: topTab === "sql" ? "pointer" : "default",
                                        padding: "0.15rem 0 0.15rem 2.5rem",
                                        display: "flex",
                                        alignItems: "center",
                                        gap: "0.3rem",
                                        color: col.virtual
                                          ? "var(--text-muted)"
                                          : "var(--text-muted)",
                                        fontSize: "0.72rem",
                                        fontFamily: "monospace",
                                        opacity: col.virtual ? 0.6 : 1,
                                      }}
                                      title={
                                        col.description ??
                                        (topTab === "sql"
                                          ? "Click to insert quoted column"
                                          : undefined)
                                      }
                                    >
                                      <Columns3
                                        size={8}
                                        style={{
                                          flexShrink: 0,
                                          color: col.virtual ? "var(--accent)" : undefined,
                                        }}
                                      />
                                      <span
                                        style={{
                                          overflow: "hidden",
                                          textOverflow: "ellipsis",
                                          whiteSpace: "nowrap",
                                          flex: 1,
                                        }}
                                      >
                                        {col.columnName}
                                      </span>
                                      {col.dataType && (
                                        <span
                                          style={{
                                            flexShrink: 0,
                                            fontSize: "0.6rem",
                                            color: "var(--text-muted)",
                                            opacity: 0.5,
                                            fontFamily: "monospace",
                                            paddingRight: "0.1rem",
                                          }}
                                        >
                                          {col.dataType}
                                        </span>
                                      )}
                                    </button>
                                    {col.description && (
                                      <span
                                        title={col.description}
                                        style={{
                                          flexShrink: 0,
                                          paddingRight: "0.5rem",
                                          color: "var(--text-muted)",
                                          opacity: 0.6,
                                          fontSize: "0.65rem",
                                          cursor: "default",
                                          lineHeight: 1,
                                        }}
                                      >
                                        ⓘ
                                      </span>
                                    )}
                                  </div>
                                ))}
                            </div>
                          );
                        })}
                        {totalDomainPages > 1 && (
                          <div
                            style={{
                              display: "flex",
                              alignItems: "center",
                              gap: "0.25rem",
                              padding: "0.2rem 0.75rem",
                              fontSize: "0.65rem",
                              color: "var(--text-muted)",
                            }}
                          >
                            <button
                              onClick={() =>
                                setDomainPages((p) => ({ ...p, [domain]: dp - 1 }))
                              }
                              disabled={dp === 0}
                              style={{
                                background: "none",
                                border: "none",
                                cursor: dp > 0 ? "pointer" : "default",
                                padding: "0.1rem 0.2rem",
                                color: "var(--text-muted)",
                                opacity: dp > 0 ? 1 : 0.4,
                              }}
                            >
                              ‹
                            </button>
                            <span>
                              {dp * DOMAIN_PAGE_SIZE + 1}–
                              {Math.min((dp + 1) * DOMAIN_PAGE_SIZE, domainTables.length)} /{" "}
                              {domainTables.length}
                            </span>
                            <button
                              onClick={() =>
                                setDomainPages((p) => ({ ...p, [domain]: dp + 1 }))
                              }
                              disabled={dp >= totalDomainPages - 1}
                              style={{
                                background: "none",
                                border: "none",
                                cursor: dp < totalDomainPages - 1 ? "pointer" : "default",
                                padding: "0.1rem 0.2rem",
                                color: "var(--text-muted)",
                                opacity: dp < totalDomainPages - 1 ? 1 : 0.4,
                              }}
                            >
                              ›
                            </button>
                          </div>
                        )}
                      </>
                    );
                  })()}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
