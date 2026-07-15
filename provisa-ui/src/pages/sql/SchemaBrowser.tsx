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
import { ActionIcon, ScrollArea, Text, Tooltip, UnstyledButton } from "@mantine/core";
import { ChevronRight, ChevronDown, Table2, Columns3, Info } from "lucide-react";
import { useTranslation } from "react-i18next";
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
  const { t } = useTranslation();
  return (
    <div
      style={{
        display: "flex",
        flexShrink: 0,
        borderRight: "1px solid var(--border)",
        position: "relative",
      }}
    >
      <ActionIcon
        onClick={() => setSidebarOpen((v) => !v)}
        aria-label={sidebarOpen ? t("schemaBrowser.collapsePanel") : t("schemaBrowser.expandPanel")}
        title={sidebarOpen ? t("schemaBrowser.collapsePanel") : t("schemaBrowser.expandPanel")}
        data-testid="schema-browser-toggle"
        variant="default"
        radius={0}
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
          color: "var(--text-muted)",
          fontSize: "0.55rem",
        }}
      >
        {sidebarOpen ? "‹" : "›"}
      </ActionIcon>

      <div
        style={{
          width: sidebarOpen ? 210 : 0,
          overflow: "hidden",
          transition: "width 0.18s ease",
          background: "var(--bg)",
        }}
      >
        <ScrollArea style={{ width: 210, height: "100%" }} type="auto">
          <div style={{ padding: "0.5rem 0" }}>
            <Text
              component="div"
              style={{
                padding: "0 0.75rem 0.4rem",
                fontSize: "0.65rem",
                fontWeight: 700,
                letterSpacing: "0.08em",
                textTransform: "uppercase",
                color: "var(--text-muted)",
              }}
            >
              {t("schemaBrowser.heading")}
            </Text>
            {Object.entries(domainGroups).map(([domain, domainTables]) => {
              const domainOpen = expandedDomains.has(domain);
              return (
                <div key={domain}>
                  <UnstyledButton
                    onClick={() => toggleDomain(domain)}
                    title={domainMap[domain]?.description || undefined}
                    aria-expanded={domainOpen}
                    data-testid={`schema-domain-${domain}`}
                    style={{
                      width: "100%",
                      textAlign: "left",
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
                      <Tooltip label={domainMap[domain]?.description} withinPortal>
                        <Info
                          size={10}
                          aria-hidden
                          style={{
                            flexShrink: 0,
                            color: "var(--text-muted)",
                            opacity: 0.5,
                          }}
                        />
                      </Tooltip>
                    )}
                  </UnstyledButton>
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
                          {paged.map((tbl) => {
                            const tOpen = expandedTables.has(tbl.tableName);
                            return (
                              <div key={tbl.tableName}>
                                <div style={{ display: "flex", alignItems: "center" }}>
                                  <UnstyledButton
                                    onClick={() => toggleTable(tbl.tableName)}
                                    draggable={topTab === "canvas"}
                                    onDragStart={
                                      topTab === "canvas"
                                        ? (e) => e.dataTransfer.setData("tableName", tbl.tableName)
                                        : undefined
                                    }
                                    data-testid={`schema-table-${tbl.tableName}`}
                                    aria-expanded={tOpen}
                                    style={{
                                      flex: 1,
                                      minWidth: 0,
                                      textAlign: "left",
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
                                        ? t("schemaBrowser.dragToCanvas")
                                        : t("schemaBrowser.insertQualifiedName")
                                    }
                                    onDoubleClick={
                                      topTab === "sql"
                                        ? () =>
                                            insertAtCursor(
                                              `"${normalizeDomain(tbl.domainId || tbl.schemaName)}"."${tbl.alias || tbl.tableName}"`,
                                            )
                                        : undefined
                                    }
                                  >
                                    {tOpen ? <ChevronDown size={9} /> : <ChevronRight size={9} />}
                                    <Table2
                                      size={9}
                                      aria-hidden
                                      style={{ flexShrink: 0, color: "var(--primary)" }}
                                    />
                                    <span
                                      style={{
                                        overflow: "hidden",
                                        textOverflow: "ellipsis",
                                        whiteSpace: "nowrap",
                                      }}
                                    >
                                      {tbl.alias || tbl.tableName}
                                    </span>
                                  </UnstyledButton>
                                  <ActionIcon
                                    onClick={() =>
                                      insertAtCursor(
                                        `"${normalizeDomain(tbl.domainId || tbl.schemaName)}"."${tbl.alias || tbl.tableName}"`,
                                      )
                                    }
                                    aria-label={t("schemaBrowser.insertTableReference")}
                                    title={t("schemaBrowser.insertTableReference")}
                                    variant="subtle"
                                    size="sm"
                                    data-testid={`schema-insert-table-${tbl.tableName}`}
                                    style={{
                                      flexShrink: 0,
                                      color: "var(--primary)",
                                      opacity: 0.6,
                                    }}
                                  >
                                    →
                                  </ActionIcon>
                                  {tbl.description && (
                                    <Tooltip label={tbl.description} withinPortal>
                                      <Info
                                        size={10}
                                        aria-hidden
                                        style={{
                                          flexShrink: 0,
                                          marginRight: "0.35rem",
                                          color: "var(--primary)",
                                          opacity: 0.7,
                                        }}
                                      />
                                    </Tooltip>
                                  )}
                                </div>
                                {tOpen &&
                                  [
                                    ...tbl.columns.map((col) => ({
                                      columnName: col.computedSqlAlias,
                                      dataType: col.dataType,
                                      description: col.description,
                                      virtual: false,
                                    })),
                                    {
                                      columnName: "_name_",
                                      dataType: "text",
                                      description: t("schemaBrowser.tableAliasName"),
                                      virtual: true,
                                    },
                                    {
                                      columnName: "_domain_",
                                      dataType: "text",
                                      description: t("schemaBrowser.domainId"),
                                      virtual: true,
                                    },
                                  ].map((col) => (
                                    <div
                                      key={col.columnName}
                                      style={{ display: "flex", alignItems: "center" }}
                                    >
                                      <UnstyledButton
                                        onClick={() =>
                                          topTab === "sql"
                                            ? insertAtCursor(
                                                `"${tbl.alias || tbl.tableName}"."${col.columnName}"`,
                                              )
                                            : undefined
                                        }
                                        data-testid={`schema-column-${tbl.tableName}-${col.columnName}`}
                                        style={{
                                          flex: 1,
                                          minWidth: 0,
                                          textAlign: "left",
                                          cursor: topTab === "sql" ? "pointer" : "default",
                                          padding: "0.15rem 0 0.15rem 2.5rem",
                                          display: "flex",
                                          alignItems: "center",
                                          gap: "0.3rem",
                                          color: "var(--text-muted)",
                                          fontSize: "0.72rem",
                                          fontFamily: "monospace",
                                          opacity: col.virtual ? 0.6 : 1,
                                        }}
                                        title={
                                          col.description ??
                                          (topTab === "sql"
                                            ? t("schemaBrowser.insertQuotedColumn")
                                            : undefined)
                                        }
                                      >
                                        <Columns3
                                          size={8}
                                          aria-hidden
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
                                      </UnstyledButton>
                                      {col.description && (
                                        <Tooltip label={col.description} withinPortal>
                                          <Info
                                            size={10}
                                            aria-hidden
                                            style={{
                                              flexShrink: 0,
                                              marginRight: "0.5rem",
                                              color: "var(--text-muted)",
                                              opacity: 0.6,
                                            }}
                                          />
                                        </Tooltip>
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
                              <ActionIcon
                                onClick={() =>
                                  setDomainPages((p) => ({ ...p, [domain]: dp - 1 }))
                                }
                                disabled={dp === 0}
                                aria-label={t("schemaBrowser.previousPage")}
                                title={t("schemaBrowser.previousPage")}
                                variant="subtle"
                                size="xs"
                                data-testid={`schema-page-prev-${domain}`}
                                style={{ color: "var(--text-muted)" }}
                              >
                                ‹
                              </ActionIcon>
                              <Text component="span" style={{ fontSize: "0.65rem", color: "var(--text-muted)" }}>
                                {dp * DOMAIN_PAGE_SIZE + 1}–
                                {Math.min((dp + 1) * DOMAIN_PAGE_SIZE, domainTables.length)} /{" "}
                                {domainTables.length}
                              </Text>
                              <ActionIcon
                                onClick={() =>
                                  setDomainPages((p) => ({ ...p, [domain]: dp + 1 }))
                                }
                                disabled={dp >= totalDomainPages - 1}
                                aria-label={t("schemaBrowser.nextPage")}
                                title={t("schemaBrowser.nextPage")}
                                variant="subtle"
                                size="xs"
                                data-testid={`schema-page-next-${domain}`}
                                style={{ color: "var(--text-muted)" }}
                              >
                                ›
                              </ActionIcon>
                            </div>
                          )}
                        </>
                      );
                    })()}
                </div>
              );
            })}
          </div>
        </ScrollArea>
      </div>
    </div>
  );
}
