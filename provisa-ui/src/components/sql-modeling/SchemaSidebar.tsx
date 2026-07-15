// Copyright (c) 2026 Kenneth Stott
// Canary: c681b1a1-3ca0-4102-849a-813ddfa81a4a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { ActionIcon, Box, Text, Tooltip, UnstyledButton } from "@mantine/core";
import { ChevronRight, ChevronDown, Table2, Columns3 } from "lucide-react";
import { useTranslation } from "react-i18next";
import type { Domain, RegisteredTable } from "../../types/admin";
import type { TopTab } from "./types";
import { normalizeDomain } from "./types";

interface SchemaSidebarProps {
  domainGroups: Record<string, RegisteredTable[]>;
  expandedDomains: Set<string>;
  toggleDomain: (d: string) => void;
  domainMap: Record<string, Domain>;
  expandedTables: Set<string>;
  toggleTable: (t: string) => void;
  topTab: TopTab;
  insertAtCursor: (text: string) => void;
}

export function SchemaSidebar({
  domainGroups,
  expandedDomains,
  toggleDomain,
  domainMap,
  expandedTables,
  toggleTable,
  topTab,
  insertAtCursor,
}: SchemaSidebarProps) {
  const { t } = useTranslation();
  const [sidebarOpen, setSidebarOpen] = useState(true);

  return (
    <div
      style={{
        display: "flex",
        flexShrink: 0,
        borderRight: "1px solid var(--border)",
        position: "relative",
      }}
    >
      {/* Drawer toggle tab */}
      <Tooltip
        label={sidebarOpen ? t("schemaSidebar.collapse") : t("schemaSidebar.expand")}
        position="right"
      >
        <ActionIcon
          onClick={() => setSidebarOpen((v) => !v)}
          aria-label={sidebarOpen ? t("schemaSidebar.collapse") : t("schemaSidebar.expand")}
          data-testid="schema-sidebar-toggle"
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
      </Tooltip>

      <div
        style={{
          width: sidebarOpen ? 210 : 0,
          overflow: "hidden",
          transition: "width 0.18s ease",
          background: "var(--bg)",
        }}
      >
        <div style={{ width: 210, overflow: "auto", height: "100%", padding: "0.5rem 0" }}>
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
            {t("schemaSidebar.title")}
          </Text>
          {Object.entries(domainGroups).map(([domain, domainTables]) => {
            const domainOpen = expandedDomains.has(domain);
            const domainDescription = domainMap[domain]?.description;
            const domainButton = (
              <UnstyledButton
                onClick={() => toggleDomain(domain)}
                aria-expanded={domainOpen}
                data-testid={`schema-domain-${domain}`}
                style={{
                  width: "100%",
                  textAlign: "left",
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
                {domainDescription && (
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
              </UnstyledButton>
            );
            return (
              <div key={domain}>
                {domainDescription ? (
                  <Tooltip label={domainDescription} position="right">
                    {domainButton}
                  </Tooltip>
                ) : (
                  domainButton
                )}
                {domainOpen &&
                  domainTables.map((tbl) => {
                    const tOpen = expandedTables.has(tbl.tableName);
                    const tableButton = (
                      <UnstyledButton
                        onClick={() => toggleTable(tbl.tableName)}
                        aria-expanded={tOpen}
                        data-testid={`schema-table-${tbl.tableName}`}
                        draggable={topTab === "canvas"}
                        onDragStart={
                          topTab === "canvas"
                            ? (e) => e.dataTransfer.setData("tableName", tbl.tableName)
                            : undefined
                        }
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
                        onDoubleClick={
                          topTab === "sql"
                            ? () =>
                                insertAtCursor(
                                  `"${normalizeDomain(tbl.domainId || tbl.schemaName)}"."${tbl.tableName}"`,
                                )
                            : undefined
                        }
                      >
                        {tOpen ? <ChevronDown size={9} /> : <ChevronRight size={9} />}
                        <Table2 size={9} style={{ flexShrink: 0, color: "var(--primary)" }} />
                        <span
                          style={{
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {tbl.tableName}
                        </span>
                      </UnstyledButton>
                    );
                    return (
                      <div key={tbl.tableName}>
                        <div style={{ display: "flex", alignItems: "center" }}>
                          <Tooltip
                            label={
                              topTab === "canvas"
                                ? t("schemaSidebar.dragToCanvas")
                                : t("schemaSidebar.doubleClickToInsert")
                            }
                            position="right"
                          >
                            {tableButton}
                          </Tooltip>
                          {/* → insert button */}
                          <Tooltip label={t("schemaSidebar.insertTableReference")}>
                            <ActionIcon
                              onClick={() =>
                                insertAtCursor(
                                  `"${normalizeDomain(tbl.domainId || tbl.schemaName)}"."${tbl.tableName}"`,
                                )
                              }
                              aria-label={t("schemaSidebar.insertTableReference")}
                              data-testid={`schema-table-insert-${tbl.tableName}`}
                              variant="subtle"
                              size="sm"
                              style={{
                                flexShrink: 0,
                                color: "var(--primary)",
                                fontSize: "0.7rem",
                                opacity: 0.6,
                                lineHeight: 1,
                              }}
                            >
                              →
                            </ActionIcon>
                          </Tooltip>
                          {tbl.description && (
                            <Tooltip label={tbl.description}>
                              <Box
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
                              </Box>
                            </Tooltip>
                          )}
                        </div>
                        {tOpen &&
                          tbl.columns.map((col) => {
                            const columnButton = (
                              <UnstyledButton
                                onClick={() =>
                                  topTab === "sql"
                                    ? insertAtCursor(`"${tbl.tableName}"."${col.columnName}"`)
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
                                }}
                              >
                                <Columns3 size={8} style={{ flexShrink: 0 }} />
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
                            );
                            const columnTooltip =
                              col.description ??
                              (topTab === "sql" ? t("schemaSidebar.clickToInsertColumn") : undefined);
                            return (
                              <div
                                key={col.columnName}
                                style={{ display: "flex", alignItems: "center" }}
                              >
                                {columnTooltip ? (
                                  <Tooltip label={columnTooltip} position="right">
                                    {columnButton}
                                  </Tooltip>
                                ) : (
                                  columnButton
                                )}
                                {col.description && (
                                  <Tooltip label={col.description}>
                                    <Box
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
                                    </Box>
                                  </Tooltip>
                                )}
                              </div>
                            );
                          })}
                      </div>
                    );
                  })}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
