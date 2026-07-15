// Copyright (c) 2026 Kenneth Stott
// Canary: 605384b5-29e5-47b7-a96f-644456fe1de9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { ActionIcon, Group, Table, Text, Tooltip } from "@mantine/core";
import { Download } from "lucide-react";
import type { ColumnProfile } from "./types";

interface ProfilePanelProps {
  profile: ColumnProfile[];
  resultRows: Record<string, unknown>[];
  handleDownloadProfile: () => void;
}

export function ProfilePanel({ profile, resultRows, handleDownloadProfile }: ProfilePanelProps) {
  const { t } = useTranslation();

  if (profile.length === 0) {
    return (
      <Text
        style={{ padding: "1.5rem" }}
        ta="center"
        c="dimmed"
        size="0.85rem"
        data-testid="profile-panel-empty"
      >
        {t("profilePanel.emptyState")}
      </Text>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <Group
        style={{
          padding: "0.25rem 0.75rem",
          borderBottom: "1px solid var(--border)",
          flexShrink: 0,
          background: "var(--surface)",
        }}
      >
        <Tooltip label={t("profilePanel.downloadJsonLabel")}>
          <ActionIcon
            variant="subtle"
            size="sm"
            aria-label={t("profilePanel.downloadJsonLabel")}
            onClick={handleDownloadProfile}
            data-testid="profile-panel-download"
          >
            <Download size={14} />
          </ActionIcon>
        </Tooltip>
      </Group>
      <div style={{ flex: 1, overflow: "auto" }}>
        <Table style={{ fontSize: "0.75rem" }}>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("profilePanel.colColumn")}</Table.Th>
              <Table.Th title={t("profilePanel.colNullsTitle")}>{t("profilePanel.colNulls")}</Table.Th>
              <Table.Th title={t("profilePanel.colBlanksTitle")}>{t("profilePanel.colBlanks")}</Table.Th>
              <Table.Th title={t("profilePanel.colDistinctTitle")}>{t("profilePanel.colDistinct")}</Table.Th>
              <Table.Th title={t("profilePanel.colConstantTitle")}>{t("profilePanel.colConstant")}</Table.Th>
              <Table.Th>{t("profilePanel.colMin")}</Table.Th>
              <Table.Th>{t("profilePanel.colMax")}</Table.Th>
              <Table.Th title={t("profilePanel.colMeanTitle")}>{t("profilePanel.colMean")}</Table.Th>
              <Table.Th>{t("profilePanel.colTopValues")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {profile.map((p) => {
              const total = resultRows.length;
              const nullPct = total > 0 ? Math.round((p.nullCount / total) * 100) : 0;
              const isHighNull = nullPct >= 50;
              const isConstant = p.constantValue !== undefined;
              return (
                <Table.Tr key={p.col}>
                  <Table.Td style={{ fontFamily: "monospace", fontWeight: 600 }}>{p.col}</Table.Td>
                  <Table.Td
                    style={{
                      color: isHighNull
                        ? "var(--destructive)"
                        : p.nullCount > 0
                          ? "var(--text)"
                          : "var(--text-muted)",
                    }}
                  >
                    {p.nullCount > 0 ? (
                      `${p.nullCount} (${nullPct}%)`
                    ) : (
                      <span style={{ color: "var(--text-muted)" }}>{t("profilePanel.emptyValue")}</span>
                    )}
                  </Table.Td>
                  <Table.Td
                    style={{
                      color: p.blankCount > 0 ? "var(--text)" : "var(--text-muted)",
                    }}
                  >
                    {p.blankCount > 0 ? (
                      p.blankCount
                    ) : (
                      <span style={{ color: "var(--text-muted)" }}>{t("profilePanel.emptyValue")}</span>
                    )}
                  </Table.Td>
                  <Table.Td
                    style={{
                      color: isConstant ? "var(--text-muted)" : "var(--text)",
                    }}
                  >
                    {p.distinctCount}
                  </Table.Td>
                  <Table.Td
                    style={{
                      color: isConstant ? "var(--destructive)" : "var(--text-muted)",
                    }}
                  >
                    {isConstant ? (
                      <span title={String(p.constantValue)}>
                        {t("profilePanel.constantYes", { value: String(p.constantValue).slice(0, 12) })}
                      </span>
                    ) : (
                      <span style={{ color: "var(--text-muted)" }}>{t("profilePanel.emptyValue")}</span>
                    )}
                  </Table.Td>
                  <Table.Td style={{ fontFamily: "monospace" }}>
                    {p.min !== null ? (
                      String(p.min).slice(0, 16)
                    ) : (
                      <span style={{ color: "var(--text-muted)" }}>{t("profilePanel.emptyValue")}</span>
                    )}
                  </Table.Td>
                  <Table.Td style={{ fontFamily: "monospace" }}>
                    {p.max !== null ? (
                      String(p.max).slice(0, 16)
                    ) : (
                      <span style={{ color: "var(--text-muted)" }}>{t("profilePanel.emptyValue")}</span>
                    )}
                  </Table.Td>
                  <Table.Td style={{ fontFamily: "monospace" }}>
                    {p.mean !== null ? (
                      p.mean.toFixed(2)
                    ) : (
                      <span style={{ color: "var(--text-muted)" }}>{t("profilePanel.emptyValue")}</span>
                    )}
                  </Table.Td>
                  <Table.Td>
                    <div
                      style={{
                        display: "flex",
                        flexWrap: "wrap",
                        gap: "0.2rem",
                      }}
                    >
                      {p.topValues.map(({ value, count }) => (
                        <span
                          key={value}
                          style={{
                            background: "var(--surface)",
                            border: "1px solid var(--border)",
                            borderRadius: "3px",
                            padding: "0 0.3rem",
                            fontSize: "0.7rem",
                            fontFamily: "monospace",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {value.slice(0, 20)}
                          <span style={{ color: "var(--text-muted)" }}>×{count}</span>
                        </span>
                      ))}
                    </div>
                  </Table.Td>
                </Table.Tr>
              );
            })}
          </Table.Tbody>
        </Table>
      </div>
    </div>
  );
}
