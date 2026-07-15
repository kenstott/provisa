// Copyright (c) 2026 Kenneth Stott
// Canary: ed102b59-16aa-4d17-912b-875dbfe96bba
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { useTranslation } from "react-i18next";
import { Button, Table, Text } from "@mantine/core";
import type { HistoryEntry, ResultTab } from "./types";

interface HistoryPanelProps {
  history: HistoryEntry[];
  setSqlText: React.Dispatch<React.SetStateAction<string>>;
  setRole: React.Dispatch<React.SetStateAction<string>>;
  setResultTab: React.Dispatch<React.SetStateAction<ResultTab>>;
}

export function HistoryPanel({ history, setSqlText, setRole, setResultTab }: HistoryPanelProps) {
  const { t } = useTranslation();

  if (history.length === 0) {
    return (
      <Text ta="center" c="dimmed" fz="sm" p="lg">
        {t("sqlModelingHistoryPanel.empty")}
      </Text>
    );
  }

  return (
    <Table.ScrollContainer minWidth={640}>
      <Table verticalSpacing="xs" fz="xs">
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("sqlModelingHistoryPanel.colTime")}</Table.Th>
            <Table.Th>{t("sqlModelingHistoryPanel.colRole")}</Table.Th>
            <Table.Th>{t("sqlModelingHistoryPanel.colDuration")}</Table.Th>
            <Table.Th>{t("sqlModelingHistoryPanel.colRows")}</Table.Th>
            <Table.Th style={{ width: "50%" }}>{t("sqlModelingHistoryPanel.colSql")}</Table.Th>
            <Table.Th>
              <Text span visibleFrom="xs" fz="xs" fw={600}>
                {t("sqlModelingHistoryPanel.colActions")}
              </Text>
            </Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {history.map((h, i) => {
            const ts = new Date(h.executedAt);
            const timeLabel = ts.toLocaleTimeString([], {
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
            });
            const dateLabel = ts.toLocaleDateString([], {
              month: "short",
              day: "numeric",
            });
            const isToday = ts.toDateString() === new Date().toDateString();
            return (
              <Table.Tr key={i} style={{ verticalAlign: "top" }}>
                <Table.Td style={{ whiteSpace: "nowrap" }} c="dimmed">
                  <div>{timeLabel}</div>
                  {!isToday && <div style={{ fontSize: "0.68rem" }}>{dateLabel}</div>}
                </Table.Td>
                <Table.Td style={{ whiteSpace: "nowrap" }} c="dimmed">
                  {h.role}
                </Table.Td>
                <Table.Td
                  style={{ whiteSpace: "nowrap" }}
                  c={h.error ? "red" : "dimmed"}
                >
                  {h.durationMs}ms
                </Table.Td>
                <Table.Td
                  style={{ whiteSpace: "nowrap" }}
                  c={h.error ? "red" : undefined}
                >
                  {h.error ? (
                    <span title={h.error}>{t("sqlModelingHistoryPanel.error")}</span>
                  ) : (
                    h.rowCount
                  )}
                </Table.Td>
                <Table.Td>
                  <pre
                    style={{
                      margin: 0,
                      fontSize: "0.72rem",
                      whiteSpace: "pre-wrap",
                      wordBreak: "break-all",
                      color: "var(--text)",
                      maxHeight: "4.5em",
                      overflow: "hidden",
                    }}
                  >
                    {h.sql}
                  </pre>
                </Table.Td>
                <Table.Td style={{ whiteSpace: "nowrap" }}>
                  <Button
                    variant="default"
                    size="compact-xs"
                    aria-label={t("sqlModelingHistoryPanel.restoreLabel", { time: timeLabel })}
                    onClick={() => {
                      setSqlText(h.sql);
                      setRole(h.role);
                      setResultTab("results");
                    }}
                  >
                    {t("sqlModelingHistoryPanel.restore")}
                  </Button>
                </Table.Td>
              </Table.Tr>
            );
          })}
        </Table.Tbody>
      </Table>
    </Table.ScrollContainer>
  );
}
