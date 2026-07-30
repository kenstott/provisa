// Copyright (c) 2026 Kenneth Stott
// Canary: 0f62c0c5-a9e2-4f0c-812f-d84dd160d2c4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Group, Stack, Text, Tooltip } from "@mantine/core";
import { Pencil, Trash2 } from "lucide-react";
import type { Metric } from "../../types/admin";

interface MetricDetailPanelProps {
  m: Metric;
  dependentViews: string[];
  onEdit: () => void;
  onDelete: () => void;
}

// Row-click detail view for a metric — same detail-then-edit pattern as
// SourceDetailPanel: the read-only facts with Edit/Delete inside.
export function MetricDetailPanel({ m, dependentViews, onEdit, onDelete }: MetricDetailPanelProps) {
  const { t } = useTranslation();

  const rows: [string, React.ReactNode][] = [
    [
      "expression",
      <Text key="expr" component="span" size="sm" ff="monospace" style={{ whiteSpace: "pre-wrap" }}>
        {m.expression}
      </Text>,
    ],
    ["datatype", m.datatype || "—"],
    ["description", m.description || "—"],
    ["aiContext", m.aiContext || "—"],
    ["sourceFact", m.fromFact ?? t("metricsPage.detail.handAuthored")],
    ["visibleTo", m.visibleTo.length ? m.visibleTo.join(", ") : "*"],
    [
      "dependentViews",
      dependentViews.length ? dependentViews.join(", ") : t("metricsPage.detail.noDependents"),
    ],
  ];

  return (
    <Stack gap="sm" data-testid={`metric-detail-${m.name}`}>
      <dl
        style={{
          display: "grid",
          gridTemplateColumns: "max-content 1fr",
          gap: "0.25rem 1rem",
          margin: 0,
          color: "var(--text)",
        }}
      >
        {rows.map(([k, v]) => (
          <React.Fragment key={k}>
            <Text component="dt" c="dimmed" fw={500} size="sm">
              {t(`metricsPage.detail.field.${k}`)}
            </Text>
            <Text component="dd" m={0} size="sm">
              {v}
            </Text>
          </React.Fragment>
        ))}
      </dl>
      <Group gap="xs" mt={4}>
        <Tooltip label={t("metricsPage.editMetric", { name: m.name })}>
          <ActionIcon
            variant="subtle"
            aria-label={t("metricsPage.editMetric", { name: m.name })}
            data-testid={`metric-detail-edit-${m.name}`}
            onClick={(e) => {
              e.stopPropagation();
              onEdit();
            }}
          >
            <Pencil size={14} />
          </ActionIcon>
        </Tooltip>
        <Tooltip label={t("metricsPage.deleteMetric", { name: m.name })}>
          <ActionIcon
            variant="subtle"
            color="red"
            aria-label={t("metricsPage.deleteMetric", { name: m.name })}
            data-testid={`metric-detail-delete-${m.name}`}
            onClick={(e) => {
              e.stopPropagation();
              onDelete();
            }}
          >
            <Trash2 size={14} />
          </ActionIcon>
        </Tooltip>
      </Group>
    </Stack>
  );
}
