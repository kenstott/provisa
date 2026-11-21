// Copyright (c) 2026 Kenneth Stott
// Canary: 6a1d9e43-2f7c-4b58-a0e6-3c8b5d1f7e29
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Stack, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { useTableMetrics } from "../../hooks/useAdminQueries";

/**
 * REQ-2243: the governed metrics a dataset uses, as the reader's role may see them: each with its
 * name, description and AI definition, and, when it names a glossary term, that term's definition
 * and calculation method. A metric's expression is not shown.
 */
export function TableMetrics({ tableId }: { tableId: number }) {
  const { t } = useTranslation();
  const { metrics, error } = useTableMetrics(tableId);
  if (error)
    return (
      <Text span size="sm" c="var(--destructive)">
        {error.message}
      </Text>
    );
  if (metrics === undefined)
    return (
      <Text span size="sm" c="dimmed">
        {t("common.loading")}
      </Text>
    );
  if (metrics.length === 0)
    return (
      <Text span size="sm" c="dimmed">
        {t("tableSections.noMetrics")}
      </Text>
    );
  return (
    <Stack gap="xs">
      {metrics.map((m) => (
        <Stack key={m.name} gap={0} data-testid={`table-metric-${m.name}`}>
          <Text size="sm" fw={500}>
            {m.name}
          </Text>
          {m.description && <Text size="sm">{m.description}</Text>}
          {m.aiContext && (
            <Text size="sm" c="dimmed">
              {m.aiContext}
            </Text>
          )}
          {m.term && (
            <Text size="sm" data-testid={`table-metric-${m.name}-term`}>
              {t("tableSections.metricTerm", { term: m.term })}
              {m.termDefinition ? ` ${m.termDefinition}` : ""}
            </Text>
          )}
          {m.termCalculationMethod && (
            <Text size="sm" data-testid={`table-metric-${m.name}-method`}>
              {t("tableSections.metricMethod", { method: m.termCalculationMethod })}
            </Text>
          )}
        </Stack>
      ))}
    </Stack>
  );
}
