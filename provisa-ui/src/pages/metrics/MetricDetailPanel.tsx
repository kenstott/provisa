// Copyright (c) 2026 Kenneth Stott
// Canary: b77ac0bf-d035-44e8-b4c7-bab6145c8e69
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { ActionIcon, Group, Stack, Text } from "@mantine/core";
import { Pencil } from "lucide-react";
import type { Metric } from "../../types/admin";
import { PanelActions } from "../../components/list/SplitPane";
import { PanelDelete } from "../../components/list/PanelDelete";
import { PanelDuplicate } from "../../components/list/PanelDuplicate";
import { Tip } from "../../components/Tip";
import { DetailFields } from "../../components/detail/DetailFields";
import { TagControl } from "../../components/TagControl";

interface MetricDetailPanelProps {
  m: Metric;
  dependentViews: string[];
  onEdit: () => void;
  /** Opens the create form filled from this metric, under a new name. */
  onDuplicate: () => void;
  /** REQ-2198: resolves once the metric is deleted and the list read again (PanelDelete). */
  onDelete: () => Promise<boolean | void>;
}

// A metric's Edit, Duplicate and Delete. In the page's detail panel they sit in its pinned head
// (PanelActions); the form's Save and Cancel take their place while it is open.
function MetricActions({
  m,
  onEdit,
  onDuplicate,
  onDelete,
}: {
  m: Metric;
  onEdit: () => void;
  onDuplicate: () => void;
  /** REQ-2198: resolves once the metric is deleted and the list read again (PanelDelete). */
  onDelete: () => Promise<boolean | void>;
}) {
  const { t } = useTranslation();
  return (
    <PanelActions>
      <fieldset className="panel-action-set">
        <Group gap="xs">
          <Tip text={t("metricsPage.editMetric", { name: m.name })} named>
            <ActionIcon
              aria-label={t("metricsPage.editMetric", { name: m.name })}
              data-testid={`metric-detail-edit-${m.name}`}
              onClick={(e) => {
                e.stopPropagation();
                onEdit();
              }}
            >
              <Pencil size={14} />
            </ActionIcon>
          </Tip>
          <PanelDuplicate
            name={m.name}
            onDuplicate={onDuplicate}
            testId={`metric-detail-duplicate-${m.name}`}
          />
          <PanelDelete
            itemKey={m.name}
            title={t("metricsPage.deleteTitle")}
            consequence={t("metricsPage.deleteConfirm", { name: m.name })}
            ariaLabel={t("metricsPage.deleteMetric", { name: m.name })}
            testId={`metric-detail-delete-${m.name}`}
            confirmTestId="metric-delete-modal"
            onDelete={onDelete}
          />
        </Group>
      </fieldset>
    </PanelActions>
  );
}

// Row-click detail view for a metric — detail-then-edit: the read-only facts, with Edit,
// Duplicate and Delete in the panel's head.
export function MetricDetailPanel({
  m,
  dependentViews,
  onEdit,
  onDuplicate,
  onDelete,
}: MetricDetailPanelProps) {
  const { t } = useTranslation();

  return (
    <Stack gap="sm" data-testid={`metric-detail-${m.name}`}>
      <MetricActions m={m} onEdit={onEdit} onDuplicate={onDuplicate} onDelete={onDelete} />
      {/* Every value of the metric, under its form's labels. */}
      <DetailFields
        testId={`metric-read-${m.name}`}
        fields={[
          { label: t("metricsPage.fieldName"), value: m.name },
          {
            label: t("metricsPage.fieldExpression"),
            value: (
              <Text component="span" size="sm" ff="monospace" style={{ whiteSpace: "pre-wrap" }}>
                {m.expression}
              </Text>
            ),
          },
          { label: t("metricsPage.fieldDatatype"), value: m.datatype },
          { label: t("metricsPage.fieldDescription"), value: m.description },
          { label: t("metricsPage.fieldAiContext"), value: m.aiContext },
          // REQ-2243: the glossary term the metric names.
          { label: t("metricsPage.fieldTerm"), value: m.term },
          // REQ-2208: a metric carries tags as the model's objects do.
          {
            label: t("metricsPage.fieldTags"),
            value: <TagControl objectType="metric" metricName={m.name} />,
          },
          {
            label: t("metricsPage.builderFact"),
            value: m.fromFact ?? t("metricsPage.detail.handAuthored"),
          },
          // REQ-1958: null is "not shown to you" (no view_governance), not "granted to everyone".
          {
            label: t("metricsPage.detail.field.visibleTo"),
            value:
              m.visibleTo === null
                ? t("tableReadView.notShown")
                : m.visibleTo.length
                  ? m.visibleTo.join(", ")
                  : "*",
          },
          {
            label: t("metricsPage.detail.field.dependentViews"),
            value: dependentViews.join(", "),
          },
        ]}
      />
    </Stack>
  );
}
