// Copyright (c) 2026 Kenneth Stott
// Canary: f19c4a8e-6b25-4d73-90e1-8a3d5c7b2f46
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: a layered graph's options, in its header row: the view's own menu first, then how much
// of each node's body is drawn, how lines are routed, the grid snap, whether nodes without lines
// are drawn, collapse or expand every group, and the graph's help last.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Button, Checkbox, Group, Select } from "@mantine/core";
import { ChevronDown, ChevronRight } from "lucide-react";
import { HelpBubble } from "../HelpBubble";
import { Tip } from "../Tip";
import type { Detail } from "./model";
import type { LayeredOptions } from "./ProvisaGraph";

export function GraphOptionsBar({
  testId,
  tourId,
  lead,
  options,
  onOptions,
  groupIds,
  collapsed,
  onCollapsedChange,
  help,
}: {
  testId: string;
  tourId?: string;
  /** The view's own menu, first in the row. */
  lead?: ReactNode;
  options: LayeredOptions;
  onOptions: (change: Partial<LayeredOptions>) => void;
  /** The groups drawn, which collapse and expand. */
  groupIds: string[];
  collapsed: ReadonlySet<string>;
  onCollapsedChange: (next: Set<string>) => void;
  help?: { title: string; text: string };
}) {
  const { t } = useTranslation();
  const allCollapsed = groupIds.length > 0 && groupIds.every((id) => collapsed.has(id));
  const toggleAll = () => onCollapsedChange(allCollapsed ? new Set() : new Set(groupIds));
  const toggleLabel = allCollapsed ? t("erdModal.expandAll") : t("erdModal.collapseAll");
  return (
    <Group gap="0.5rem" wrap="wrap" data-tour={tourId}>
      {lead}
      <Select
        aria-label={t("erdModal.columnDetailLabel")}
        value={options.detail}
        onChange={(v) => v && onOptions({ detail: v as Detail })}
        allowDeselect={false}
        size="xs"
        w={110}
        data={[
          { value: "all", label: t("erdModal.columnDetailAll") },
          { value: "key", label: t("erdModal.columnDetailKey") },
          { value: "none", label: t("erdModal.columnDetailNone") },
        ]}
        data-testid={`${testId}-column-detail`}
      />
      <Select
        aria-label={t("erdModal.edgeRoutingLabel")}
        value={options.routing}
        onChange={(v) => v && onOptions({ routing: v as LayeredOptions["routing"] })}
        allowDeselect={false}
        size="xs"
        w={150}
        data={[
          { value: "bezier", label: t("erdModal.edgeRoutingBezier") },
          { value: "taxi", label: t("erdModal.edgeRoutingTaxi") },
        ]}
        data-testid={`${testId}-edge-routing`}
      />
      <Select
        aria-label={t("erdModal.gridSnapLabel")}
        value={String(options.gridSnap)}
        onChange={(v) => v && onOptions({ gridSnap: parseInt(v, 10) })}
        allowDeselect={false}
        size="xs"
        w={110}
        data={[
          { value: "0", label: t("erdModal.gridSnapNone") },
          { value: "5", label: t("erdModal.gridSnap5") },
          { value: "10", label: t("erdModal.gridSnap10") },
          { value: "15", label: t("erdModal.gridSnap15") },
          { value: "20", label: t("erdModal.gridSnap20") },
        ]}
        data-testid={`${testId}-grid-snap`}
      />
      <Group gap="xs" h={30} align="center" wrap="nowrap">
        <Checkbox
          label={t("erdModal.orphans")}
          checked={options.showOrphans}
          onChange={(e) => onOptions({ showOrphans: e.currentTarget.checked })}
          size="xs"
          data-testid={`${testId}-show-orphans`}
        />
      </Group>
      <Tip text={toggleLabel}>
        <Button
          size="xs"
          h={30}
          variant="default"
          onClick={toggleAll}
          leftSection={allCollapsed ? <ChevronRight size={12} /> : <ChevronDown size={12} />}
          data-testid={`${testId}-toggle-all`}
        >
          {toggleLabel}
        </Button>
      </Tip>
      {/* How the groups work: in the row's (?) help, last and at its end, so nothing is drawn
          over the graph. */}
      {help && groupIds.length > 0 && (
        <Group ml="auto">
          <HelpBubble
            title={help.title}
            paragraphs={[help.text]}
            ariaLabel={help.text}
            testId={`${testId}-hint`}
          />
        </Group>
      )}
    </Group>
  );
}
