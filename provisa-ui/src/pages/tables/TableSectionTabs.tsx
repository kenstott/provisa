// Copyright (c) 2026 Kenneth Stott
// Canary: 22a0570f-7ffb-4c20-8de0-982f0c7f9007
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Badge, Tabs } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { TABLE_SECTIONS, type TableSection } from "./tableSections";

/**
 * The table's seven sections, one tab each. `errors` counts, per section, what Save will refuse
 * that the form itself can tell: a section that is not in view shows its count on its label.
 */
export function TableSectionTabs({
  value,
  onChange,
  errors = {},
}: {
  value: TableSection;
  onChange: (section: TableSection) => void;
  errors?: Partial<Record<TableSection, number>>;
}) {
  const { t } = useTranslation();
  return (
    <Tabs value={value} onChange={(v) => v && onChange(v as TableSection)} mb="sm">
      <Tabs.List aria-label={t("tableSections.label")}>
        {TABLE_SECTIONS.map((section) => {
          const count = errors[section] ?? 0;
          return (
            <Tabs.Tab
              key={section}
              value={section}
              data-testid={`table-section-${section}`}
              rightSection={
                count > 0 ? (
                  <Badge
                    size="xs"
                    color="red"
                    circle
                    aria-label={t("tableSections.errors", { count })}
                    data-testid={`table-section-${section}-errors`}
                  >
                    {count}
                  </Badge>
                ) : undefined
              }
            >
              {t(`tableSections.${section}`)}
            </Tabs.Tab>
          );
        })}
      </Tabs.List>
    </Tabs>
  );
}
