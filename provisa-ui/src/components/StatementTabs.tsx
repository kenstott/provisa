// Copyright (c) 2026 Kenneth Stott
// Canary: 25a24ae2-7351-4b29-91d5-bc1820a0a3e7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A batch's results, one tab per statement in order ("Result 1 · glossary_terms"), each with its
// row count, or marked failed. The one chosen is the result shown. The SQL page and the Cypher page
// show their batches by it.

import { useTranslation } from "react-i18next";
import { Badge, Tabs } from "@mantine/core";

/** One statement's tab: what it reads first (null: nothing to name it by), its row count (null:
 * its result is elsewhere, in the results store), or that it failed. */
export interface StatementTab {
  name: string | null;
  rows: number | null;
  failed: boolean;
}

export function StatementTabs({
  tabs,
  index,
  onChange,
  testId,
}: {
  tabs: StatementTab[];
  index: number;
  onChange: (index: number) => void;
  /** The tabs' test ids: `${testId}s`, `${testId}-${i}`, `${testId}-rows-${i}`, `${testId}-failed-${i}`. */
  testId: string;
}) {
  const { t } = useTranslation();
  return (
    <Tabs
      value={String(index)}
      onChange={(v) => v !== null && onChange(Number(v))}
      variant="pills"
      keepMounted={false}
      style={{ flexShrink: 0, padding: "0.25rem 0.5rem", borderBottom: "1px solid var(--border)" }}
      data-testid={`${testId}s`}
    >
      <Tabs.List>
        {tabs.map((r, i) => (
          <Tabs.Tab
            key={i}
            value={String(i)}
            data-testid={`${testId}-${i}`}
            rightSection={
              r.failed ? (
                <Badge size="xs" color="red" data-testid={`${testId}-failed-${i}`}>
                  {t("sqlResultsPanel.statementFailed")}
                </Badge>
              ) : r.rows === null ? undefined : (
                <Badge size="xs" color="blue" data-testid={`${testId}-rows-${i}`}>
                  {r.rows}
                </Badge>
              )
            }
          >
            {r.name === null
              ? t("sqlResultsPanel.statementTabUnnamed", { n: i + 1 })
              : t("sqlResultsPanel.statementTab", { n: i + 1, name: r.name })}
          </Tabs.Tab>
        ))}
      </Tabs.List>
    </Tabs>
  );
}
