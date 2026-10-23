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
// row count, or marked failed. The one chosen is the result shown: its grid, profile and exports.

import { useTranslation } from "react-i18next";
import { Badge, Tabs } from "@mantine/core";
import { statementLabel, type StatementResult } from "./statementResults";

export function StatementTabs({
  statements,
  index,
  onChange,
}: {
  statements: StatementResult[];
  index: number;
  onChange: (index: number) => void;
}) {
  const { t } = useTranslation();
  return (
    <Tabs
      value={String(index)}
      onChange={(v) => v !== null && onChange(Number(v))}
      variant="pills"
      keepMounted={false}
      style={{ flexShrink: 0, padding: "0.25rem 0.5rem", borderBottom: "1px solid var(--border)" }}
      data-testid="sql-statement-tabs"
    >
      <Tabs.List>
        {statements.map((r, i) => {
          const name = statementLabel(r.statement);
          return (
            <Tabs.Tab
              key={i}
              value={String(i)}
              data-testid={`sql-statement-tab-${i}`}
              rightSection={
                r.error ? (
                  <Badge size="xs" color="red" data-testid={`sql-statement-failed-${i}`}>
                    {t("sqlResultsPanel.statementFailed")}
                  </Badge>
                ) : r.redirect ? undefined : (
                  <Badge size="xs" color="blue" data-testid={`sql-statement-rows-${i}`}>
                    {r.rows.length}
                  </Badge>
                )
              }
            >
              {name === null
                ? t("sqlResultsPanel.statementTabUnnamed", { n: i + 1 })
                : t("sqlResultsPanel.statementTab", { n: i + 1, name })}
            </Tabs.Tab>
          );
        })}
      </Tabs.List>
    </Tabs>
  );
}
