// Copyright (c) 2026 Kenneth Stott
// Canary: 9d41c6a8-0e5b-4f73-b2a9-7c18e3f5d062
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a contract's promises as a table in the shared list style -- one row per check: what it
// applies to (the table, its rows or a column), the check in plain words, its thresholds in plain
// words and its latest outcome. A row opens its promise's results in the SQL page.

import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { Table, UnstyledButton } from "@mantine/core";
import type { ProductDqEntry, ProductDqRule } from "../../api/lineage";
import { ListRow } from "../../components/list/ListTable";
import { SortGroupTable } from "../../components/list/SortGroupTable";
import { tableSqlRef } from "../../naming";
import { OutcomeBadge } from "./dataProductDq";
import { appliesOrder, appliesTo, checkDetails, checkName } from "./promiseRules";

interface Row {
  key: string;
  promise: ProductDqEntry;
  rule: ProductDqRule;
  applies: ReturnType<typeof appliesTo>;
}

export function PromisesTable({
  promises,
  testId,
}: {
  promises: ProductDqEntry[];
  testId: string;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  // The table's checks first, then its rows', then each column's; authored order within each.
  const rows = useMemo(
    () =>
      promises
        .flatMap((promise) =>
          promise.rules.map((rule, i) => ({
            key: `${promise.checker_table_id}-${i}`,
            promise,
            rule,
            applies: appliesTo(rule),
          })),
        )
        .map((row, order) => ({ row, order }))
        .sort(
          (a, b) => appliesOrder(a.row.applies) - appliesOrder(b.row.applies) || a.order - b.order,
        )
        .map(({ row }): Row => row),
    [promises],
  );
  const applies = (r: Row) =>
    r.applies.level === "column"
      ? r.applies.column
      : t(`contractPromises.level.${r.applies.level}`);
  const open = (p: ProductDqEntry) => {
    const results = tableSqlRef({ domainId: p.domain_id, tableName: p.table_name, alias: p.alias });
    navigate("/sql", { state: { sql: `select * from ${results}`, autoRun: true } });
  };
  return (
    <SortGroupTable
      testPrefix={testId}
      testId={testId}
      minWidth={480}
      rows={rows}
      columns={[
        { key: "applies", label: t("contractPromises.col.appliesTo"), sortValue: applies },
        {
          key: "check",
          label: t("contractPromises.col.check"),
          sortValue: (r) => checkName(r.rule, t),
        },
      ]}
      headers={[
        { col: "applies" },
        { col: "check" },
        t("contractPromises.col.details"),
        t("contractPromises.col.result"),
      ]}
      colSpan={4}
      rowKey={(r) => r.key}
      render={(r) => (
        <ListRow key={r.key} testId={`${testId}-item-${r.key}`} onClick={() => open(r.promise)}>
          <Table.Td>{applies(r)}</Table.Td>
          <Table.Td>{checkName(r.rule, t)}</Table.Td>
          <Table.Td>{checkDetails(r.rule, t)}</Table.Td>
          <Table.Td>
            <UnstyledButton
              onClick={(e) => {
                e.stopPropagation();
                open(r.promise);
              }}
              aria-label={r.promise.description || r.promise.table_name}
            >
              <OutcomeBadge outcome={r.promise.latest_outcome} />
            </UnstyledButton>
          </Table.Td>
        </ListRow>
      )}
    />
  );
}
