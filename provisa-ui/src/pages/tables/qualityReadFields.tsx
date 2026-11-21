// Copyright (c) 2026 Kenneth Stott
// Canary: 619e9dd8-a463-4047-8ce1-11bbef0eb99c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import type { ReactNode } from "react";
import { Alert, Anchor, Badge, Table, Text } from "@mantine/core";
import { Link } from "react-router-dom";
import type { TFunction } from "i18next";
import { useDqContract } from "../../hooks/useAdminQueries";
import type { DqContract, QualityChecker, UniqueConstraint } from "../../types/admin";
import type { DetailField } from "../../components/detail/DetailFields";
import { detailHref } from "../../components/list/useDetailParam";

// What the Quality tab reads of a table's stored settings, under the edit controls' own labels.

/** The table's declared unique keys, as the uniques editor lists them. */
export function uniquesField(t: TFunction, uniques: UniqueConstraint[]): DetailField {
  return {
    label: t("uniquesPanel.title"),
    testId: "table-read-uniques",
    value:
      uniques.length === 0 ? null : (
        <Table className="data-table detail-table">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("uniquesPanel.nameLabel")}</Table.Th>
              <Table.Th>{t("uniquesPanel.columnsLabel")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {uniques.map((u) => (
              <Table.Tr key={u.name}>
                <Table.Td>
                  <code>{u.name}</code>
                </Table.Td>
                <Table.Td>{u.columns.join(", ")}</Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      ),
  };
}

/** A checker results table's contract, read the way its builder lists it: the table it observes
 *  and its checks. `checker` is null for a table whose source is no checker. */
export function useContractFields(
  t: TFunction,
  checker: string | null,
  contractText: string | null,
): DetailField[] {
  const { parseContract } = useDqContract();
  const [parsed, setParsed] = useState<DqContract | null>(null);
  useEffect(() => {
    if (checker === null || contractText === null) return;
    let live = true;
    void parseContract({ checker, contractText }).then((p) => {
      if (live) setParsed(p);
    });
    return () => {
      live = false;
    };
  }, [checker, contractText, parseContract]);
  if (checker === null) return [];
  const contract = contractText === null ? null : parsed;
  return [
    {
      label: t("dataQualityPanel.datasetTableLabel"),
      // The builder's picker shows a dataset `{source}/{domain}/{table}` as its logical name.
      value: contract?.dataset ? contract.dataset.split("/").slice(1).join(".") : null,
    },
    {
      label: t("dataQualityPanel.title"),
      testId: "table-read-dq-checks",
      value: contract ? checksTable(t, contract) : null,
    },
  ];
}

function checksTable(t: TFunction, contract: DqContract): ReactNode {
  if (contract.error !== null) return <Alert color="red">{contract.error}</Alert>;
  if (contract.checks.length === 0) return null;
  return (
    <Table className="data-table detail-table">
      <Table.Thead>
        <Table.Tr>
          <Table.Th>{t("dataQualityPanel.columnHeader")}</Table.Th>
          <Table.Th>{t("dataQualityPanel.checkHeader")}</Table.Th>
          <Table.Th>{t("dataQualityPanel.definitionHeader")}</Table.Th>
        </Table.Tr>
      </Table.Thead>
      <Table.Tbody>
        {contract.checks.map((c, i) => (
          <Table.Tr key={`${c.columnName}:${c.checkType}:${i}`}>
            <Table.Td>{c.columnName || t("dataQualityPanel.datasetScope")}</Table.Td>
            <Table.Td>{c.checkType}</Table.Td>
            <Table.Td>
              <Text component="pre" size="xs" ff="monospace" className="detail-code" m={0}>
                {c.definition}
              </Text>
            </Table.Td>
          </Table.Tr>
        ))}
      </Table.Tbody>
    </Table>
  );
}

const OUTCOME_COLOR: Record<string, string> = {
  pass: "teal",
  warn: "yellow",
  fail: "red",
  error: "red",
  skipped: "gray",
};

/** REQ-1443: the checker tables whose contracts scan this table, each linked to its own panel,
 *  with its last scan's outcome and time ("—" before a first scan). Read only: a checker names
 *  its target in its contract, so it is edited there. */
export function checkersField(t: TFunction, checkers: QualityChecker[]): DetailField {
  return {
    label: t("tableSections.checkedBy"),
    testId: "table-read-checkers",
    value:
      checkers.length === 0 ? null : (
        <Table className="data-table detail-table">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("tableSections.checkerTable")}</Table.Th>
              <Table.Th>{t("tableSections.checkerSource")}</Table.Th>
              <Table.Th>{t("tableSections.lastScan")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {checkers.map((c) => (
              <Table.Tr key={c.tableId} data-testid={`table-read-checker-${c.tableId}`}>
                <Table.Td>
                  <Anchor component={Link} to={detailHref("tables", c.tableId)} size="sm">
                    {c.tableName}
                  </Anchor>
                </Table.Td>
                <Table.Td>{c.sourceId}</Table.Td>
                <Table.Td>
                  {c.outcome === null ? (
                    t("detailFields.empty")
                  ) : (
                    <>
                      <Badge size="sm" variant="light" color={OUTCOME_COLOR[c.outcome]} mr="xs">
                        {t(`tableSections.outcome.${c.outcome}`)}
                      </Badge>
                      {c.scanTime && new Date(c.scanTime).toLocaleString()}
                    </>
                  )}
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      ),
  };
}
