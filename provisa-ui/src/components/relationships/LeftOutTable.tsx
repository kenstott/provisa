// Copyright (c) 2026 Kenneth Stott
// Canary: 7c2e9a54-1d3b-4f86-b0a7-e5f48c6d2b19
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: what a discovery run read and does not suggest, under its suggestions and closed until
// opened: each proposal with the confidence given (the model's, and Jev's where Jev scored it),
// its reasoning and why it was left out. One that names two compared columns can be accepted, as
// a suggestion is; one that named none is shown as the model wrote it.

import React from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Table, Text } from "@mantine/core";
import type { LeftOutProposal } from "../../api/admin";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";
import { candidateName, type Candidate } from "./relationship-types";

interface LeftOutTableProps {
  proposals: LeftOutProposal[];
  /** The confidence a suggestion needs, of 1. */
  threshold: number;
  tableDomainById: Record<string, string>;
  tableNameById: Record<string, string>;
  onAccept: (id: number, name: string) => void;
  error: string;
}

const percent = (confidence: number) => (confidence * 100).toFixed(0);

/** A left-out proposal that names its two columns, as a suggestion names them. */
function named(p: LeftOutProposal): Candidate | null {
  if (
    p.source_table_id === null ||
    p.target_table_id === null ||
    p.source_column === null ||
    p.target_column === null ||
    p.confidence === null
  ) {
    return null;
  }
  return {
    id: p.id,
    source_table_id: p.source_table_id,
    target_table_id: p.target_table_id,
    source_column: p.source_column,
    target_column: p.target_column,
    cardinality: p.cardinality,
    confidence: p.confidence,
    reasoning: p.reasoning ?? "",
    suggested_name: p.suggested_name ?? undefined,
  };
}

export function LeftOutTable({
  proposals,
  threshold,
  tableDomainById,
  tableNameById,
  onAccept,
  error,
}: LeftOutTableProps) {
  const { t } = useTranslation();
  if (proposals.length === 0 && !error) return null;

  const label = (tableId: number, column: string) => {
    const domain = tableDomainById[tableId];
    const table = tableNameById[tableId] ?? String(tableId);
    return domain ? `${domain}.${table}.${column}` : `${table}.${column}`;
  };

  return (
    <Collapsibles id="relationships.discover-run" testId="discover-left-out">
      <CollapsibleItem
        value="left-out"
        title={t("leftOutTable.heading", { count: proposals.length })}
        defaultOpen={false}
        controlTestId="discover-left-out-toggle"
      >
        <Text fz="sm" c="dimmed" mb="xs">
          {t("leftOutTable.intro")}
        </Text>
        {error && (
          <Alert color="red" mb="md" data-testid="discover-left-out-error">
            {error}
          </Alert>
        )}
        <Table.ScrollContainer minWidth={640}>
          <Table withTableBorder verticalSpacing="xs">
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("candidatesTable.colSource")}</Table.Th>
                <Table.Th>{t("candidatesTable.colTarget")}</Table.Th>
                <Table.Th>{t("candidatesTable.colConfidence")}</Table.Th>
                <Table.Th>{t("leftOutTable.colReason")}</Table.Th>
                <Table.Th>{t("candidatesTable.colActions")}</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {proposals.map((p) => {
                const c = named(p);
                const name = c ? candidateName(c, tableNameById) : "";
                return (
                  <React.Fragment key={p.id}>
                    <Table.Tr data-testid={`left-out-${p.id}`}>
                      <Table.Td>{c ? label(c.source_table_id, c.source_column) : "—"}</Table.Td>
                      <Table.Td>{c ? label(c.target_table_id, c.target_column) : "—"}</Table.Td>
                      <Table.Td>
                        {c === null
                          ? "—"
                          : p.scored === null
                            ? `${percent(c.confidence)}%`
                            : `${t("leftOutTable.byModel", { confidence: percent(c.confidence) })}, ${t(
                                "leftOutTable.byJev",
                                { confidence: percent(p.scored) },
                              )}`}
                      </Table.Td>
                      <Table.Td>
                        {t(`leftOutTable.reason.${p.reason}`, {
                          threshold: Math.round(threshold * 100),
                        })}
                      </Table.Td>
                      <Table.Td>
                        {c && p.acceptable && (
                          <Button
                            size="xs"
                            aria-label={t("leftOutTable.acceptAria", { name })}
                            onClick={() => onAccept(p.id, name)}
                          >
                            {t("candidatesTable.accept")}
                          </Button>
                        )}
                      </Table.Td>
                    </Table.Tr>
                    <Table.Tr>
                      <Table.Td
                        colSpan={5}
                        style={{ padding: "0.25rem 1rem 0.75rem", borderTop: "none" }}
                      >
                        <Text c="dimmed" fz="sm" fs="italic" style={{ overflowWrap: "anywhere" }}>
                          {c ? p.reasoning : t("leftOutTable.asWritten", { answer: p.answer })}
                        </Text>
                      </Table.Td>
                    </Table.Tr>
                  </React.Fragment>
                );
              })}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      </CollapsibleItem>
    </Collapsibles>
  );
}
