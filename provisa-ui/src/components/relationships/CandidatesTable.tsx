// Copyright (c) 2026 Kenneth Stott
// Canary: c8be322e-5e5c-4922-8e69-c46fb2d330bf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { useTranslation } from "react-i18next";
import { Button, Group, Table, Text, Title } from "@mantine/core";
import type { Candidate } from "./relationship-types";

interface CandidatesTableProps {
  candidates: Candidate[];
  tableDomainById: Record<string, string>;
  tableNameById: Record<string, string>;
  onAccept: (id: number, name: string) => void;
  onReject: (id: number) => void;
}

export function CandidatesTable({
  candidates,
  tableDomainById,
  tableNameById,
  onAccept,
  onReject,
}: CandidatesTableProps) {
  const { t } = useTranslation();

  return (
    <>
      <Title order={3} mt="xl">
        {t("candidatesTable.heading")}
      </Title>
      <Table.ScrollContainer minWidth={640}>
        <Table withTableBorder verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("candidatesTable.colName")}</Table.Th>
              <Table.Th>{t("candidatesTable.colSource")}</Table.Th>
              <Table.Th>{t("candidatesTable.colTarget")}</Table.Th>
              <Table.Th>{t("candidatesTable.colCardinality")}</Table.Th>
              <Table.Th>{t("candidatesTable.colConfidence")}</Table.Th>
              <Table.Th>{t("candidatesTable.colActions")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {candidates.map((c) => {
              const srcDomain = tableDomainById[c.source_table_id];
              const tgtDomain = tableDomainById[c.target_table_id];
              const srcTable = tableNameById[c.source_table_id] ?? String(c.source_table_id);
              const tgtTable = tableNameById[c.target_table_id] ?? String(c.target_table_id);
              const srcLabel = srcDomain
                ? `${srcDomain}.${srcTable}.${c.source_column}`
                : `${srcTable}.${c.source_column}`;
              const tgtLabel = tgtDomain
                ? `${tgtDomain}.${tgtTable}.${c.target_column}`
                : `${tgtTable}.${c.target_column}`;
              const suggestedName =
                c.suggested_name || `${srcTable}-${c.source_column}-to-${tgtTable}`;
              return (
                <React.Fragment key={c.id}>
                  <Table.Tr>
                    <Table.Td>
                      <code>{suggestedName}</code>
                    </Table.Td>
                    <Table.Td>{srcLabel}</Table.Td>
                    <Table.Td>{tgtLabel}</Table.Td>
                    <Table.Td>{c.cardinality}</Table.Td>
                    <Table.Td>{(c.confidence * 100).toFixed(0)}%</Table.Td>
                    <Table.Td>
                      <Group gap="xs" wrap="nowrap">
                        <Button
                          size="xs"
                          aria-label={t("candidatesTable.acceptAria", { name: suggestedName })}
                          onClick={() => onAccept(c.id, suggestedName)}
                        >
                          {t("candidatesTable.accept")}
                        </Button>
                        <Button
                          size="xs"
                          color="red"
                          aria-label={t("candidatesTable.rejectAria", { name: suggestedName })}
                          onClick={() => onReject(c.id)}
                        >
                          {t("candidatesTable.reject")}
                        </Button>
                      </Group>
                    </Table.Td>
                  </Table.Tr>
                  <Table.Tr>
                    <Table.Td
                      colSpan={6}
                      style={{
                        padding: "0.25rem 1rem 0.75rem",
                        borderTop: "none",
                      }}
                    >
                      <Text c="dimmed" fz="sm" fs="italic">
                        {c.reasoning}
                      </Text>
                    </Table.Td>
                  </Table.Tr>
                </React.Fragment>
              );
            })}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </>
  );
}
