// Copyright (c) 2026 Kenneth Stott
// Canary: a2bae5fe-7f63-4cca-be80-6fa7252e362a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { useTranslation } from "react-i18next";
import { Badge, Button, Select, Table, Text, TextInput } from "@mantine/core";
import { Check } from "lucide-react";
import type { ModelingCandidate } from "./types";

interface CandidatesPanelProps {
  candidates: ModelingCandidate[];
  setCandidates: React.Dispatch<React.SetStateAction<ModelingCandidate[]>>;
  tableNameSet: Set<string>;
  onPromote: ((candidate: ModelingCandidate) => Promise<void>) | undefined;
  handlePromote: (idx: number) => void;
}

export function CandidatesPanel({
  candidates,
  setCandidates,
  tableNameSet,
  onPromote,
  handlePromote,
}: CandidatesPanelProps) {
  const { t } = useTranslation();

  if (candidates.length === 0) {
    return (
      <Text ta="center" c="dimmed" fz="sm" p="lg">
        {t("candidatesPanel.empty")}
      </Text>
    );
  }

  return (
    <Table.ScrollContainer minWidth={640}>
      <Table withTableBorder verticalSpacing="xs" fz="sm">
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("candidatesPanel.colId")}</Table.Th>
            <Table.Th>{t("candidatesPanel.colSource")}</Table.Th>
            <Table.Th>{t("candidatesPanel.colTarget")}</Table.Th>
            <Table.Th>{t("candidatesPanel.colCardinality")}</Table.Th>
            <Table.Th>{t("candidatesPanel.colActions")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {candidates.map((c, idx) => (
            <Table.Tr key={idx}>
              <Table.Td>
                <TextInput
                  aria-label={t("candidatesPanel.idLabel")}
                  value={c.id}
                  size="xs"
                  onChange={(e) =>
                    setCandidates((prev) =>
                      prev.map((item, i) =>
                        i === idx ? { ...item, id: e.currentTarget.value } : item,
                      ),
                    )
                  }
                />
              </Table.Td>
              <Table.Td>
                <Text span c={tableNameSet.has(c.sourceTable) ? undefined : "red"} fz="sm">
                  {c.sourceTable}
                </Text>
                <Text span c="dimmed" fz="sm">
                  .
                </Text>
                <Text span fz="sm">
                  {c.sourceCol}
                </Text>
              </Table.Td>
              <Table.Td>
                <Text span c={tableNameSet.has(c.targetTable) ? undefined : "red"} fz="sm">
                  {c.targetTable}
                </Text>
                <Text span c="dimmed" fz="sm">
                  .
                </Text>
                <Text span fz="sm">
                  {c.targetCol}
                </Text>
              </Table.Td>
              <Table.Td>
                <Select
                  aria-label={t("candidatesPanel.cardinalityLabel")}
                  size="xs"
                  value={c.cardinality}
                  data={[
                    { value: "many-to-one", label: t("candidatesPanel.manyToOne") },
                    { value: "one-to-many", label: t("candidatesPanel.oneToMany") },
                  ]}
                  allowDeselect={false}
                  onChange={(value) =>
                    setCandidates((prev) =>
                      prev.map((item, i) =>
                        i === idx ? { ...item, cardinality: value ?? item.cardinality } : item,
                      ),
                    )
                  }
                />
              </Table.Td>
              <Table.Td>
                {c.promoted ? (
                  <Badge color="green" variant="light" leftSection={<Check size={12} />}>
                    {t("candidatesPanel.promoted")}
                  </Badge>
                ) : onPromote ? (
                  <Button size="compact-xs" onClick={() => handlePromote(idx)}>
                    {t("candidatesPanel.promote")}
                  </Button>
                ) : null}
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Table.ScrollContainer>
  );
}
