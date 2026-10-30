// Copyright (c) 2026 Kenneth Stott
// Canary: 3e8f5a02-c1b7-4e96-a4d3-78b02f6e1c45
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1386 (DRIFT FROM THE MODEL'S HISTORY): the terms that lost or gained grounding since the
// previous period. Computed by Provisa from the model's commits rather than read from a view, so
// it sits beside the report's grid. No commit to compare with is no history, never zero drift.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import { Anchor, Loader, ScrollArea, Stack, Table, Text } from "@mantine/core";
import { fetchOntologyDrift, type OntologyDrift, type TermGrade } from "../../api/glossary";
import { reportCellHref } from "../../lib/reportCellHref";

/** The UTC date of a timestamp the server wrote in ISO form. */
const utcDate = (iso: string) => iso.slice(0, 10);

export function OntologyDriftPanel() {
  const { t } = useTranslation();
  const [drift, setDrift] = useState<OntologyDrift | null>(null);
  const [problem, setProblem] = useState("");
  useEffect(() => {
    let live = true;
    fetchOntologyDrift().then(
      (found) => live && setDrift(found),
      (e: Error) => live && setProblem(e.message),
    );
    return () => {
      live = false;
    };
  }, []);

  const grade = (value: TermGrade | null) =>
    value === null ? t("reportsTab.drift.absent") : t(`glossaryTab.groundingValue_${value}`);

  const body = () => {
    if (problem) {
      return (
        <Text size="xs" c="red" data-testid="drift-error">
          {problem}
        </Text>
      );
    }
    if (drift === null) return <Loader size="xs" />;
    if (drift.terms === null || drift.counts === null || drift.base === null) {
      return (
        <Text size="xs" c="dimmed" data-testid="drift-no-history">
          {t("reportsTab.drift.noHistory", { cutoff: utcDate(drift.cutoff) })}
        </Text>
      );
    }
    const baseline = (
      <Text size="xs" c="dimmed" data-testid="drift-baseline">
        {t("reportsTab.drift.baseline", {
          cutoff: utcDate(drift.cutoff),
          date: utcDate(drift.base.committed_at),
        })}
      </Text>
    );
    if (drift.terms.length === 0) {
      return (
        <>
          {baseline}
          <Text size="xs" data-testid="drift-none">
            {t("reportsTab.drift.none")}
          </Text>
        </>
      );
    }
    return (
      <>
        {baseline}
        <Text size="xs" data-testid="drift-counts">
          {t("reportsTab.drift.counts", drift.counts)}
        </Text>
        <Text size="xs" c="dimmed" data-testid="drift-rename-rule">
          {t("reportsTab.drift.renameRule")}
        </Text>
        <ScrollArea.Autosize mah={180} type="auto">
          <Table withRowBorders stickyHeader fz="xs" verticalSpacing={2}>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("reportsTab.drift.term")}</Table.Th>
                <Table.Th>{t("reportsTab.drift.before")}</Table.Th>
                <Table.Th>{t("reportsTab.drift.after")}</Table.Th>
                <Table.Th>{t("reportsTab.drift.change")}</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {drift.terms.map((term) => {
                const href =
                  term.term_id === null ? null : reportCellHref("term_id", term.term_id);
                return (
                  <Table.Tr key={term.name} data-testid={`drift-row-${term.name}`}>
                    <Table.Td>
                      {href === null ? (
                        term.name
                      ) : (
                        <Anchor component={Link} to={href} size="xs">
                          {term.name}
                        </Anchor>
                      )}
                    </Table.Td>
                    <Table.Td>{grade(term.before)}</Table.Td>
                    <Table.Td>{grade(term.after)}</Table.Td>
                    <Table.Td>{t(`reportsTab.drift.changeKind.${term.change}`)}</Table.Td>
                  </Table.Tr>
                );
              })}
            </Table.Tbody>
          </Table>
        </ScrollArea.Autosize>
      </>
    );
  };

  return (
    <Stack
      gap={4}
      px="xs"
      py={6}
      style={{ flexShrink: 0, borderTop: "1px solid var(--border)" }}
      data-testid="ontology-drift"
    >
      <Text size="sm" fw={600}>
        {t("reportsTab.drift.title")}
      </Text>
      {body()}
    </Stack>
  );
}
