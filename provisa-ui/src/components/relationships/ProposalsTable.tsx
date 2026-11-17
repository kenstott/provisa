// Copyright (c) 2026 Kenneth Stott
// Canary: 7c2e9a54-1d3b-4f86-b0a7-e5f48c6d2b19
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: a discovery run's result, one table: every proposal the run read, foreign key
// constraints and semantic ones together, the highest score first. A row's first box accepts it;
// its last marks it not to be recommended again; with neither it stays undecided. A proposal that
// cannot be decided (it names no two compared columns, or repeats what is already there) is
// listed with why and has no box. One Apply says what it will do, with the counts.

import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Checkbox, Group, Table, Text } from "@mantine/core";
import type { RunProposal } from "../../api/admin";
import { BreakableName } from "../list/BreakableName";
import { ListActionsHead, ListHead, ListRow, ListTable } from "../list/ListTable";
import type { ListSelection } from "../list/listSelection";
import { useListSortGroup, type ListColumn } from "../list/useListSortGroup";
import { candidateName } from "./relationship-types";
import type { RunProposalsState } from "./useRunProposals";

interface ProposalsTableProps {
  state: RunProposalsState;
  tableDomainById: Record<string, string>;
  tableNameById: Record<string, string>;
  /** Names the table's parts (and its remembered sort): the dialog's result is "discover", the
   *  Relationships page's suggestions their own, so the two can be open at once. */
  testId?: string;
  /** Names each row's parts. */
  rowTestId?: string;
}

const percent = (confidence: number) => (confidence * 100).toFixed(0);
const NONE = new Set<string>();

export function ProposalsTable({
  state,
  tableDomainById,
  tableNameById,
  testId = "discover",
  rowTestId = "proposal",
}: ProposalsTableProps) {
  const { t } = useTranslation();
  const proposals = useMemo(() => state.proposals ?? [], [state.proposals]);

  const label = (tableId: number | null, column: string | null) => {
    if (tableId === null || column === null) return "";
    const domain = tableDomainById[tableId];
    const table = tableNameById[tableId] ?? String(tableId);
    return domain ? `${domain}.${table}.${column}` : `${table}.${column}`;
  };
  const nameOf = (p: RunProposal) =>
    p.source_table_id === null || p.target_table_id === null || p.source_column === null
      ? ""
      : candidateName(
          {
            id: p.id,
            source_table_id: p.source_table_id,
            target_table_id: p.target_table_id,
            source_column: p.source_column,
            target_column: p.target_column ?? "",
            cardinality: "many-to-one",
            confidence: p.confidence ?? 0,
            reasoning: "",
            suggested_name: p.suggested_name ?? undefined,
          },
          tableNameById,
        );

  // Jev's column is shown where Jev scored a proposal of the run: its score beside the model's,
  // which is the row's (REQ-1789).
  const jevScored = proposals.some((p) => p.scored !== null);
  const columns: ListColumn<RunProposal>[] = [
    // A proposal with no score sorts below every scored one.
    { key: "score", label: t("proposalsTable.colScore"), sortValue: (p) => p.confidence ?? -1 },
    ...(jevScored
      ? [
          {
            key: "jev",
            label: t("proposalsTable.colJev"),
            sortValue: (p: RunProposal) => p.scored ?? -1,
          },
        ]
      : []),
    {
      key: "source",
      label: t("candidatesTable.colSource"),
      sortValue: (p) => label(p.source_table_id, p.source_column),
    },
    {
      key: "target",
      label: t("candidatesTable.colTarget"),
      sortValue: (p) => label(p.target_table_id, p.target_column),
    },
    { key: "kind", label: t("proposalsTable.colKind"), sortValue: (p) => p.kind },
  ];
  // Unsorted, the rows keep the order they were read in: the highest score first.
  const sort = useListSortGroup(proposals, columns, `${testId}-proposals`);

  const decidable = useMemo(
    () => proposals.filter((p) => p.decidable).map((p) => String(p.id)),
    [proposals],
  );
  // The list's own selection column is the accept box.
  const selection: ListSelection = {
    selected: new Set(decidable.filter((id) => state.accept.has(Number(id)))),
    shown: decidable,
    deleting: NONE,
    toggle: (key) => state.toggleAccept(Number(key)),
    toggleAll: state.toggleAcceptAll,
    clear: () => undefined,
    markDeleting: () => undefined,
  };

  const chosen = (ids: ReadonlySet<number>) =>
    proposals.filter((p) => p.decidable && ids.has(p.id)).length;
  const accepting = chosen(state.accept);
  const marking = chosen(state.notAgain);
  const applyLabel =
    accepting > 0 && marking > 0
      ? t("proposalsTable.applyBoth", { accept: accepting, notAgain: marking })
      : accepting > 0
        ? t("proposalsTable.applyAccept", { count: accepting })
        : marking > 0
          ? t("proposalsTable.applyNotAgain", { count: marking })
          : t("proposalsTable.apply");

  const note = (p: RunProposal) =>
    [
      p.reason === null
        ? ""
        : t(`proposalsTable.reason.${p.reason}`, {
            threshold: Math.round(state.threshold * 100),
          }),
      p.reduced_information ? t("candidatesTable.reducedInformation") : "",
    ]
      .filter(Boolean)
      .join(" ");

  return (
    <>
      {state.error && (
        <Alert color="red" mb="md" data-testid={`${testId}-proposals-error`}>
          {state.error}
        </Alert>
      )}
      {state.failures.length > 0 && (
        <Alert color="red" mb="md" data-testid={`${testId}-apply-failures`}>
          <Text fz="0.875rem">{t("proposalsTable.failed", { count: state.failures.length })}</Text>
          {state.failures.map(({ proposal, error }) => (
            <Text key={proposal.id} fz="0.875rem">
              {t("candidatesTable.failedOne", { name: nameOf(proposal), error })}
            </Text>
          ))}
        </Alert>
      )}
      {state.applied && (
        <Text c="var(--approve)" mb="md" fz="0.875rem" data-testid={`${testId}-applied`}>
          {t("proposalsTable.applied", {
            accepted: state.applied.accepted,
            notAgain: state.applied.notAgain,
          })}
        </Text>
      )}
      {proposals.length === 0 ? (
        state.proposals !== null && (
          <Text fz="sm" c="dimmed" data-testid={`${testId}-no-proposals`}>
            {t("proposalsTable.none")}
          </Text>
        )
      ) : (
        <>
          <Group justify="space-between" align="center" mb="xs" wrap="nowrap">
            <Text fz="sm" c="dimmed">
              {t("proposalsTable.intro", { threshold: Math.round(state.threshold * 100) })}
            </Text>
            <Button
              onClick={() => void state.apply(nameOf)}
              disabled={accepting + marking === 0}
              loading={state.applying}
              data-testid={`${testId}-apply`}
              style={{ flexShrink: 0 }}
            >
              {applyLabel}
            </Button>
          </Group>
          {/* The list fills the dialog and scrolls sideways within it where it cannot fit. */}
          <div style={{ overflowX: "auto" }}>
            <ListTable
              testId={`${testId}-proposals`}
              minWidth={900}
              style={{ width: "100%" }}
              selection={selection}
            >
              <ListHead
                sortGroup={sort}
                columns={[
                  { col: "score" },
                  ...(jevScored ? [{ col: "jev" }] : []),
                  { col: "source" },
                  { col: "target" },
                  t("candidatesTable.colCardinality"),
                  { col: "kind" },
                  t("proposalsTable.colReasoning"),
                  t("proposalsTable.colNote"),
                  // Pinned at the row's end, as a list's actions are: where the list is wider
                  // than the dialog it scrolls under this column, which is never cut.
                  <ListActionsHead key="not-again">
                    <span style={{ whiteSpace: "normal" }}>{t("proposalsTable.colNotAgain")}</span>
                  </ListActionsHead>,
                ]}
              />
              <Table.Tbody>
                {sort.items.map((item) => {
                  if (item.type !== "row") return null;
                  const p = item.row;
                  const name = nameOf(p);
                  return (
                    <ListRow
                      key={p.id}
                      testId={`${rowTestId}-${p.id}`}
                      itemKey={p.decidable ? p.id : undefined}
                    >
                      <Table.Td>
                        {p.confidence === null ? "" : `${percent(p.confidence)}%`}
                      </Table.Td>
                      {jevScored && (
                        <Table.Td data-testid={`${rowTestId}-jev-${p.id}`}>
                          {p.scored === null ? "" : `${percent(p.scored)}%`}
                        </Table.Td>
                      )}
                      <Table.Td>
                        <BreakableName name={label(p.source_table_id, p.source_column)} />
                      </Table.Td>
                      <Table.Td>
                        <BreakableName name={label(p.target_table_id, p.target_column)} />
                      </Table.Td>
                      <Table.Td>{p.cardinality ?? ""}</Table.Td>
                      <Table.Td>{t(`proposalsTable.kind.${p.kind}`)}</Table.Td>
                      <Table.Td>
                        {p.source_table_id === null
                          ? t("proposalsTable.asWritten", { answer: p.answer })
                          : (p.reasoning ?? "")}
                      </Table.Td>
                      <Table.Td>{note(p)}</Table.Td>
                      {/* The cell a row's actions take (ListActions), written as the table cell it is. */}
                      <Table.Td className="list-actions">
                        {p.decidable && (
                          <Checkbox
                            size="xs"
                            aria-label={t("proposalsTable.notAgainAria", { name })}
                            data-testid={`${rowTestId}-not-again-${p.id}`}
                            checked={state.notAgain.has(p.id)}
                            onChange={() => state.toggleNotAgain(p.id)}
                          />
                        )}
                      </Table.Td>
                    </ListRow>
                  );
                })}
              </Table.Tbody>
            </ListTable>
          </div>
        </>
      )}
    </>
  );
}
