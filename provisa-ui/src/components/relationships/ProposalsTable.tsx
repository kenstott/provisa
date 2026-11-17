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
// listed with why and has no box. One Apply says what it will do, with the counts. Each row
// shows its pair's values measured against each other; the Score is the measure's where values
// were compared, the model's own beside it. The threshold rows start ticked from is the
// person's own, set here. The run's actions (New run, Apply) sit in one row above the table;
// the table scrolls under its pinned header row in the room the dialog leaves it (REQ-1587's
// pinned list header).

import { useMemo, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Checkbox, Group, NumberInput, Table, Text } from "@mantine/core";
import type { RunProposal } from "../../api/admin";
import { FormField, FormLayout } from "../form/FormLayout";
import { BreakableName } from "../list/BreakableName";
import { ListActionsHead, ListHead, ListRow, ListTable } from "../list/ListTable";
import type { ListSelection } from "../list/listSelection";
import { useListSortGroup, type ListColumn } from "../list/useListSortGroup";
import { measureLines } from "./pairMeasure";
import { candidateName } from "./relationship-types";
import type { RunProposalsState } from "./useRunProposals";

interface ProposalsTableProps {
  state: RunProposalsState;
  tableDomainById: Record<string, string>;
  tableNameById: Record<string, string>;
  /** The dialog's New run, shown beside Apply. */
  newRun: ReactNode;
}

const percent = (confidence: number) => (confidence * 100).toFixed(0);
const NONE = new Set<string>();

export function ProposalsTable({
  state,
  tableDomainById,
  tableNameById,
  newRun,
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
            confidence: p.score ?? 0,
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
    { key: "score", label: t("proposalsTable.colScore"), sortValue: (p) => p.score ?? -1 },
    { key: "model", label: t("proposalsTable.colModel"), sortValue: (p) => p.confidence ?? -1 },
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
  const sort = useListSortGroup(proposals, columns, "discover-proposals");

  const acceptable = useMemo(
    () => proposals.filter((p) => p.acceptable).map((p) => String(p.id)),
    [proposals],
  );
  // The list's own selection column is the accept box.
  const selection: ListSelection = {
    selected: new Set(acceptable.filter((id) => state.accept.has(Number(id)))),
    shown: acceptable,
    deleting: NONE,
    toggle: (key) => state.toggleAccept(Number(key)),
    toggleAll: state.toggleAcceptAll,
    clear: () => undefined,
    markDeleting: () => undefined,
  };

  const chosen = (ids: ReadonlySet<number>) =>
    proposals.filter((p) => p.decidable && ids.has(p.id)).length;
  const accepting = proposals.filter((p) => p.acceptable && state.accept.has(p.id)).length;
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
            threshold: Math.round(state.keptAt * 100),
          }),
      p.decidable && !p.acceptable ? t("proposalsTable.manyToMany") : "",
      p.reduced_information ? t("candidatesTable.reducedInformation") : "",
    ]
      .filter(Boolean)
      .join(" ");

  return (
    <>
      {state.error && (
        <Alert color="red" mb="md" style={{ flexShrink: 0 }} data-testid="discover-proposals-error">
          {state.error}
        </Alert>
      )}
      {state.failures.length > 0 && (
        <Alert color="red" mb="md" style={{ flexShrink: 0 }} data-testid="discover-apply-failures">
          <Text fz="0.875rem">{t("proposalsTable.failed", { count: state.failures.length })}</Text>
          {state.failures.map(({ proposal, error }) => (
            <Text key={proposal.id} fz="0.875rem">
              {t("candidatesTable.failedOne", { name: nameOf(proposal), error })}
            </Text>
          ))}
        </Alert>
      )}
      {state.applied && (
        <Text c="var(--approve)" mb="md" fz="0.875rem" data-testid="discover-applied">
          {t("proposalsTable.applied", {
            accepted: state.applied.accepted,
            notAgain: state.applied.notAgain,
          })}
        </Text>
      )}
      <Group justify="space-between" align="center" mb="xs" wrap="nowrap">
        {proposals.length > 0 ? (
          <Group gap="md" align="flex-end" wrap="nowrap">
            <FormLayout style={{ flexShrink: 0 }}>
              <FormField size="short" hint={t("proposalsTable.thresholdHint")}>
                <NumberInput
                  label={t("proposalsTable.threshold")}
                  data-testid="discover-threshold"
                  value={Math.round(state.threshold * 100)}
                  onChange={(v) => {
                    if (typeof v === "number") state.setThreshold(v / 100);
                  }}
                  min={0}
                  max={100}
                  step={5}
                  suffix="%"
                  clampBehavior="strict"
                  allowDecimal={false}
                />
              </FormField>
            </FormLayout>
            <Text fz="sm" c="dimmed">
              {t("proposalsTable.intro")}
            </Text>
          </Group>
        ) : state.proposals !== null ? (
          <Text fz="sm" c="dimmed" data-testid="discover-no-proposals">
            {t("proposalsTable.none")}
          </Text>
        ) : (
          <span />
        )}
        <Group gap="xs" wrap="nowrap" style={{ flexShrink: 0 }}>
          {newRun}
          {proposals.length > 0 && (
            <Button
              onClick={() => void state.apply(nameOf)}
              disabled={accepting + marking === 0}
              loading={state.applying}
              data-testid="discover-apply"
            >
              {applyLabel}
            </Button>
          )}
        </Group>
      </Group>
      {proposals.length > 0 && (
        // The list's pinned header (REQ-1587): the rows scroll beneath it in the height the
        // dialog leaves, and sideways where the columns cannot fit.
        <div className="page-sticky-head" data-testid="discover-proposals-scroll">
          <ListTable
            testId="discover-proposals"
            minWidth={1100}
            style={{ width: "100%" }}
            selection={selection}
          >
            <ListHead
              sortGroup={sort}
              columns={[
                { col: "score" },
                { col: "model" },
                ...(jevScored ? [{ col: "jev" }] : []),
                { col: "source" },
                { col: "target" },
                t("candidatesTable.colCardinality"),
                t("proposalsTable.colMeasure"),
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
                    testId={`proposal-${p.id}`}
                    itemKey={p.acceptable ? p.id : undefined}
                  >
                    <Table.Td data-testid={`proposal-score-${p.id}`}>
                      {p.score === null ? "" : `${percent(p.score)}%`}
                    </Table.Td>
                    <Table.Td data-testid={`proposal-model-${p.id}`}>
                      {p.confidence === null ? "" : `${percent(p.confidence)}%`}
                    </Table.Td>
                    {jevScored && (
                      <Table.Td data-testid={`proposal-jev-${p.id}`}>
                        {p.scored === null ? "" : `${percent(p.scored)}%`}
                      </Table.Td>
                    )}
                    <Table.Td>
                      <BreakableName name={label(p.source_table_id, p.source_column)} />
                    </Table.Td>
                    <Table.Td>
                      <BreakableName name={label(p.target_table_id, p.target_column)} />
                    </Table.Td>
                    <Table.Td data-testid={`proposal-cardinality-${p.id}`}>
                      {p.cardinality ?? ""}
                    </Table.Td>
                    <Table.Td data-testid={`proposal-measure-${p.id}`}>
                      {[
                        ...(p.measure ? measureLines(p.measure, t) : []),
                        p.source_table_id !== null && !p.measured
                          ? t("proposalsTable.measure.modelScore")
                          : "",
                      ]
                        .filter(Boolean)
                        .join(" ")}
                    </Table.Td>
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
                          data-testid={`proposal-not-again-${p.id}`}
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
      )}
    </>
  );
}
