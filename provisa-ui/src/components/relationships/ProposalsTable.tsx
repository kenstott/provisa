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
// pinned list header). A many-to-many pair (neither column a key) is accepted as a shared
// dimension: its row names the dimension, and Apply sends that name.
// The Score column carries the list's column filter, Score >= 10% when the results open, shown as
// a chip above the rows like any other filter; a line under them says how many it hides
// (REQ-413, amended 2026-11-18). Every other column has the list's column filter too, as the
// standard list gives its filterable columns: a number filter on the scores, a text filter (with
// its checklist of the values held) on the rest. Only the Score filter is kept for the next runs.

import { useMemo, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Checkbox,
  Group,
  NumberInput,
  Table,
  Text,
} from "@mantine/core";
import { Pencil, X } from "lucide-react";
import type { DiscoveryKind, RunProposal } from "../../api/admin";
import { FormField, FormLayout } from "../form/FormLayout";
import { BreakableName } from "../list/BreakableName";
import { ListActionsHead, ListHead, ListRow, ListTable } from "../list/ListTable";
import type { ListSelection } from "../list/listSelection";
import { useListSortGroup, type ListColumn } from "../list/useListSortGroup";
import { ColumnFilterMenu } from "../../pages/sql/ColumnFilterMenu";
import { describeFilter } from "../../pages/sql/columnFilterLabel";
import {
  cellKey,
  matchesFilter,
  type ActiveFilter,
  type ColumnKind,
  type FilterSpec,
} from "../../pages/sql/columnFilter";
import { measureLines } from "./pairMeasure";
import { ProposalEditDialog } from "./ProposalEditDialog";
import { candidateName } from "./relationship-types";
import { scorePercent, type RunProposalsState } from "./useRunProposals";

interface ProposalsTableProps {
  state: RunProposalsState;
  tableDomainById: Record<string, string>;
  tableNameById: Record<string, string>;
  /** The dialog's New run, shown beside Apply. */
  newRun: ReactNode;
}

const percent = (confidence: number) => (confidence * 100).toFixed(0);
const NONE = new Set<string>();

/** A column the list filters: its kind, and the value its cell shows, which the filter is against. */
interface FilterCol {
  key: string;
  label: string;
  kind: ColumnKind;
  cell: (p: RunProposal) => string | number | null;
}

/** A column's checklist: the values the run's rows hold, as their cells show them. */
function valuesOf(rows: RunProposal[], col: FilterCol) {
  const counts = new Map<string, number>();
  for (const p of rows) {
    const k = cellKey(col.cell(p));
    counts.set(k, (counts.get(k) ?? 0) + 1);
  }
  const entries = [...counts.entries()].map(([value, count]) => ({ value, count }));
  return col.kind === "number"
    ? entries.sort((a, b) => Number(b.value) - Number(a.value))
    : entries.sort((a, b) => a.value.localeCompare(b.value));
}

/** What produced a candidate no values were compared for: the rule that proposed it, else the model. */
const UNMEASURED_RULE_KEY: Record<DiscoveryKind, string> = {
  foreign_keys: "proposalsTable.measure.ruleForeignKeys",
  key_names: "proposalsTable.measure.ruleKeyNames",
  semantics: "proposalsTable.measure.modelScore",
};

export function ProposalsTable({
  state,
  tableDomainById,
  tableNameById,
  newRun,
}: ProposalsTableProps) {
  const { t } = useTranslation();
  const proposals = useMemo(() => state.proposals ?? [], [state.proposals]);
  // The row whose edit dialog is open (REQ-2262).
  const [editing, setEditing] = useState<RunProposal | null>(null);

  const label = (tableId: number | null, column: string | null) => {
    if (tableId === null || column === null) return "";
    const domain = tableDomainById[tableId];
    const table = tableNameById[tableId] ?? String(tableId);
    return domain ? `${domain}.${table}.${column}` : `${table}.${column}`;
  };
  const nameOf = (p: RunProposal) =>
    p.dimension
      ? p.dimension.name
      : p.source_table_id === null || p.target_table_id === null || p.source_column === null
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

  // What accepting a many-to-many row does comes first: the cell shows three lines, and the
  // dimension's name must be among them.
  const note = (p: RunProposal) =>
    [
      // REQ-1586: a junction suggestion names the junction its two ends are joined through.
      p.via ? t("proposalsTable.through", { table: tableNameById[p.via.table_id] }) : "",
      p.dimension
        ? t(
            p.dimension.reused ? "proposalsTable.dimensionJoined" : "proposalsTable.dimensionMade",
            { name: p.dimension.name },
          )
        : "",
      p.decidable && !p.acceptable ? t("proposalsTable.twoDimensions") : "",
      p.reason === null
        ? ""
        : t(`proposalsTable.reason.${p.reason}`, {
            threshold: Math.round(state.keptAt * 100),
          }),
      p.reduced_information ? t("candidatesTable.reducedInformation") : "",
      p.admitted_on_key_values ? t("proposalsTable.admittedOnKeyValues") : "",
    ]
      .filter(Boolean)
      .join(" ");
  const measured = (p: RunProposal) =>
    [
      ...(p.measure ? measureLines(p.measure, t) : []),
      p.source_table_id !== null && !p.measured ? t(UNMEASURED_RULE_KEY[p.kind]) : "",
    ]
      .filter(Boolean)
      .join(" ");
  const reasoning = (p: RunProposal) =>
    p.source_table_id === null
      ? t("proposalsTable.asWritten", { answer: p.answer })
      : (p.reasoning ?? "");
  const whole = (v: number | null) => (v === null ? null : Number(percent(v)));

  // Jev's column is shown where Jev scored a proposal of the run: its score beside the model's,
  // which is the row's (REQ-1789).
  const jevScored = proposals.some((p) => p.scored !== null);
  const filterCols: FilterCol[] = [
    { key: "score", label: t("proposalsTable.colScore"), kind: "number", cell: scorePercent },
    {
      key: "confidence",
      label: t("proposalsTable.colConfidence"),
      kind: "number",
      cell: (p) => whole(p.confidence),
    },
    ...(jevScored
      ? [
          {
            key: "jev",
            label: t("proposalsTable.colJev"),
            kind: "number" as const,
            cell: (p: RunProposal) => whole(p.scored),
          },
        ]
      : []),
    {
      key: "source",
      label: t("candidatesTable.colSource"),
      kind: "text",
      cell: (p) => label(p.source_table_id, p.source_column),
    },
    {
      key: "target",
      label: t("candidatesTable.colTarget"),
      kind: "text",
      cell: (p) => label(p.target_table_id, p.target_column),
    },
    {
      key: "cardinality",
      label: t("candidatesTable.colCardinality"),
      kind: "text",
      cell: (p) => p.cardinality,
    },
    { key: "measure", label: t("proposalsTable.colMeasure"), kind: "text", cell: measured },
    {
      key: "kind",
      label: t("proposalsTable.colKind"),
      kind: "text",
      cell: (p) => t(`proposalsTable.kind.${p.kind}`),
    },
    { key: "reasoning", label: t("proposalsTable.colReasoning"), kind: "text", cell: reasoning },
    { key: "note", label: t("proposalsTable.colNote"), kind: "text", cell: note },
  ];
  const columns: ListColumn<RunProposal>[] = filterCols.map((c) => ({
    key: c.key,
    label: c.label,
    // A proposal with no score sorts below every scored one.
    sortValue:
      c.kind === "number" ? (p) => (c.cell(p) as number | null) ?? -1 : (p) => c.cell(p) ?? "",
  }));

  // The filters on the columns other than Score, which the run's state keeps.
  const [filters, setFilters] = useState<Record<string, FilterSpec>>({});
  const setFilter = (key: string, spec: FilterSpec | null) => {
    if (key === "score") {
      state.setScoreFilter(spec);
      return;
    }
    setFilters((prev) => {
      const next = { ...prev };
      if (spec === null) delete next[key];
      else next[key] = spec;
      return next;
    });
  };
  const specOf = (key: string): FilterSpec | null =>
    key === "score" ? state.scoreFilter : (filters[key] ?? null);
  const active: (ActiveFilter & { key: string })[] = filterCols.flatMap((c) => {
    const spec = specOf(c.key);
    return spec === null ? [] : [{ key: c.key, col: c.label, kind: c.kind, spec }];
  });
  // Unsorted, the rows keep the order they were read in: the highest score first. Only the rows
  // every filter shows are listed.
  const listed = state.shown.filter((p) =>
    filterCols.every((c) => {
      const spec = c.key === "score" ? null : (filters[c.key] ?? null);
      return spec === null || matchesFilter(c.kind, spec, c.cell(p));
    }),
  );
  const sort = useListSortGroup(listed, columns, "discover-proposals");
  const hidden = proposals.length - listed.length;
  const listedIds = new Set(listed.map((p) => p.id));

  // Select-all and Accept act only on the rows the filters show.
  const acceptable = listed.filter((p) => p.acceptable).map((p) => p.id);
  // The list's own selection column is the accept box.
  const selection: ListSelection = {
    selected: new Set(acceptable.filter((id) => state.accept.has(id)).map(String)),
    shown: acceptable.map(String),
    deleting: NONE,
    toggle: (key) => state.toggleAccept(Number(key)),
    toggleAll: () => state.toggleAcceptAll(acceptable),
    clear: () => undefined,
    markDeleting: () => undefined,
  };

  const chosen = (ids: ReadonlySet<number>) =>
    proposals.filter((p) => p.decidable && ids.has(p.id)).length;
  const accepting = listed.filter((p) => p.acceptable && state.accept.has(p.id)).length;
  const marking = chosen(state.notAgain);
  const applyLabel =
    accepting > 0 && marking > 0
      ? t("proposalsTable.applyBoth", { accept: accepting, notAgain: marking })
      : accepting > 0
        ? t("proposalsTable.applyAccept", { count: accepting })
        : marking > 0
          ? t("proposalsTable.applyNotAgain", { count: marking })
          : t("proposalsTable.apply");

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
              onClick={() => void state.apply(listedIds)}
              disabled={accepting + marking === 0}
              loading={state.applying}
              data-testid="discover-apply"
            >
              {applyLabel}
            </Button>
          )}
        </Group>
      </Group>
      {proposals.length > 0 && active.length > 0 && (
        <Group gap="xs" mb="xs" data-testid="discover-filter-chips">
          {active.map((f) => (
            <Badge
              key={f.key}
              size="md"
              variant="light"
              tt="none"
              data-testid={`discover-filter-chip-${f.key}`}
              rightSection={
                <ActionIcon
                  size="xs"
                  variant="transparent"
                  aria-label={t("columnFilter.removeFilter", { column: f.col })}
                  data-testid={`discover-filter-chip-remove-${f.key}`}
                  onClick={() => setFilter(f.key, null)}
                >
                  <X size={11} />
                </ActionIcon>
              }
            >
              {describeFilter(t, f)}
            </Badge>
          ))}
        </Group>
      )}
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
                ...filterCols.map((c) => ({
                  col: c.key,
                  filter: (
                    <ColumnFilterMenu
                      col={c.label}
                      kind={c.kind}
                      current={specOf(c.key)}
                      values={valuesOf(proposals, c)}
                      valuesFromPage={false}
                      rowsHeld={proposals.length}
                      onApply={(spec) => setFilter(c.key, spec)}
                    />
                  ),
                })),
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
                      {state.edits.get(p.id)?.cardinality ?? p.cardinality ?? ""}
                    </Table.Td>
                    <Table.Td data-testid={`proposal-measure-${p.id}`}>{measured(p)}</Table.Td>
                    <Table.Td>{t(`proposalsTable.kind.${p.kind}`)}</Table.Td>
                    <Table.Td>{reasoning(p)}</Table.Td>
                    <Table.Td>{note(p)}</Table.Td>
                    {/* The cell a row's actions take (ListActions), written as the table cell it is. */}
                    <Table.Td className="list-actions">
                      {p.acceptable && (
                        <ActionIcon
                          size="sm"
                          variant="subtle"
                          aria-label={t("proposalsTable.editAria", { name })}
                          data-testid={`proposal-edit-${p.id}`}
                          onClick={() => setEditing(p)}
                        >
                          <Pencil size={14} />
                        </ActionIcon>
                      )}
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
      {editing && (
        <ProposalEditDialog
          name={nameOf(editing)}
          // A junction or shared-dimension row's cardinality is fixed by what accepting makes.
          cardinality={editing.via || editing.dimension ? null : editing.cardinality}
          edit={state.edits.get(editing.id)}
          onSave={(edit) => state.setEdit(editing.id, edit)}
          onClose={() => setEditing(null)}
        />
      )}
      {proposals.length > 0 && active.length > 0 && (
        <Text fz="sm" c="dimmed" mt="xs" data-testid="discover-filter-hidden">
          {t("proposalsTable.hiddenByFilter", { count: hidden })}
        </Text>
      )}
    </>
  );
}
