// Copyright (c) 2026 Kenneth Stott
// Canary: 1d7a4c93-5e28-4b6f-a0c2-9f3b8e6d1a57
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2247: a data catalog entry's differences with its catalog, and its standing choices.
//
// The difference list is the shared list, grouped by kind and then by table or term, each heading
// with its count. A row shows Provisa's value, the catalog's, the value last exchanged and which
// side moved off it, with Accept for each side; a table or term heading takes a side for all its
// rows at once. Provisa's side is an Export and the catalog's an Import, so a side whose direction
// is off is not offered, nor the catalog's side of a kind Provisa cannot take yet; each says why.
//
// A standing choice is kept per direction and kind: take Provisa's value, take the catalog's, or
// hold. A kind's heading sets it ("always"), and the entry's table of standing choices shows and
// changes every kind's, for each direction the entry runs.

import { useEffect, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Group, Select, Stack, Table, Text, Title } from "@mantine/core";
import { TriangleAlert } from "lucide-react";
import {
  applyReview,
  DIFF_KINDS,
  EXCHANGE_DIRECTIONS,
  fetchReview,
  fetchStandingChoices,
  IMPORTABLE_KINDS,
  setStandingChoice,
  STANDING_CHOICES,
  type DiffDecision,
  type DiffKind,
  type DiffRow,
  type DiffSide,
  type DiffValue,
  type EntryReview,
  type EntryStandingChoices,
  type ExchangeDirection,
  type StandingChoice,
} from "../../../api/syncing";
import {
  ListActions,
  ListActionsHead,
  ListGroupRow,
  ListHead,
  ListRow,
  ListTable,
} from "../../list/ListTable";
import { ListCell, ListPills } from "../../list/ListCell";
import { Tip } from "../../Tip";

const errorText = (e: unknown) => (e instanceof Error ? e.message : String(e));

/** Why a side of a kind cannot be taken on this entry, or null when it can. */
function useRefusal(directions: Record<ExchangeDirection, boolean>) {
  const { t } = useTranslation();
  return (side: DiffSide, kind: DiffKind): string | null => {
    if (side === "provisa") return directions.export ? null : t("syncingTab.diffExportOff");
    if (!directions.import) return t("syncingTab.diffImportOff");
    return IMPORTABLE_KINDS.includes(kind) ? null : t("syncingTab.diffKindNotImportable");
  };
}

/** Accept for one side; disabled with the reason in its tip when that side cannot be taken. */
function AcceptButton({
  side,
  refusal,
  busy,
  testId,
  onAccept,
}: {
  side: DiffSide;
  refusal: string | null;
  busy: boolean;
  testId: string;
  onAccept: () => void;
}) {
  const { t } = useTranslation();
  const button = (
    <Button
      size="xs"
      variant="light"
      disabled={refusal !== null || busy}
      data-testid={testId}
      onClick={onAccept}
    >
      {side === "provisa" ? t("syncingTab.acceptProvisa") : t("syncingTab.acceptCatalog")}
    </Button>
  );
  // A disabled button takes no pointer, so its reason is on what holds it.
  return refusal === null ? (
    button
  ) : (
    <Tip text={refusal}>
      <span>{button}</span>
    </Tip>
  );
}

/** A value as the list draws it: a text, or its names as pills; none says so. */
function Value({ value }: { value: DiffValue }) {
  const { t } = useTranslation();
  if (Array.isArray(value)) {
    return value.length === 0 ? (
      <Text size="sm" c="dimmed">
        {t("syncingTab.diffNone")}
      </Text>
    ) : (
      <ListPills values={value} />
    );
  }
  return value === "" ? (
    <Text size="sm" c="dimmed">
      {t("syncingTab.diffNone")}
    </Text>
  ) : (
    <ListCell text={value} />
  );
}

/** The select of one kind's standing choice in one direction; cleared, the kind has none. */
function StandingSelect({
  kind,
  value,
  testId,
  ariaLabel,
  onChange,
}: {
  kind: DiffKind;
  value: StandingChoice | null;
  testId: string;
  ariaLabel: string;
  onChange: (choice: StandingChoice | null) => void;
}) {
  const { t } = useTranslation();
  const importable = IMPORTABLE_KINDS.includes(kind);
  const data = STANDING_CHOICES.map((choice) => ({
    value: choice,
    label:
      choice === "catalog" && !importable
        ? t("syncingTab.choiceCatalogUnsupported")
        : t(`syncingTab.choice_${choice}`),
    disabled: choice === "catalog" && !importable,
  }));
  return (
    <Select
      size="xs"
      w={180}
      data={data}
      value={value}
      clearable
      allowDeselect={false}
      placeholder={t("syncingTab.choiceNone")}
      aria-label={ariaLabel}
      data-testid={testId}
      onChange={(next) => onChange((next as StandingChoice | null) ?? null)}
    />
  );
}

const COLS = 5;

/** An object's name within its table or term: a column by its own name, the table or term by
 *  its last part. The whole name is the cell's hint when it is cut. */
const shortName = (row: DiffRow) =>
  row.object === row.group
    ? (row.group.split(".").pop() as string)
    : row.object.startsWith(`${row.group}.`)
      ? row.object.slice(row.group.length + 1)
      : row.object;

function DifferenceList({
  review,
  standing,
  busy,
  onDecide,
  onStanding,
}: {
  review: EntryReview;
  /** The entry's standing choices; null while they are not read, when a kind offers none. */
  standing: EntryStandingChoices["standing_choices"] | null;
  busy: boolean;
  onDecide: (decision: DiffDecision) => void;
  onStanding: (direction: ExchangeDirection, kind: DiffKind, choice: StandingChoice | null) => void;
}) {
  const { t } = useTranslation();
  const refusal = useRefusal(review.directions);
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(new Set());
  const toggle = (key: string) =>
    setCollapsed((was) => {
      const next = new Set(was);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  const running = standing === null ? [] : EXCHANGE_DIRECTIONS.filter((d) => review.directions[d]);

  if (review.total === 0) {
    // A direction that is off lists no differences (REQ-2247): both off says why.
    const bothOff = !review.directions.export && !review.directions.import;
    return (
      <Text size="sm" c="dimmed" data-testid="syncing-diff-none">
        {t(bothOff ? "syncingTab.diffBothOff" : "syncingTab.diffNoDifferences")}
      </Text>
    );
  }

  const rows: ReactNode[] = [];
  for (const kind of review.kinds) {
    const kindKey = kind.kind;
    rows.push(
      <ListGroupRow
        key={`kind-${kindKey}`}
        testId={`syncing-diff-kind-${kindKey}`}
        colSpan={COLS}
        level={1}
        label={t(`syncingTab.kind_${kindKey}`)}
        count={kind.count}
        collapsed={collapsed.has(kindKey)}
        onToggle={() => toggle(kindKey)}
        actions={running.map((direction) => (
          <Group key={direction} gap={4} wrap="nowrap">
            <Text size="xs" c="dimmed">
              {t(`syncingTab.always_${direction}`)}
            </Text>
            <StandingSelect
              kind={kindKey}
              value={standing?.[direction][kindKey] ?? null}
              testId={`syncing-always-${direction}-${kindKey}`}
              ariaLabel={t(`syncingTab.always_${direction}`)}
              onChange={(choice) => onStanding(direction, kindKey, choice)}
            />
          </Group>
        ))}
      />,
    );
    if (collapsed.has(kindKey)) continue;
    for (const group of kind.groups) {
      const groupKey = `${kindKey}:${group.group}`;
      rows.push(
        <ListGroupRow
          key={`group-${groupKey}`}
          testId={`syncing-diff-group-${kindKey}-${group.group}`}
          colSpan={COLS}
          level={2}
          label={group.group}
          count={group.count}
          collapsed={collapsed.has(groupKey)}
          onToggle={() => toggle(groupKey)}
          actions={(["provisa", "catalog"] as DiffSide[]).map((side) => (
            <AcceptButton
              key={side}
              side={side}
              refusal={refusal(side, kindKey)}
              busy={busy}
              testId={`syncing-accept-group-${side}-${kindKey}-${group.group}`}
              onAccept={() => onDecide({ side, kind: kindKey, group: group.group })}
            />
          ))}
        />,
      );
      if (collapsed.has(groupKey)) continue;
      for (const row of group.rows) {
        rows.push(
          <ListRow
            key={`row-${kindKey}:${row.object}`}
            itemKey={`${kindKey}:${row.object}`}
            testId={`syncing-diff-row-${kindKey}-${row.object}`}
          >
            <Table.Td>
              <ListCell text={shortName(row)} />
            </Table.Td>
            <Table.Td>
              <Value value={row.provisa} />
            </Table.Td>
            <Table.Td>
              <Value value={row.catalog} />
            </Table.Td>
            <Table.Td>
              {/* The value last exchanged, and under it which side moved off it since. */}
              {row.last_exchanged === null ? (
                <Text size="sm" c="dimmed">
                  {t("syncingTab.diffNeverExchanged")}
                </Text>
              ) : (
                <Stack gap={2}>
                  <Value value={row.last_exchanged.value} />
                  {row.changed !== null && (
                    <Text size="xs" c="dimmed">
                      {t(`syncingTab.changed_${row.changed}`)}
                    </Text>
                  )}
                </Stack>
              )}
            </Table.Td>
            <ListActions>
              <Stack gap={4} align="flex-start">
                {(["provisa", "catalog"] as DiffSide[]).map((side) => (
                  <AcceptButton
                    key={side}
                    side={side}
                    refusal={refusal(side, kindKey)}
                    busy={busy}
                    testId={`syncing-accept-${side}-${kindKey}-${row.object}`}
                    onAccept={() => onDecide({ side, kind: kindKey, object: row.object })}
                  />
                ))}
              </Stack>
            </ListActions>
          </ListRow>,
        );
      }
    }
  }

  return (
    <ListTable testId="syncing-diff-list" minWidth={560}>
      <ListHead
        columns={[
          t("syncingTab.diffObject"),
          t("syncingTab.diffProvisa"),
          t("syncingTab.diffCatalog"),
          t("syncingTab.diffLastExchanged"),
          <ListActionsHead key="actions" />,
        ]}
      />
      <Table.Tbody>{rows}</Table.Tbody>
    </ListTable>
  );
}

function StandingChoicesTable({
  held,
  onStanding,
}: {
  held: EntryStandingChoices;
  onStanding: (direction: ExchangeDirection, kind: DiffKind, choice: StandingChoice | null) => void;
}) {
  const { t } = useTranslation();
  const running = EXCHANGE_DIRECTIONS.filter((d) => held.directions[d]);
  return (
    <ListTable testId="syncing-standing-choices">
      <ListHead
        columns={[
          t("syncingTab.standingKind"),
          ...running.map((d) => t(`syncingTab.direction_${d}`)),
        ]}
      />
      <Table.Tbody>
        {DIFF_KINDS.map((kind) => (
          <ListRow key={kind} itemKey={kind} testId={`syncing-standing-row-${kind}`}>
            <Table.Td>
              <ListCell text={t(`syncingTab.kind_${kind}`)} />
            </Table.Td>
            {running.map((direction) => (
              <Table.Td key={direction}>
                <StandingSelect
                  kind={kind}
                  value={held.standing_choices[direction][kind] ?? null}
                  testId={`syncing-standing-${direction}-${kind}`}
                  ariaLabel={`${t(`syncingTab.kind_${kind}`)}: ${t(`syncingTab.direction_${direction}`)}`}
                  onChange={(choice) => onStanding(direction, kind, choice)}
                />
              </Table.Td>
            ))}
          </ListRow>
        ))}
      </Table.Tbody>
    </ListTable>
  );
}

/** A data catalog entry's differences with its catalog and its standing choices. */
export function EntrySync({ entryId }: { entryId: string }) {
  const { t } = useTranslation();
  const [review, setReview] = useState<EntryReview | null>(null);
  const [held, setHeld] = useState<EntryStandingChoices | null>(null);
  const [problem, setProblem] = useState<string | null>(null);
  const [standingProblem, setStandingProblem] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    fetchReview(entryId).then(setReview, (e) => setProblem(errorText(e)));
    fetchStandingChoices(entryId).then(setHeld, (e) => setStandingProblem(errorText(e)));
  }, [entryId]);

  const decide = async (decision: DiffDecision) => {
    setBusy(true);
    setProblem(null);
    try {
      await applyReview(entryId, [decision]);
      // The list read again: what was taken is gone from it, and a group may have shrunk.
      setReview(await fetchReview(entryId));
    } catch (e) {
      setProblem(errorText(e));
    } finally {
      setBusy(false);
    }
  };

  const stand = async (
    direction: ExchangeDirection,
    kind: DiffKind,
    choice: StandingChoice | null,
  ) => {
    setStandingProblem(null);
    try {
      setHeld(await setStandingChoice(entryId, direction, kind, choice));
    } catch (e) {
      setStandingProblem(errorText(e));
    }
  };

  return (
    <Stack gap="lg">
      <Stack gap="xs" data-testid="syncing-differences">
        <Title order={6}>{t("syncingTab.differences")}</Title>
        <Text size="sm" c="dimmed">
          {t("syncingTab.differencesHelp")}
        </Text>
        {problem && (
          <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="syncing-diff-problem">
            {problem}
          </Alert>
        )}
        {review === null ? (
          !problem && (
            <Text size="sm" c="dimmed">
              {t("syncingTab.loading")}
            </Text>
          )
        ) : (
          <DifferenceList
            review={review}
            standing={held === null ? null : held.standing_choices}
            busy={busy}
            onDecide={(d) => void decide(d)}
            onStanding={(d, k, c) => void stand(d, k, c)}
          />
        )}
      </Stack>
      <Stack gap="xs">
        <Title order={6}>{t("syncingTab.standingChoices")}</Title>
        <Text size="sm" c="dimmed">
          {t("syncingTab.standingChoicesHelp")}
        </Text>
        {standingProblem && (
          <Alert
            color="red"
            icon={<TriangleAlert size={16} />}
            data-testid="syncing-standing-problem"
          >
            {standingProblem}
          </Alert>
        )}
        {held && <StandingChoicesTable held={held} onStanding={(d, k, c) => void stand(d, k, c)} />}
      </Stack>
    </Stack>
  );
}
