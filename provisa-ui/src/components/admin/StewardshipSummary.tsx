// Copyright (c) 2026 Kenneth Stott
// Canary: 8f1d2a63-5b7e-4c90-a3d4-e6b20c9f7a15
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2144 (A STEWARD SEES THE IMPACT OF EACH ACTION): the stewardship summary -- the terms agents
// can see, the terms one approval from reaching data, and the approved terms that reach no
// data -- and the defined, grounded drafts a steward approves together. REQ-2259: it is Context
// Health's one action, and each term links to the Ontology page through the entity link helper.
// An enterprise holds thousands of terms, so each of the three is a standard list: sortable, under
// its own filter (in the URL), at a fixed height that scrolls.

/** The height each of the three lists keeps, its rows scrolling inside it. */
const LIST_HEIGHT = 280;

import { useCallback, useEffect, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Checkbox,
  Group,
  Loader,
  Modal,
  SimpleGrid,
  Stack,
  Table,
  Text,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { CheckCheck } from "lucide-react";
import {
  bulkApproveTerms,
  fetchStewardshipSummary,
  listApprovableTerms,
  type StewardshipSummary as Summary,
  type TermRef,
} from "../../api/glossary";
import { EntityLink } from "../EntityLink";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { ListRow } from "../list/ListTable";
import { SortGroupTable } from "../list/SortGroupTable";
import type { ListColumn } from "../list/useListSortGroup";
import { useUrlFilter } from "../list/useUrlFilter";
import { matchesQuery } from "../../lib/searchItems";
import { FilterInput } from "./FilterInput";
import { TermStatusBadge } from "./TermRoleBadge";

/** A term, linked to the Ontology page for a viewer who holds its right (REQ-2259). */
function TermLink({ term, testId }: { term: TermRef; testId: string }) {
  return (
    <Text span size="sm">
      <EntityLink kind="term" id={term.term_id} testId={testId}>
        {term.name}
      </EntityLink>
    </Text>
  );
}

/** One of the summary's three lists: its title and count, its filter, and its terms. */
function TermList<T extends TermRef>({
  title,
  testId,
  filterParam,
  rows,
  columns,
  texts,
}: {
  title: string;
  testId: string;
  /** The URL parameter the list's filter is kept in. */
  filterParam: string;
  rows: T[];
  /** The columns after the term's name. */
  columns: { column: ListColumn<T>; cell: (row: T) => ReactNode }[];
  /** What the filter matches in a row besides the term's name. */
  texts: (row: T) => string[];
}) {
  const { t } = useTranslation();
  const [search, setSearch] = useUrlFilter(filterParam);
  const shown = rows.filter((row) => matchesQuery([row.name, ...texts(row)], search));
  const all: ListColumn<T>[] = [
    { key: "term", label: t("glossaryTab.stewardshipColTerm"), sortValue: (r) => r.name },
    ...columns.map((c) => c.column),
  ];
  return (
    <Stack gap={6} data-testid={testId} style={{ minWidth: 0 }}>
      <Group gap={6} wrap="nowrap">
        <Title order={6}>{title}</Title>
        <Badge size="sm" variant="light" color="gray" data-testid={`${testId}-count`}>
          {rows.length}
        </Badge>
      </Group>
      <FilterInput
        value={search}
        onChange={setSearch}
        placeholder={t("glossaryTab.stewardshipFilter")}
        testId={`${testId}-filter`}
        remember={false}
      />
      <div style={{ height: LIST_HEIGHT, overflowY: "auto" }} data-testid={`${testId}-scroll`}>
        <SortGroupTable
          testPrefix={testId}
          rows={shown}
          columns={all}
          headers={all.map((c) => ({ col: c.key }))}
          colSpan={all.length}
          rowKey={(r) => r.term_id}
          initialSortCol="term"
          empty={t("glossaryTab.noneListed")}
          emptyTestId={`${testId}-empty`}
          render={(row) => (
            <ListRow key={row.term_id} testId={`${testId}-row-${row.term_id}`}>
              <Table.Td>
                <TermLink term={row} testId={`${testId}-term-${row.term_id}`} />
              </Table.Td>
              {columns.map((c) => (
                <Table.Td key={c.column.key}>{c.cell(row)}</Table.Td>
              ))}
            </ListRow>
          )}
        />
      </div>
    </Stack>
  );
}

/** The summary, read when shown and again after a bulk approval (``onApproved`` tells the page). */
export function StewardshipSummary({
  canEdit,
  onApproved,
}: {
  canEdit: boolean;
  onApproved: () => void;
}) {
  const { t } = useTranslation();
  const [summary, setSummary] = useState<Summary | null>(null);
  const [error, setError] = useState("");
  const [seq, setSeq] = useState(0);
  const [approvable, setApprovable] = useState<TermRef[] | null>(null);
  const [picked, setPicked] = useState<number[]>([]);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    let live = true;
    fetchStewardshipSummary().then(
      (s) => live && setSummary(s),
      (e: unknown) => live && setError(e instanceof Error ? e.message : String(e)),
    );
    return () => {
      live = false;
    };
  }, [seq]);

  const openBulk = useCallback(async () => {
    setError("");
    try {
      const rows = await listApprovableTerms();
      setApprovable(rows);
      setPicked(rows.map((r) => r.term_id));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  const approve = async () => {
    setSaving(true);
    setError("");
    try {
      const { approved } = await bulkApproveTerms(picked);
      notifications.show({
        color: "green",
        message: t("glossaryTab.bulkApproveDone", { count: approved.length }),
      });
      setApprovable(null);
      setSeq((n) => n + 1);
      onApproved();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  if (summary === null) {
    return error ? (
      <Alert color="red">{error}</Alert>
    ) : (
      <Group justify="center" py="md">
        <Loader size="xs" />
      </Group>
    );
  }
  return (
    <Stack gap="md" pb="md" data-testid="glossary-stewardship">
      <Group justify="space-between" wrap="wrap">
        <Title order={4}>{t("glossaryTab.stewardshipTitle")}</Title>
        {canEdit && (
          <Button
            variant="light"
            leftSection={<CheckCheck size={14} />}
            onClick={() => void openBulk()}
            data-testid="glossary-bulk-approve-btn"
          >
            {t("glossaryTab.bulkApprove")}
          </Button>
        )}
      </Group>
      {error && (
        <Alert color="red" data-testid="glossary-stewardship-error">
          {error}
        </Alert>
      )}
      <SimpleGrid cols={{ base: 1, md: 3 }} spacing="lg">
        <TermList
          title={t("glossaryTab.agentVisibleTitle")}
          testId="stewardship-agent-visible"
          filterParam="agentVisibleSearch"
          rows={summary.agent_visible}
          texts={(term) => [t(`glossaryTab.status_${term.status}`)]}
          columns={[
            {
              column: {
                key: "status",
                label: t("glossaryTab.stewardshipColStatus"),
                sortValue: (term) => t(`glossaryTab.status_${term.status}`),
                groupValue: (term) => t(`glossaryTab.status_${term.status}`),
              },
              cell: (term) => <TermStatusBadge status={term.status} />,
            },
          ]}
        />
        <TermList
          title={t("glossaryTab.waitingTitle")}
          testId="stewardship-waiting"
          filterParam="waitingSearch"
          rows={summary.waiting_on_one_approval}
          texts={(term) => term.approve.map((a) => a.name)}
          columns={[
            {
              column: {
                key: "approve",
                label: t("glossaryTab.stewardshipColApprove"),
                sortValue: (term) => term.approve.map((a) => a.name).join(", "),
              },
              cell: (term) => (
                <Group gap={6} wrap="wrap">
                  {term.approve.map((a) => (
                    <TermLink
                      key={a.term_id}
                      term={a}
                      testId={`stewardship-waiting-approve-${term.term_id}-${a.term_id}`}
                    />
                  ))}
                </Group>
              ),
            },
          ]}
        />
        <TermList
          title={t("glossaryTab.approvedNotGroundedTitle")}
          testId="stewardship-approved-not-grounded"
          filterParam="noDataSearch"
          rows={summary.approved_not_grounded}
          texts={() => []}
          columns={[]}
        />
      </SimpleGrid>
      {approvable !== null && (
        <Modal
          opened
          onClose={() => setApprovable(null)}
          title={t("glossaryTab.bulkApproveTitle")}
          data-testid="glossary-bulk-approve"
        >
          <Stack gap="sm">
            {approvable.length === 0 ? (
              <Text size="sm" c="dimmed">
                {t("glossaryTab.bulkApproveNone")}
              </Text>
            ) : (
              <>
                <Text size="sm">{t("glossaryTab.bulkApproveBody")}</Text>
                <FormLayout>
                  <FormField size="full">
                    <Checkbox.Group
                      value={picked.map(String)}
                      onChange={(v) => setPicked(v.map(Number))}
                    >
                      <Stack gap={4} mah={320} style={{ overflow: "auto" }}>
                        {approvable.map((term) => (
                          <Checkbox
                            key={term.term_id}
                            value={String(term.term_id)}
                            label={term.name}
                            data-testid={`glossary-bulk-approve-${term.term_id}`}
                          />
                        ))}
                      </Stack>
                    </Checkbox.Group>
                  </FormField>
                </FormLayout>
              </>
            )}
            <FormFooter
              onCancel={() => setApprovable(null)}
              action={
                <Button
                  color="teal"
                  leftSection={<CheckCheck size={14} />}
                  disabled={picked.length === 0}
                  loading={saving}
                  onClick={() => void approve()}
                  data-testid="glossary-bulk-approve-confirm"
                >
                  {t("glossaryTab.bulkApproveConfirm", { count: picked.length })}
                </Button>
              }
            />
          </Stack>
        </Modal>
      )}
    </Stack>
  );
}
