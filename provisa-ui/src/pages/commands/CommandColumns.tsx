// Copyright (c) 2026 Kenneth Stott
// Canary: 7e3a9c52-4b18-4d6f-8a07-c15d9e2f6b34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-25): a command's output dataset columns on a Columns tab of their own,
// carrying a table column's governance: who sees each, how it is masked, its SQL alias and its
// description. They are edited and read with the table columns editor. No fake data, physical name or
// cache setting: a command's output comes from calling its service. Each column's tags, glossary
// term and classification are shown read-only, linked to the Tags and Glossary pages that edit them.

import { useEffect, useState } from "react";
import { Alert, Group, Stack, Table, Text, Title } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { DatasetColumn } from "../../api/actions";
import { fetchCommandColumnTerms } from "../../api/glossary";
import {
  fetchCommandClassifications,
  type CommandClassificationDecision,
} from "../../api/profiler";
import { TagControl } from "../../components/TagControl";
import { EntityLink, EntityPageLink } from "../../components/EntityLink";
import type { MaskEntry } from "../../lib/masks";
import { ColumnsTable } from "../tables/ColumnsTable";
import { blankColumn, type ColumnForm } from "../tables/types";
import type { ColumnTab } from "../tables/columnTopics";

/** The topics a command's output column has: its naming and its access. Its name and type are the
 * contract, declared on the Command tab. */
const COMMAND_COLUMN_TABS: ColumnTab[] = ["naming", "access"];

const toForm = (c: DatasetColumn): ColumnForm => ({
  ...blankColumn(c.name, c.type, c.visible_to ?? []),
  masks: c.masks ?? [],
  alias: c.alias ?? "",
  description: c.description ?? "",
});

/** The columns that are declared: a row still without a name is not a column yet. */
const declared = (columns: readonly DatasetColumn[]) =>
  columns.map((c, index) => ({ c, index })).filter(({ c }) => c.name.trim() !== "");

export function CommandColumnsFields({
  columns,
  onChange,
  roleOptions,
  tab,
  onTabChange,
}: {
  columns: DatasetColumn[];
  onChange: (columns: DatasetColumn[]) => void;
  roleOptions: { id: string; label: string }[];
  tab?: ColumnTab;
  onTabChange?: (tab: ColumnTab) => void;
}) {
  const { t } = useTranslation();
  const shown = declared(columns);
  if (shown.length === 0) {
    return (
      <Text size="sm" c="dimmed" data-testid="command-columns-empty">
        {t("commandColumns.none")}
      </Text>
    );
  }
  const update = (
    i: number,
    key: keyof ColumnForm,
    value: string | boolean | string[] | MaskEntry[],
  ) => {
    const at = shown[i].index;
    const next = [...columns];
    const col = { ...next[at] };
    if (key === "visibleTo") {
      const roles = value as string[];
      // No grant list is the command's public shape (REQ-1679), so none is stored as null.
      col.visible_to = roles.length > 0 ? roles : null;
    } else if (key === "masks") col.masks = value as MaskEntry[];
    else if (key === "alias") col.alias = (value as string) || null;
    else if (key === "description") col.description = (value as string) || null;
    else throw new Error(`a command's output column has no editable ${String(key)}`);
    next[at] = col;
    onChange(next);
  };
  return (
    <div data-testid="command-columns-fields">
      <Text size="sm" c="dimmed" mb="xs">
        {t("commandColumns.hint")}
      </Text>
      <ColumnsTable
        columns={shown.map(({ c }) => toForm(c))}
        updateCol={update}
        roles={roleOptions}
        irTypes={[]}
        sourceType="command"
        tabs={COMMAND_COLUMN_TABS}
        selectable={false}
        tabValue={tab}
        onTabChange={onTabChange}
      />
    </div>
  );
}

export function CommandColumnsRead({
  name,
  columns,
  tab,
  onTabChange,
}: {
  /** The command: its columns' tags, terms and classifications are read by its name. */
  name: string;
  columns: readonly DatasetColumn[];
  tab?: ColumnTab;
  onTabChange?: (tab: ColumnTab) => void;
}) {
  const { t } = useTranslation();
  const shown = declared(columns).map(({ c }) => c.name);
  return (
    <div data-testid="command-columns-read">
      <ColumnsTable
        columns={declared(columns).map(({ c }) => toForm(c))}
        roles={[]}
        irTypes={[]}
        sourceType="command"
        readOnly
        physicalNames
        tabs={COMMAND_COLUMN_TABS}
        tabValue={tab}
        onTabChange={onTabChange}
        testId="command-columns-table"
        empty={t("commandColumns.none")}
      />
      {shown.length > 0 && <CommandColumnGovernance name={name} columns={shown} />}
    </div>
  );
}

/** A read that may be refused: its value, or the refusal said in its place. */
type Read<T> = { value: T } | { refused: string } | null;

function useRead<T>(key: string, load: (key: string) => Promise<T>): Read<T> {
  const [read, setRead] = useState<Read<T>>(null);
  useEffect(() => {
    let live = true;
    load(key).then(
      (value) => live && setRead({ value }),
      (e: Error) => live && setRead({ refused: e.message }),
    );
    return () => {
      live = false;
    };
  }, [key, load]);
  return read;
}

const decisionsOf = (command: string) =>
  fetchCommandClassifications(command).then((r) => r.decisions);

/** REQ-1159 (amended 2026-11-25): each output column's tags, glossary term and classification,
 * read-only; the Tags and Glossary pages edit them. */
function CommandColumnGovernance({ name, columns }: { name: string; columns: string[] }) {
  const { t } = useTranslation();
  const terms = useRead(name, fetchCommandColumnTerms);
  const decisions = useRead(name, decisionsOf);
  const accepted = (column: string): CommandClassificationDecision[] =>
    decisions !== null && "value" in decisions
      ? decisions.value.filter((d) => d.physical_column === column && d.status === "accepted")
      : [];
  return (
    <Stack gap="xs" mt="md" data-testid="command-columns-governance">
      <Group justify="space-between">
        <Title order={5}>{t("commandColumns.governanceTitle")}</Title>
        <Group gap="md">
          <EntityPageLink href="/admin/tags" testId="command-columns-tags-link">
            {t("commandColumns.editTags")}
          </EntityPageLink>
          <EntityPageLink href="/context/ontology" testId="command-columns-glossary-link">
            {t("commandColumns.editTerms")}
          </EntityPageLink>
        </Group>
      </Group>
      {terms !== null && "refused" in terms && (
        <Alert color="gray" data-testid="command-columns-terms-refused">
          {terms.refused}
        </Alert>
      )}
      {decisions !== null && "refused" in decisions && (
        <Alert color="gray" data-testid="command-columns-classifications-refused">
          {decisions.refused}
        </Alert>
      )}
      <Table.ScrollContainer minWidth={640}>
        <Table striped withTableBorder verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("commandColumns.colColumn")}</Table.Th>
              <Table.Th>{t("commandColumns.colTags")}</Table.Th>
              <Table.Th>{t("commandColumns.colTerm")}</Table.Th>
              <Table.Th>{t("commandColumns.colClassification")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {columns.map((column) => {
              const term = terms !== null && "value" in terms ? terms.value[column] : undefined;
              return (
                <Table.Tr key={column} data-testid={`command-column-governance-${column}`}>
                  <Table.Td ff="monospace" fz="sm">
                    {column}
                  </Table.Td>
                  <Table.Td>
                    <TagControl
                      objectType="command_column"
                      commandName={name}
                      columnName={column}
                      readOnly
                    />
                  </Table.Td>
                  <Table.Td data-testid={`command-column-term-${column}`}>
                    {term !== undefined && (
                      <EntityLink kind="term" id={term.id}>
                        {term.name}
                      </EntityLink>
                    )}
                  </Table.Td>
                  <Table.Td data-testid={`command-column-classification-${column}`}>
                    {accepted(column).map((d) => (
                      <EntityLink key={d.kind} kind="classification" id={d.kind}>
                        {d.kind}
                      </EntityLink>
                    ))}
                  </Table.Td>
                </Table.Tr>
              );
            })}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </Stack>
  );
}
