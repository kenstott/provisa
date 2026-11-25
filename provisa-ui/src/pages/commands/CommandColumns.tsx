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
// cache setting: a command's output comes from calling its service.

import { Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { DatasetColumn } from "../../api/actions";
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
  columns,
  tab,
  onTabChange,
}: {
  columns: readonly DatasetColumn[];
  tab?: ColumnTab;
  onTabChange?: (tab: ColumnTab) => void;
}) {
  const { t } = useTranslation();
  return (
    <div data-testid="command-columns-read">
      <ColumnsTable
        columns={declared(columns).map(({ c }) => toForm(c))}
        roles={[]}
        irTypes={[]}
        sourceType="command"
        readOnly
        tabs={COMMAND_COLUMN_TABS}
        tabValue={tab}
        onTabChange={onTabChange}
        testId="command-columns-table"
        empty={t("commandColumns.none")}
      />
    </div>
  );
}
