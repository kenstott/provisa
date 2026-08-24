// Copyright (c) 2026 Kenneth Stott
// Canary: 4e6a1d20-3b57-49f8-9a1c-77e0d2c4b915
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { Checkbox, Group, MultiSelect, Paper, Select, Stack, TextInput } from "@mantine/core";
import { keyList } from "../../naming";
import type { RegisteredTable } from "../../types/admin";
import type { RelForm } from "./relationship-types";

interface JunctionPanelProps {
  form: RelForm;
  setForm: (f: RelForm) => void;
  tables: RegisteredTable[];
  testIdPrefix: string;
}

/**
 * REQ-1586: the junction declaration for a relationship.
 *
 * Declaring a junction is rare next to ordinary relationship registration, so the mapping does not
 * sit inline: one checkbox carries the declaration and the mapping fields appear only once it is
 * checked. The checkbox is the storage discriminator (via_table_id IS NOT NULL) made visible rather
 * than a disclosure control, so clearing it discards the mapping instead of leaving values behind a
 * closed panel; the surface a steward sees for an ordinary FK/PK edge is unchanged.
 */
export function JunctionPanel({ form, setForm, tables, testIdPrefix }: JunctionPanelProps) {
  const { t } = useTranslation();

  const tableOptions = tables.map((tbl) => ({ value: tbl.tableName, label: tbl.tableName }));
  const viaColumnOptions = (
    tables.find((tbl) => tbl.tableName === form.viaTable)?.columns ?? []
  ).map((c) => ({ value: c.columnName, label: c.columnName }));

  const checked = Boolean(form.viaTable) || form.junctionDeclared;

  return (
    <Stack gap="xs" data-testid={`${testIdPrefix}-junction-panel`}>
      <Checkbox
        label={t("addRelationshipForm.junctionTitle")}
        description={t("addRelationshipForm.junctionHelp")}
        checked={checked}
        onChange={(e) =>
          setForm({
            ...form,
            junctionDeclared: e.currentTarget.checked,
            // Clearing the declaration discards the mapping — a stored junction with no table is
            // not a shape the control plane accepts, so it must not survive an unchecked box.
            ...(e.currentTarget.checked
              ? {}
              : {
                  viaTable: "",
                  viaSourceColumn: "",
                  viaTargetColumn: "",
                  viaTypeColumn: "",
                  viaTypeValue: "",
                  viaLabelSource: "",
                }),
          })
        }
        data-testid={`${testIdPrefix}-junction-toggle`}
      />
      {checked && (
        <Paper withBorder p="sm" radius="sm">
      <Stack gap="sm">
        <Group align="flex-end" wrap="wrap">
          <Select
            label={t("addRelationshipForm.viaTableLabel")}
            placeholder={t("addRelationshipForm.selectPlaceholder")}
            data={tableOptions}
            clearable
            searchable
            value={form.viaTable || null}
            onChange={(v) =>
              setForm({
                ...form,
                viaTable: v ?? "",
                viaSourceColumn: "",
                viaTargetColumn: "",
                viaTypeColumn: "",
                viaTypeValue: "",
                viaLabelSource: v ? form.viaLabelSource : "",
              })
            }
            data-testid={`${testIdPrefix}-via-table`}
          />
          {/* REQ-1586: an end is an ordered column list, so a composite foreign key is mapped by
              picking its columns in order; the picks are paired positionally against the
              relationship's own keys. A single-column end is one pick. */}
          <MultiSelect
            label={t("addRelationshipForm.viaSourceColumnLabel")}
            placeholder={t("addRelationshipForm.selectPlaceholder")}
            data={viaColumnOptions}
            disabled={!form.viaTable}
            value={keyList(form.viaSourceColumn)}
            onChange={(v) => setForm({ ...form, viaSourceColumn: v.join(",") })}
            data-testid={`${testIdPrefix}-via-source-column`}
          />
          <MultiSelect
            label={t("addRelationshipForm.viaTargetColumnLabel")}
            placeholder={t("addRelationshipForm.selectPlaceholder")}
            data={viaColumnOptions}
            disabled={!form.viaTable}
            value={keyList(form.viaTargetColumn)}
            onChange={(v) => setForm({ ...form, viaTargetColumn: v.join(",") })}
            data-testid={`${testIdPrefix}-via-target-column`}
          />
        </Group>
        <Group align="flex-end" wrap="wrap">
          <Select
            label={t("addRelationshipForm.viaTypeColumnLabel")}
            placeholder={t("addRelationshipForm.selectPlaceholder")}
            data={viaColumnOptions}
            disabled={!form.viaTable}
            clearable
            value={form.viaTypeColumn || null}
            onChange={(v) => setForm({ ...form, viaTypeColumn: v ?? "", viaTypeValue: "" })}
            data-testid={`${testIdPrefix}-via-type-column`}
          />
          <TextInput
            label={t("addRelationshipForm.viaTypeValueLabel")}
            placeholder={t("addRelationshipForm.viaTypeValuePlaceholder")}
            disabled={!form.viaTypeColumn}
            value={form.viaTypeValue}
            onChange={(e) => setForm({ ...form, viaTypeValue: e.currentTarget.value })}
            data-testid={`${testIdPrefix}-via-type-value`}
          />
          <Select
            label={t("addRelationshipForm.viaLabelSourceLabel")}
            placeholder={t("addRelationshipForm.selectPlaceholder")}
            data={[
              { value: "column", label: t("addRelationshipForm.viaLabelSourceColumn") },
              { value: "table", label: t("addRelationshipForm.viaLabelSourceTable") },
              { value: "fixed", label: t("addRelationshipForm.viaLabelSourceFixed") },
            ]}
            disabled={!form.viaTable}
            value={form.viaLabelSource || null}
            onChange={(v) => setForm({ ...form, viaLabelSource: v ?? "" })}
            data-testid={`${testIdPrefix}-via-label-source`}
          />
        </Group>
      </Stack>
        </Paper>
      )}
    </Stack>
  );
}
