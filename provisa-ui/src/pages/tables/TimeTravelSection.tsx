// Copyright (c) 2026 Kenneth Stott
// Canary: e1e4c48b-dc3b-496c-962f-f70c54b15576
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2109: the one Time Travel control of every form that creates or edits a table, a view or a
// model. Off, or Bitemporal: Bitemporal maps the business key, valid time and recorded time to
// the object's columns, and Provisa decides how the versions are kept (no mode is asked).

import { Select } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { RegisteredTable } from "../../types/admin";
import { FormField } from "../../components/form/FormLayout";
import { CollapsibleSection } from "./CollapsibleSection";
import { BitemporalFields, type HistoryDeclaration } from "./BitemporalFields";
import { useHistoryChoice, useHistoryLocked, type HistoryChoice } from "./timeTravel";

export function TimeTravelSection({
  choice,
  value,
  columns,
  onChange,
  locked,
  withBusinessKey,
}: {
  choice: HistoryChoice;
  value: HistoryDeclaration;
  columns: string[];
  onChange: (next: HistoryDeclaration) => void;
  /** Versions already exist: the switch, business key and valid time are not offered. */
  locked: boolean;
  /** False where the form names the business key itself (a model's key). */
  withBusinessKey: boolean;
}) {
  const { t } = useTranslation();
  return (
    <CollapsibleSection
      title={t("tableEditForm.timeTravelPanel")}
      testId="time-travel-section"
      info={{
        label: t("tableEditForm.timeTravelPanel"),
        text: t("tableEditForm.timeTravelSectionHelp"),
      }}
      flat
    >
      {/* Once versions exist the switch, the business key and the valid time can no longer
          change, and are not offered (REQ-2109). */}
      {!locked && (
        <FormField size="short">
          <Select
            label={t("tableEditForm.timeTravelLabel")}
            data-testid="time-travel"
            data={[
              { value: "off", label: t("tableEditForm.timeTravelOff") },
              { value: "bitemporal", label: t("tableEditForm.timeTravelBitemporal") },
            ]}
            value={choice.on ? "bitemporal" : "off"}
            onChange={(v) => choice.onToggle(v === "bitemporal")}
            allowDeselect={false}
            comboboxProps={{ withinPortal: true }}
          />
        </FormField>
      )}
      {choice.on && (
        <BitemporalFields
          value={value}
          columns={columns}
          onChange={onChange}
          locked={locked}
          withBusinessKey={withBusinessKey}
        />
      )}
    </CollapsibleSection>
  );
}

/** The Time Travel section of the table and view editor, on the staged table. */
export function TimeTravelEditor({
  editingTable,
  setEditingTable,
  saved,
  choice,
}: {
  editingTable: RegisteredTable;
  setEditingTable: (t: RegisteredTable) => void;
  /** The table as last saved; null while it is being created. */
  saved: RegisteredTable | null;
  choice: HistoryChoice;
}) {
  const locked = useHistoryLocked(saved);
  return (
    <TimeTravelSection
      choice={choice}
      value={editingTable}
      columns={editingTable.columns.map((c) => c.columnName)}
      onChange={(next) => setEditingTable({ ...editingTable, ...next })}
      locked={locked}
      withBusinessKey
    />
  );
}

/** A table's Time Travel section in the editor (a view's is among its materialization panels). */
export function TableTimeTravel({
  editingTable,
  setEditingTable,
  saved,
}: {
  editingTable: RegisteredTable;
  setEditingTable: (t: RegisteredTable) => void;
  /** The table as last saved; null while it is being created. */
  saved: RegisteredTable | null;
}) {
  const choice = useHistoryChoice(editingTable, (next) =>
    setEditingTable({ ...editingTable, ...next }),
  );
  return (
    <TimeTravelEditor
      editingTable={editingTable}
      setEditingTable={setEditingTable}
      saved={saved}
      choice={choice}
    />
  );
}
