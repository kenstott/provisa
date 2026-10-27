// Copyright (c) 2026 Kenneth Stott
// Canary: 4a8c1f59-6e3d-4b2a-9f7c-0d5e8b1a3c72
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert } from "@mantine/core";
import type {
  DataProduct,
  MutationResult,
  RegisteredTable,
  Relationship,
  Source,
  WriteGrant,
} from "../../types/admin";
import type { Role } from "../../types/auth";
import type { PlatformSettings } from "../../api/admin";
import { useLivePolicyPreview } from "./useLivePolicyPreview";
import { roleTtlRowError, tableTtlSignalError } from "./roleTtl";
import { FormFooter } from "../../components/form/FormLayout";
import { ParametersPanel } from "./ParametersPanel";
import { usePanelDirty } from "../../components/list/panelLeave";
import { TableSectionTabs } from "./TableSectionTabs";
import { WriteGrantsPanel } from "./WriteGrantsPanel";
import { useSetTableWriteGrants } from "../../hooks/useWriteGrants";
import { useTableSection, type TableSection } from "./tableSections";
import { TableAssociations } from "./TableAssociations";
import { IdentityRead, LoadRead, QualityRead, QueryRead } from "./TableSectionsRead";
import { dataProductField, isCheckerSource } from "./tableSectionFields";
import { TableIdentitySection } from "./TableIdentitySection";
import { TableAssociationsSection } from "./TableAssociationsSection";
import { TableLoadSection } from "./TableLoadSection";
import { TableQuerySection } from "./TableQuerySection";
import { TableQualitySection } from "./TableQualitySection";
import { TableColumnsSection } from "./TableColumnsSection";
import type { CacheTtlEdit, UpdateEditCol } from "./tableEditTypes";

interface TableEditFormProps {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
  /**
   * The table as last saved: REQ-2109 judges on it whether its versions exist. Null while the
   * table is being created: the same sections are its create form, and nothing of it is saved yet.
   */
  savedTable: RegisteredTable | null;
  /** REQ-1934: the table's profiler membership as last saved. */
  savedProfilerId: string | null;
  editingColumnTypes: Record<string, string>;
  cacheTtlEdits: Record<number, CacheTtlEdit>;
  setCacheTtlEdits: React.Dispatch<React.SetStateAction<Record<number, CacheTtlEdit>>>;
  sources: Source[];
  roles: Role[];
  dataProducts: DataProduct[]; // REQ-1634
  settings: PlatformSettings | null;
  saving: boolean;
  generateTableDescription: (id: number) => Promise<string>;
  generateColumnDescription: (id: number, colName: string) => Promise<string>;
  cancelEditing: () => void;
  handleSaveEdit: () => void;
  /** What the page has to say of a save that did not go through; shown with Save and Cancel. */
  problem?: string | null;
  /** Whether anything of the table has been changed since the form opened. */
  dirty?: boolean;
  updateEditCol: UpdateEditCol;
  /**
   * REQ-1944: the editor holds a hiding right but not table_registration -- only how the columns
   * are hidden (grants, masks, fakes, synthetic rules) is editable; the server refuses the rest.
   */
  hidingOnly?: boolean;
  /** The relationships the table takes part in are listed on Associations. */
  relationships: Relationship[];
  /** The section in view, kept by the page across the read view and the edit form. */
  section?: TableSection;
  onSectionChange?: (section: TableSection) => void;
}

export function TableEditForm({
  editingTable,
  setEditingTable,
  savedTable,
  savedProfilerId,
  editingColumnTypes,
  cacheTtlEdits,
  setCacheTtlEdits,
  sources,
  roles,
  dataProducts,
  settings,
  saving,
  generateTableDescription,
  generateColumnDescription,
  cancelEditing,
  handleSaveEdit,
  problem,
  dirty = false,
  updateEditCol,
  hidingOnly = false,
  relationships,
  section: sectionIn,
  onSectionChange,
}: TableEditFormProps) {
  const { t } = useTranslation();
  const [section, setSection] = useTableSection(sectionIn, onSectionChange);
  const roleOptions = roles.map((r) => ({ id: r.id, label: r.id }));
  const { setTableWriteGrants } = useSetTableWriteGrants();
  const creating = savedTable === null;

  // REQ-1143: live-previewed refresh-policy summary; logic extracted to useLivePolicyPreview.
  const stagedTtl = cacheTtlEdits[editingTable.id]?.value;
  const shownPolicy = useLivePolicyPreview(editingTable, stagedTtl);

  const editSource = sources.find((s) => s.id === editingTable.sourceId);
  // Leaving the panel with something changed asks first (the panel's one mechanism).
  // Named as it was when the form opened: its alias may be what is being changed.
  const [openedAs] = useState(() => editingTable.alias || editingTable.tableName);
  usePanelDirty(dirty, openedAs);
  // REQ-1443: a checker source's table is a results table; the source's type is the one authority.
  const isCheckerTable = isCheckerSource(editSource);
  const effLoadProtected =
    editingTable.loadProtected == null
      ? (editSource?.loadProtected ?? false)
      : editingTable.loadProtected;

  // A __derived__ virtual view has no external source, so the source-freshness controls (cache TTL,
  // replicate, load protection, off-peak) don't apply — the view/MV rebuild path reads only
  // materialize + mv_refresh_interval + change_signal. Materialization is driven by the "Materialized
  // View" checkbox, not replicate. Hide those fields for a view to avoid contradictory knobs. A
  // metric view being created has no SQL yet: the server compiles it from its metrics.
  const isView = editingTable.viewSql != null || editingTable.viewMetrics != null;

  // REQ-1907/REQ-930: the table's resolved landing cache_ttl — the staged Cache TTL edit, else the
  // saved table value, else the source's. The global default is the response-cache TTL, not a
  // landing clock, so it is not part of this chain. null = neither sets one.
  const tableCacheTtl =
    stagedTtl === undefined ? editingTable.cacheTtl : stagedTtl === "" ? null : Number(stagedTtl);
  const floorTtl = tableCacheTtl ?? editSource?.cacheTtl ?? null;
  // A ttl/ttl_probe signal (the table's own, else inherited from its source) with no resolved
  // landing TTL has no staleness clock; the save refuses it (tableTtlSignalError).
  const ttlSignalError = !isView && tableTtlSignalError(editingTable, editSource, floorTtl);
  // What Save will refuse on Load and timeliness that the form itself can tell: a ttl signal with
  // no Cache TTL, and a role TTL row that is not valid. Shown on the section's tab.
  const loadErrors =
    (ttlSignalError ? 1 : 0) +
    (isView
      ? 0
      : editingTable.roleTtl.filter((_, i) => roleTtlRowError(editingTable.roleTtl, i) !== null)
          .length);

  // While the table is being created its write grants are kept with it and saved once it exists.
  const saveWriteGrants = async (grants: WriteGrant[]): Promise<MutationResult> => {
    if (!creating) return setTableWriteGrants(editingTable.id, grants);
    setEditingTable({ ...editingTable, writeGrants: grants });
    return { success: true, message: "" };
  };

  return (
    <>
      {hidingOnly && (
        <Alert color="blue" mb="0.75rem" data-testid="table-edit-hiding-only">
          {t("tableEditForm.hidingOnlyNote")}
        </Alert>
      )}
      {/* The same seven sections as the read view, when the table is created and when it is
          edited. To a caller who may change only how columns are hidden the first five show
          read-only; Columns is theirs to edit. */}
      <TableSectionTabs value={section} onChange={setSection} errors={{ load: loadErrors }} />
      {section === "identity" &&
        (hidingOnly ? (
          <IdentityRead table={editingTable} source={editSource} />
        ) : (
          <TableIdentitySection
            editingTable={editingTable}
            setEditingTable={setEditingTable}
            isView={isView}
            describe={creating ? undefined : () => generateTableDescription(editingTable.id)}
          />
        ))}
      {section === "associations" &&
        (hidingOnly ? (
          <TableAssociations
            table={editingTable}
            relationships={relationships}
            leading={[dataProductField(t, editingTable, dataProducts)]}
          />
        ) : (
          <TableAssociationsSection
            editingTable={editingTable}
            setEditingTable={setEditingTable}
            dataProducts={dataProducts}
            relationships={relationships}
          />
        ))}
      {section === "load" &&
        (hidingOnly ? (
          <LoadRead table={editingTable} source={editSource} />
        ) : (
          <TableLoadSection
            editingTable={editingTable}
            setEditingTable={setEditingTable}
            saved={savedTable}
            isView={isView}
            shownPolicy={shownPolicy}
            sources={sources}
            editSource={editSource}
            roles={roles}
            settings={settings}
            editingColumnTypes={editingColumnTypes}
            cacheTtlEdits={cacheTtlEdits}
            setCacheTtlEdits={setCacheTtlEdits}
            floorTtl={floorTtl}
            ttlSignalError={Boolean(ttlSignalError)}
            effLoadProtected={Boolean(effLoadProtected)}
          />
        ))}
      {section === "query" &&
        (hidingOnly ? (
          <>
            <QueryRead table={editingTable} />
            <ParametersPanel columns={editingTable.columns} />
          </>
        ) : (
          <TableQuerySection
            editingTable={editingTable}
            setEditingTable={setEditingTable}
            updateEditCol={updateEditCol}
          />
        ))}
      {section === "quality" &&
        (hidingOnly ? (
          <QualityRead table={editingTable} source={editSource} />
        ) : (
          <TableQualitySection
            editingTable={editingTable}
            setEditingTable={setEditingTable}
            editSource={editSource}
            isCheckerTable={isCheckerTable}
            savedProfilerId={savedProfilerId}
            tableId={creating ? undefined : editingTable.id}
          />
        ))}
      {section === "columns" && (
        <TableColumnsSection
          editingTable={editingTable}
          updateEditCol={updateEditCol}
          editSource={editSource}
          roles={roles}
          hidingOnly={hidingOnly}
          describeColumn={
            creating ? undefined : (column) => generateColumnDescription(editingTable.id, column)
          }
        />
      )}
      {section === "writes" && (
        // REQ-2074: the grants are saved on their own, apart from the table's other settings.
        <WriteGrantsPanel
          grants={editingTable.writeGrants}
          columns={editingTable.columns.map((c) => c.columnName)}
          roles={roleOptions}
          onSave={hidingOnly ? undefined : saveWriteGrants}
          kept={creating}
        />
      )}
      {/* In the detail panel these are its head's actions while the table is edited, in place of
          the table's own; under a list row they are the form's last row. */}
      <FormFooter
        panel
        problem={problem}
        // Starts where the form's fields start: the card's own inset.
        style={{ padding: "0.75rem 1.5rem" }}
        onSave={handleSaveEdit}
        saving={saving}
        onCancel={cancelEditing}
        saveProps={{ "data-testid": "table-edit-save" }}
        cancelProps={{ "data-testid": "table-edit-cancel" }}
      />
    </>
  );
}
