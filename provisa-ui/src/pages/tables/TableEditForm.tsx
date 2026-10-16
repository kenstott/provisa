// Copyright (c) 2026 Kenneth Stott
// Canary: 4a8c1f59-6e3d-4b2a-9f7c-0d5e8b1a3c72
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { NEVER_EXPIRES } from "../../components/form/cacheTtl";
import { Tip } from "../../components/Tip";
import { TableDraftTick, TableRegionField } from "./TablePlacementFields";
import { Fragment, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Box, Checkbox, Select, Table, Text, TextInput } from "@mantine/core";
import { GrantPicker } from "../../components/GrantPicker";
import { ColumnPresetsEditor } from "../../components/admin/ColumnPresetsEditor";
import { UniquesPanel } from "../../components/admin/UniquesPanel";
import type {
  RegisteredTable,
  Source,
  DataProduct,
  Relationship,
  TableColumn,
} from "../../types/admin";
import type { Role } from "../../types/auth";
import type { PlatformSettings } from "../../api/admin";
import { sourceProbeTypes } from "../../liveCapability";
import { IR_TYPES_FALLBACK, toIrType } from "../../irTypes";
import { NumericSizeInputs } from "./NumericSizeInputs";
import { fetchIrTypes } from "../../api/admin";
import { NAMING_CONVENTIONS } from "./constants";
import { TestDataColumns } from "./testdata/TestDataColumns";
import { maskOptions } from "./maskOptions";
import { DescriptionField } from "./DescriptionField";
import { ColumnNameCell } from "./ColumnNameCell";
import { FieldLabel } from "./FieldLabel";
import { MaterializedViewPanels } from "./MaterializedViewPanels";
import { LiveDeliveryFieldset } from "./LiveDeliveryFieldset";
import { DeltaFieldset } from "./DeltaFieldset";
import { LandingScopeFields } from "./LandingScopeFields";
import { DataQualityPanel } from "./DataQualityPanel";
import { ProfilerPanel } from "./ProfilerPanel";
import { useLivePolicyPreview } from "./useLivePolicyPreview";
import { roleTtlRowError, tableTtlSignalError } from "./roleTtl";
import { governanceWithheld, shownGrants, shownGrantsLabel } from "./helpers";
import {
  FormChecks,
  FormField,
  FormFooter,
  FormIdentifiers,
  FormLayout,
} from "../../components/form/FormLayout";
import { TableLoadManagementFields } from "./TableLoadManagementFields";
import { ColumnTabs } from "./ColumnTabs";
import { ParametersPanel } from "./ParametersPanel";
import { COLUMN_TABS, useColumnTab, type ColumnTab } from "./columnTopics";
import { usePanelDirty } from "../../components/list/panelLeave";
import type { ColumnDefinitionLine } from "./ColumnGlossaryHover";
import { TableSectionTabs } from "./TableSectionTabs";
import { useTableSection, type TableSection } from "./tableSections";
import { TableAssociations } from "./TableAssociations";
import { VectorModelCell } from "./VectorModelCell";
import { useVectorModels } from "./useVectorModels";
import {
  IdentityRead,
  LoadRead,
  QualityRead,
  QueryRead,
  RefreshPolicyAlert,
} from "./TableSectionsRead";
import { changeSignalChoices, dataProductField, isCheckerSource } from "./tableSectionFields";

export interface CacheTtlEdit {
  value: string;
  dirty: boolean;
  saving: boolean;
}

interface TableEditFormProps {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
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
  generatingDesc: boolean;
  setGeneratingDesc: React.Dispatch<React.SetStateAction<boolean>>;
  generatingColDesc: string | null;
  setGeneratingColDesc: React.Dispatch<React.SetStateAction<string | null>>;
  generateTableDescription: (id: number) => Promise<string>;
  generateColumnDescription: (id: number, colName: string) => Promise<string>;
  cancelEditing: () => void;
  handleSaveEdit: () => void;
  /** What the page has to say of a save that did not go through; shown with Save and Cancel. */
  problem?: string | null;
  /** Whether anything of the table has been changed since the form opened. */
  dirty?: boolean;
  updateEditCol: (i: number, key: string, value: string | string[] | boolean) => void;
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
  savedProfilerId,
  editingColumnTypes,
  cacheTtlEdits,
  setCacheTtlEdits,
  sources,
  roles,
  dataProducts,
  settings,
  saving,
  generatingDesc,
  setGeneratingDesc,
  generatingColDesc,
  setGeneratingColDesc,
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

  // REQ-846: canonical IR type vocabulary for the per-column Data Type override dropdown, fetched
  // from the backend (authoritative list in provisa/core/ir_types.py); fallback if the fetch fails.
  const [irTypes, setIrTypes] = useState<string[]>(IR_TYPES_FALLBACK);
  useEffect(() => {
    fetchIrTypes()
      .then((types) => {
        if (types.length > 0) setIrTypes(types);
      })
      .catch(() => setIrTypes(IR_TYPES_FALLBACK));
  }, []);

  // REQ-1143: live-previewed refresh-policy summary; logic extracted to useLivePolicyPreview.
  const governanceHidden = governanceWithheld(editingTable);
  const stagedTtl = cacheTtlEdits[editingTable.id]?.value;
  const shownPolicy = useLivePolicyPreview(editingTable, stagedTtl);

  // REQ-1141: the off-peak window/zone only gate the load-protected scheduled snapshot. When load
  // protection resolves off (table override, else source default) they have no effect, so they are
  // hidden — surfaced only once the table is actually load-protected.
  const editSource = sources.find((s) => s.id === editingTable.sourceId);
  // REQ-2000: a Qdrant collection's vectors, with the registered models each may name.
  const vectorModels = useVectorModels(
    editingTable.sourceId,
    editingTable.tableName,
    editSource?.type === "qdrant" && !editingTable.queryTemplate,
  );
  // Leaving the panel with something changed asks first (the panel's one mechanism).
  // Named as it was when the form opened: its alias may be what is being changed.
  const [openedAs] = useState(() => editingTable.alias || editingTable.tableName);
  usePanelDirty(dirty, openedAs);
  // The column editor shows a topic at a time. A caller not shown grants is not offered Access;
  // one who may change only how columns are hidden is not offered Testing (their Save writes
  // nothing of a column's test data).
  const shownTabs: ColumnTab[] = COLUMN_TABS.filter(
    (topic) => !(topic === "access" && governanceHidden) && !(topic === "testing" && hidingOnly),
  );
  const [tab, setTab] = useColumnTab(shownTabs);
  // A column's full definition for the hover on its name: every attribute, whichever tab edits it.
  const definitionOf = (c: TableColumn): ColumnDefinitionLine[] => [
    { label: t("tableEditForm.columnHeader"), value: c.columnName },
    { label: t("tableEditForm.sqlAliasHeader"), value: c.alias || c.computedSqlAlias || "" },
    { label: t("tableEditForm.descriptionHeader"), value: c.description || "" },
    ...(editSource?.type === "ingest"
      ? [{ label: t("tableEditForm.jsonPathHeader"), value: c.path || "" }]
      : []),
    { label: t("tableEditForm.dataTypeHeader"), value: c.dataType ? toIrType(c.dataType) : "" },
    { label: t("tableEditForm.pkHeader"), value: c.isPrimaryKey ? "✓" : "" },
    { label: t("tableEditForm.scopeHeader"), value: c.scope || "domain" },
    ...(governanceHidden
      ? []
      : [
          { label: t("tableEditForm.visibleToHeader"), value: shownGrantsLabel(c.visibleTo, t) },
          {
            label: t("tableEditForm.writableByHeader"),
            value: shownGrantsLabel(c.writableBy, t),
          },
          { label: t("tableEditForm.maskingHeader"), value: c.maskType || "" },
        ]),
  ];
  // REQ-1443: a checker source's table is a results table; the source's type is the one authority.
  const isCheckerTable = isCheckerSource(editSource);
  const effLoadProtected =
    editingTable.loadProtected == null
      ? (editSource?.loadProtected ?? false)
      : editingTable.loadProtected;

  // A __derived__ virtual view has no external source, so the source-freshness controls (cache TTL,
  // replicate, load protection, off-peak) don't apply — the view/MV rebuild path reads only
  // materialize + mv_refresh_interval + change_signal. Materialization is driven by the "Materialized
  // View" checkbox, not replicate. Hide those fields for a view to avoid contradictory knobs.
  const isView = editingTable.viewSql != null;

  // REQ-1907/REQ-930: the table's resolved landing cache_ttl — the staged Cache TTL edit, else the
  // saved table value, else the source's. The global default is the response-cache TTL, not a
  // landing clock, so it is not part of this chain. null = neither sets one.
  const stagedCacheTtl = cacheTtlEdits[editingTable.id]?.value;
  const tableCacheTtl =
    stagedCacheTtl === undefined
      ? editingTable.cacheTtl
      : stagedCacheTtl === ""
        ? null
        : Number(stagedCacheTtl);
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

  return (
    <>
      {hidingOnly && (
        <Alert color="blue" mb="0.75rem" data-testid="table-edit-hiding-only">
          {t("tableEditForm.hidingOnlyNote")}
        </Alert>
      )}
      {/* The same six sections as the read view. To a caller who may change only how columns are
          hidden the first five show read-only; Columns is theirs to edit. */}
      <TableSectionTabs value={section} onChange={setSection} errors={{ load: loadErrors }} />
      {section === "identity" &&
        (hidingOnly ? (
          <IdentityRead table={editingTable} source={editSource} />
        ) : (
          <FormLayout className="form-card" data-testid="table-section-identity-panel">
            <FormIdentifiers>
              <FormField size="medium">
                <TextInput
                  label={t("tableSections.source")}
                  value={editingTable.sourceId}
                  readOnly
                  styles={{ input: { color: "var(--text-muted)", cursor: "default" } }}
                  data-testid="table-edit-source"
                />
              </FormField>
              <FormField size="medium">
                <TextInput
                  label={t("tableSections.schema")}
                  value={editingTable.schemaName}
                  readOnly
                  styles={{ input: { color: "var(--text-muted)", cursor: "default" } }}
                  data-testid="table-edit-schema"
                />
              </FormField>
              <FormField size="medium">
                <TextInput
                  label={t("tableSections.table")}
                  value={editingTable.tableName}
                  readOnly
                  styles={{ input: { color: "var(--text-muted)", cursor: "default" } }}
                  data-testid="table-edit-table"
                />
              </FormField>
              <FormField size="medium">
                <TextInput
                  label={t("tableSections.domain")}
                  value={editingTable.domainId}
                  readOnly
                  styles={{ input: { color: "var(--text-muted)", cursor: "default" } }}
                  data-testid="table-edit-domain"
                />
              </FormField>
              <FormField help={t("tableEditForm.sqlAliasHelp")} size="medium">
                <TextInput
                  label={t("tableEditForm.sqlAliasLabel")}
                  value={editingTable.alias || ""}
                  onChange={(e) =>
                    setEditingTable({
                      ...editingTable,
                      alias: e.target.value || null,
                    })
                  }
                  placeholder={t("tableEditForm.sqlAliasPlaceholder")}
                />
              </FormField>
            </FormIdentifiers>
            <FormField help={t("tableEditForm.namingConventionHelp")} size="medium">
              <Select
                label={t("tableEditForm.namingConventionLabel")}
                data={NAMING_CONVENTIONS.map((nc) => ({
                  value: nc.value,
                  label: nc.label,
                }))}
                value={editingTable.gqlNamingConvention ?? ""}
                onChange={(v) =>
                  setEditingTable({
                    ...editingTable,
                    gqlNamingConvention: v || null,
                  })
                }
                comboboxProps={{ withinPortal: true }}
                allowDeselect={false}
              />
            </FormField>
            <TableRegionField table={editingTable} isView={isView} onChange={setEditingTable} />

            {editingTable.apiEndpoint && (
              <FormField size="long">
                <TextInput
                  label={t("tableEditForm.apiEndpointLabel")}
                  readOnly
                  value={editingTable.apiEndpoint}
                  styles={{ input: { color: "var(--text-muted)", cursor: "default" } }}
                />
              </FormField>
            )}
            <FormChecks>
              <TableDraftTick table={editingTable} onChange={setEditingTable} />
            </FormChecks>
            <FormField size="full">
              <div>
                <FieldLabel
                  text={t("tableEditForm.descriptionLabel")}
                  help={t("tableEditForm.descriptionHelp")}
                />
                <DescriptionField
                  value={editingTable.description || ""}
                  onChange={(v) => setEditingTable({ ...editingTable, description: v || null })}
                  placeholder={t("tableEditForm.descriptionPlaceholder")}
                  rows={2}
                  generating={generatingDesc}
                  onGenerate={async () => {
                    setGeneratingDesc(true);
                    try {
                      const desc = await generateTableDescription(editingTable.id);
                      if (desc) setEditingTable({ ...editingTable, description: desc });
                    } finally {
                      setGeneratingDesc(false);
                    }
                  }}
                />
              </div>
            </FormField>
          </FormLayout>
        ))}
      {section === "associations" &&
        (hidingOnly ? (
          <TableAssociations
            table={editingTable}
            relationships={relationships}
            leading={[dataProductField(t, editingTable, dataProducts)]}
          />
        ) : (
          <>
            <FormLayout className="form-card" data-testid="table-section-associations-panel">
              {isCheckerTable ? (
                // REQ-1443 clause 10: a checker table's membership is inherited from the table its
                // contract scans — the server derives it, so it is shown, never chosen.
                <FormField size="medium" hint={t("tableEditForm.dataProductInherited")}>
                  <TextInput
                    label={t("tableEditForm.dataProductLabel")}
                    value={
                      editingTable.productId == null
                        ? t("tableEditForm.dataProductNone")
                        : (dataProducts.find((p) => p.id === editingTable.productId)?.name ??
                          editingTable.productId)
                    }
                    readOnly
                    disabled
                    data-testid="table-edit-product-id"
                  />
                </FormField>
              ) : (
                <FormField size="medium" hint={t("tableEditForm.dataProductDesc")}>
                  <Select
                    label={t("tableEditForm.dataProductLabel")}
                    placeholder={t("tableEditForm.dataProductNone")}
                    clearable
                    // REQ-1634: a table may only join a data product owned by its own domain.
                    data={dataProducts
                      .filter((p) => p.domainId === editingTable.domainId)
                      .map((p) => ({ value: p.id, label: p.name }))}
                    value={editingTable.productId}
                    onChange={(value) =>
                      setEditingTable({
                        ...editingTable,
                        productId: value,
                      })
                    }
                    data-testid="table-edit-product-id"
                  />
                </FormField>
              )}
            </FormLayout>
            <Box mt="md">
              <TableAssociations table={editingTable} relationships={relationships} />
            </Box>
          </>
        ))}
      {section === "load" &&
        (hidingOnly ? (
          <LoadRead table={editingTable} source={editSource} />
        ) : (
          <>
            {shownPolicy && <RefreshPolicyAlert policy={shownPolicy} />}
            <FormLayout className="form-card" data-testid="table-section-load-panel">
              <FormField help={t("tableEditForm.changeSignalHelp")} size="medium">
                <Select
                  label={t("tableEditForm.changeSignalLabel")}
                  data={changeSignalChoices(t)}
                  value={editingTable.changeSignal ?? ""}
                  onChange={(v) =>
                    setEditingTable({
                      ...editingTable,
                      changeSignal: v || null,
                    })
                  }
                  comboboxProps={{ withinPortal: true }}
                  allowDeselect={false}
                />
              </FormField>
              {(editingTable.changeSignal === "probe" ||
                editingTable.changeSignal === "ttl_probe") &&
                (() => {
                  const src = sources.find((s) => s.id === editingTable.sourceId);
                  const caps = sourceProbeTypes(src?.type);
                  if (caps.length === 0) return null;
                  return (
                    <FormField help={t("tableEditForm.probeTypeHelp")} size="medium">
                      <Select
                        label={t("tableEditForm.probeTypeLabel")}
                        data={[
                          { value: "", label: t("tableEditForm.probeTypeAuto") },
                          ...caps.map((pt) => ({
                            value: pt,
                            label:
                              pt +
                              (pt === "watermark"
                                ? t("tableEditForm.probeAppendSuffix")
                                : t("tableEditForm.probeReplaceSuffix")),
                          })),
                        ]}
                        value={editingTable.probeType ?? ""}
                        onChange={(v) =>
                          setEditingTable({
                            ...editingTable,
                            probeType: v || null,
                          })
                        }
                        comboboxProps={{ withinPortal: true }}
                        allowDeselect={false}
                      />
                    </FormField>
                  );
                })()}
              {(editingTable.changeSignal === "probe" ||
                editingTable.changeSignal === "ttl_probe") && (
                <FormField help={t("tableEditForm.freshnessProbeHelp")} size="medium">
                  <TextInput
                    label={t("tableEditForm.freshnessProbeLabel")}
                    value={editingTable.probeQuery ?? ""}
                    onChange={(e) =>
                      setEditingTable({
                        ...editingTable,
                        probeQuery: e.target.value || null,
                      })
                    }
                    placeholder={t("tableEditForm.freshnessProbePlaceholder")}
                  />
                </FormField>
              )}
              {(editingTable.changeSignal === "ttl" || editingTable.changeSignal === "ttl_probe") &&
                (() => {
                  // A __derived__ view has no Cache TTL — its ttl cadence is the materialized view's
                  // Refresh Interval. A non-materialized view can't honor ttl (nothing to refresh).
                  if (isView) {
                    return editingTable.materialize ? (
                      <FormField size="full">
                        <Text size="xs" c="dimmed">
                          {t("tableEditForm.ttlViewRefreshes", {
                            sec: editingTable.mvRefreshInterval,
                          })}
                        </Text>
                      </FormField>
                    ) : (
                      <FormField size="full">
                        <Text size="xs" c="var(--warning, #d19a00)">
                          {t("tableEditForm.ttlViewNeedsMv")}{" "}
                          <strong>{t("tableEditForm.materializedViewLabel")}</strong>{" "}
                          {t("tableEditForm.ttlViewNeedsMvPost")}
                        </Text>
                      </FormField>
                    );
                  }
                  const cs = sources.find((s) => s.id === editingTable.sourceId);
                  // Mirror the Cache TTL input's value resolution: the staged edit
                  // (cacheTtlEdits) wins, then the table value, then the source.
                  const staged = cacheTtlEdits[editingTable.id]?.value;
                  const tableTtl =
                    staged != null && staged !== ""
                      ? Number(staged)
                      : staged === ""
                        ? null
                        : editingTable.cacheTtl;
                  const effTtl = tableTtl ?? cs?.cacheTtl ?? null;
                  const fromTable = tableTtl != null;
                  return effTtl == null ? (
                    <FormField size="full">
                      <Text size="xs" c="var(--warning, #d19a00)">
                        {t("tableEditForm.ttlNeedsIntervalPre")}{" "}
                        <strong>{t("tableEditForm.cacheTtlLabel")}</strong>{" "}
                        {t("tableEditForm.ttlNeedsIntervalPost")}
                      </Text>
                    </FormField>
                  ) : effTtl === NEVER_EXPIRES ? null : ( // its Cache TTL's hint says it
                    <FormField size="full">
                      <Text size="xs" c="dimmed">
                        {t("tableEditForm.refreshesEvery", {
                          sec: effTtl,
                          source: fromTable
                            ? t("tableEditForm.sourceTable")
                            : t("tableEditForm.sourceSource"),
                        })}
                      </Text>
                    </FormField>
                  );
                })()}
              {(editingTable.changeSignal === "debezium" ||
                editingTable.changeSignal === "kafka") &&
                (() => {
                  const cs = sources.find((s) => s.id === editingTable.sourceId);
                  const hasCdc = !!cs?.cdc?.bootstrapServers;
                  const hasPk = editingTable.columns.some((c) => c.isPrimaryKey);
                  // Debezium derives {prefix}.{schema}.{table}; a plain Kafka feed
                  // consumes the topic named for the table (kafka_provider: topic=table).
                  const topic =
                    editingTable.changeSignal === "debezium"
                      ? `${cs?.cdc?.topicPrefix}.${editingTable.schemaName}.${editingTable.tableName}`
                      : editingTable.tableName;
                  return (
                    <>
                      {hasCdc ? (
                        <FormField size="full">
                          <Text size="xs" c="dimmed">
                            {t("tableEditForm.cdcTransportPre")}
                            {cs!.cdc!.bootstrapServers}
                            {t("tableEditForm.cdcTransportMid")} <code>{topic}</code>.{" "}
                            {t("tableEditForm.cdcTransportPost")}
                          </Text>
                        </FormField>
                      ) : (
                        <FormField size="full">
                          <Text size="xs" c="var(--warning, #d19a00)">
                            {editingTable.changeSignal} {t("tableEditForm.cdcMissingPre")}{" "}
                            <strong>{t("tableEditForm.cdcMissingBold")}</strong>.{" "}
                            {t("tableEditForm.cdcMissingPost")}
                          </Text>
                        </FormField>
                      )}
                      {!hasPk && (
                        <FormField size="full">
                          <Text size="xs" c="var(--warning, #d19a00)">
                            {t("tableEditForm.noPkPre")}{" "}
                            <strong>{t("tableEditForm.noPkBold")}</strong>{" "}
                            {t("tableEditForm.noPkPost")}
                          </Text>
                        </FormField>
                      )}
                    </>
                  );
                })()}
              {!isView && (
                <TableLoadManagementFields
                  editingTable={editingTable}
                  setEditingTable={setEditingTable}
                  editSource={editSource}
                  roles={roles}
                  cacheTtlEdits={cacheTtlEdits}
                  setCacheTtlEdits={setCacheTtlEdits}
                  floorTtl={floorTtl}
                  ttlSignalError={Boolean(ttlSignalError)}
                  effLoadProtected={Boolean(effLoadProtected)}
                />
              )}
              {isView && (
                <FormChecks>
                  <FormField
                    size="short"
                    help={`${t("tableEditForm.materializedViewDesc")} ${t("tableEditForm.materializedViewHelp")}`}
                  >
                    <Checkbox
                      checked={editingTable.materialize}
                      onChange={(e) =>
                        setEditingTable({
                          ...editingTable,
                          materialize: e.currentTarget.checked,
                        })
                      }
                      label={t("tableEditForm.materializedViewLabel")}
                    />
                  </FormField>
                </FormChecks>
              )}
              {editingTable.viewSql && editingTable.materialize && (
                <MaterializedViewPanels
                  editingTable={editingTable}
                  setEditingTable={setEditingTable}
                />
              )}
              <FormChecks>
                <LiveDeliveryFieldset
                  part="tick"
                  editingTable={editingTable}
                  setEditingTable={setEditingTable}
                  editingColumnTypes={editingColumnTypes}
                  sources={sources}
                  settings={settings}
                />
                <DeltaFieldset
                  part="tick"
                  editingTable={editingTable}
                  setEditingTable={setEditingTable}
                />
              </FormChecks>
              <LiveDeliveryFieldset
                part="settings"
                editingTable={editingTable}
                setEditingTable={setEditingTable}
                editingColumnTypes={editingColumnTypes}
                sources={sources}
                settings={settings}
              />
              <DeltaFieldset
                part="settings"
                editingTable={editingTable}
                setEditingTable={setEditingTable}
              />
              {!isView && (
                <LandingScopeFields
                  editingTable={editingTable}
                  setEditingTable={setEditingTable}
                  source={editSource}
                />
              )}
            </FormLayout>
          </>
        ))}
      {section === "query" &&
        (hidingOnly ? (
          <>
            <QueryRead table={editingTable} />
            <ParametersPanel columns={editingTable.columns} />
          </>
        ) : (
          <>
            <FormLayout className="form-card" data-testid="table-section-query-panel">
              <FormChecks>
                <FormField size="short" help={t("tableEditForm.enableAggregatesDesc")}>
                  <Checkbox
                    checked={editingTable.enableAggregates}
                    onChange={(e) =>
                      setEditingTable({
                        ...editingTable,
                        enableAggregates: e.currentTarget.checked,
                      })
                    }
                    label={t("tableEditForm.enableAggregatesLabel")}
                  />
                </FormField>
                <FormField size="short" help={t("tableEditForm.enableGroupByDesc")}>
                  <Checkbox
                    checked={editingTable.enableGroupBy}
                    onChange={(e) =>
                      setEditingTable({
                        ...editingTable,
                        enableGroupBy: e.currentTarget.checked,
                      })
                    }
                    label={t("tableEditForm.enableGroupByLabel")}
                  />
                </FormField>
              </FormChecks>
            </FormLayout>
            {(() => {
              const NOSQL = new Set(["mongodb", "cassandra"]);
              const src = sources.find((s) => s.id === editingTable.sourceId);
              // Views and materialized views are read-only — no INSERT/UPDATE path, so presets never apply.
              const isReadOnlyView = editingTable.viewSql != null;
              const isMutable =
                src && !NOSQL.has((src.type ?? "").toLowerCase()) && !isReadOnlyView;
              return isMutable ? (
                <div>
                  <ColumnPresetsEditor
                    presets={editingTable.columnPresets}
                    columns={editingTable.columns.map((c) => c.columnName)}
                    columnTypes={editingColumnTypes}
                    onChange={(presets) =>
                      setEditingTable({ ...editingTable, columnPresets: presets })
                    }
                  />
                </div>
              ) : null;
            })()}
            {/* REQ-1986: the table's parameters, and the steward's required switch. */}
            <ParametersPanel
              columns={editingTable.columns}
              onRequiredChange={(i, required) => updateEditCol(i, "requiredBySteward", required)}
            />
          </>
        ))}
      {section === "quality" &&
        (hidingOnly ? (
          <QualityRead table={editingTable} source={editSource} />
        ) : (
          <>
            {/* REQ-1443 clause 7: a checker source's table lands that checker's scans, so its contract is
              edited here. Only a checker source has one — every other table has no contract to build. */}
            {editSource != null && isCheckerTable && (
              <DataQualityPanel
                checker={(editSource.type ?? "").toLowerCase()}
                sourceId={editSource.id}
                tableId={editingTable.id}
                contractText={editingTable.dqContract ?? ""}
                onChange={(text) => setEditingTable({ ...editingTable, dqContract: text || null })}
              />
            )}
            {/* REQ-1934: any table but a checker's results or a profiler's own may join a profiler. */}
            {editSource != null && !isCheckerTable && editSource.type !== "data_profiler" && (
              <ProfilerPanel {...{ editingTable, savedProfilerId, setEditingTable }} />
            )}
            {/* REQ-1093: table-level UNIQUE constraints editor */}
            <div>
              <UniquesPanel
                flat
                uniques={editingTable.uniqueConstraints ?? []}
                columns={editingTable.columns.map((c) => c.columnName)}
                onChange={(uniques) =>
                  setEditingTable({ ...editingTable, uniqueConstraints: uniques })
                }
              />
            </div>
          </>
        ))}
      {section === "columns" && (
        <div data-testid="table-section-columns-panel">
          {/* One tab bar for everything of a column: its own attributes, and its test data. */}
          <ColumnTabs value={tab} onChange={setTab} shown={shownTabs} nested />
          {tab === "testing" ? (
            <TestDataColumns table={editingTable} updateEditCol={updateEditCol} />
          ) : (
            <Table.ScrollContainer minWidth={640}>
              <Table className="data-table" style={{ margin: "0 0 0.5rem" }}>
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>{t("tableEditForm.columnHeader")}</Table.Th>
                    {tab === "naming" && (
                      <>
                        <Table.Th>{t("tableEditForm.sqlAliasHeader")}</Table.Th>
                        <Table.Th w="60%">{t("tableEditForm.descriptionHeader")}</Table.Th>
                        {editSource?.type === "ingest" && (
                          <Table.Th>{t("tableEditForm.jsonPathHeader")}</Table.Th>
                        )}
                      </>
                    )}
                    {tab === "type" && (
                      <>
                        <Table.Th>{t("tableEditForm.dataTypeHeader")}</Table.Th>
                        <Table.Th>{t("tableEditForm.pkHeader")}</Table.Th>
                        <Table.Th>{t("tableEditForm.scopeHeader")}</Table.Th>
                        {editSource?.type === "qdrant" && (
                          <Table.Th>{t("vectorModelCell.header")}</Table.Th>
                        )}
                      </>
                    )}
                    {/* REQ-1958: a caller who is not shown a column's grant lists and mask edits
                      the table without them: the Access tab is not offered, the save names none
                      and the store keeps them. */}
                    {tab === "access" && (
                      <>
                        <Table.Th>{t("tableEditForm.visibleToHeader")}</Table.Th>
                        <Table.Th>{t("tableEditForm.writableByHeader")}</Table.Th>
                        <Table.Th>{t("tableEditForm.maskingHeader")}</Table.Th>
                      </>
                    )}
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {editingTable.columns.map((c, i) => (
                    <Fragment key={c.id}>
                      <Table.Tr data-testid={`column-row-${c.columnName}`}>
                        <ColumnNameCell table={editingTable} c={c} definition={definitionOf(c)} />
                        {tab === "naming" && (
                          <>
                            <Table.Td>
                              <TextInput
                                aria-label={t("tableEditForm.sqlAliasHeader")}
                                value={c.alias || c.computedSqlAlias}
                                onChange={(e) => updateEditCol(i, "alias", e.target.value)}
                                disabled={hidingOnly}
                              />
                            </Table.Td>
                            <Table.Td>
                              {hidingOnly ? (
                                <Text size="sm">{c.description || ""}</Text>
                              ) : (
                                <DescriptionField
                                  value={c.description || ""}
                                  onChange={(v) => updateEditCol(i, "description", v)}
                                  placeholder={t("tableEditForm.descriptionHeader")}
                                  rows={1}
                                  generating={generatingColDesc === c.columnName}
                                  onGenerate={async () => {
                                    setGeneratingColDesc(c.columnName);
                                    try {
                                      const desc = await generateColumnDescription(
                                        editingTable.id,
                                        c.columnName,
                                      );
                                      if (desc) updateEditCol(i, "description", desc);
                                    } catch (err) {
                                      console.error("generateColumnDescription failed:", err);
                                    } finally {
                                      setGeneratingColDesc(null);
                                    }
                                  }}
                                />
                              )}
                            </Table.Td>
                            {editSource?.type === "ingest" && (
                              <Table.Td>
                                <TextInput
                                  aria-label={t("tableEditForm.jsonPathHeader")}
                                  value={c.path || ""}
                                  onChange={(e) => updateEditCol(i, "path", e.target.value)}
                                  disabled={hidingOnly}
                                  placeholder="payload.order_id"
                                  data-testid={`table-edit-col-path-${c.columnName}`}
                                />
                              </Table.Td>
                            )}
                          </>
                        )}
                        {tab === "type" && (
                          <>
                            <Table.Td>
                              <Select
                                aria-label={t("tableEditForm.dataTypeHeader")}
                                placeholder={t("tableEditForm.dataTypePlaceholder")}
                                data={Array.from(
                                  new Set([
                                    ...(c.dataType ? [toIrType(c.dataType)] : []),
                                    ...irTypes,
                                  ]),
                                ).map((v) => ({ value: v, label: v }))}
                                value={c.dataType ? toIrType(c.dataType) : null}
                                onChange={(v) => updateEditCol(i, "dataType", v ?? "")}
                                disabled={hidingOnly}
                                searchable
                                comboboxProps={{ withinPortal: true }}
                                styles={{ input: { fontFamily: "monospace" } }}
                              />
                              {c.dataType && toIrType(c.dataType) === "numeric" && (
                                <NumericSizeInputs
                                  key={c.columnName}
                                  dataType={c.dataType}
                                  onChange={(v) => updateEditCol(i, "dataType", v)}
                                  disabled={hidingOnly}
                                  testId={`edit-table-col-numeric-size-${c.columnName}`}
                                />
                              )}
                            </Table.Td>
                            <Table.Td style={{ textAlign: "center" }}>
                              <Tip text={t("tableEditForm.primaryKeyAria")}>
                                <Checkbox
                                  aria-label={t("tableEditForm.primaryKeyAria")}
                                  checked={c.isPrimaryKey || false}
                                  disabled={hidingOnly}
                                  onChange={(e) =>
                                    updateEditCol(i, "isPrimaryKey", e.currentTarget.checked)
                                  }
                                />
                              </Tip>
                            </Table.Td>
                            <Table.Td>
                              <Select
                                aria-label={t("tableEditForm.scopeHeader")}
                                title={t("tableEditForm.scopeHelp")}
                                data={[
                                  { value: "domain", label: t("tableEditForm.scopeDomain") },
                                  { value: "public", label: t("tableEditForm.scopePublic") },
                                  {
                                    value: "restricted",
                                    label: t("tableEditForm.scopeRestricted"),
                                  },
                                ]}
                                value={c.scope || "domain"}
                                onChange={(v) => updateEditCol(i, "scope", v ?? "domain")}
                                disabled={hidingOnly}
                                comboboxProps={{ withinPortal: true }}
                                allowDeselect={false}
                              />
                            </Table.Td>
                            {editSource?.type === "qdrant" && (
                              <Table.Td>
                                <VectorModelCell
                                  column={c.columnName}
                                  path={c.path}
                                  value={c.embeddingModel}
                                  models={vectorModels}
                                  onChange={(m) => updateEditCol(i, "embeddingModel", m ?? "")}
                                />
                              </Table.Td>
                            )}
                          </>
                        )}
                        {tab === "access" && (
                          <>
                            <Table.Td>
                              <GrantPicker
                                options={roleOptions}
                                value={shownGrants(c.visibleTo)}
                                onChange={(selected) => updateEditCol(i, "visibleTo", selected)}
                                ariaLabel={t("tableEditForm.visibleToHeader")}
                              />
                            </Table.Td>
                            <Table.Td>
                              <GrantPicker
                                options={roleOptions}
                                value={shownGrants(c.writableBy)}
                                onChange={(selected) => updateEditCol(i, "writableBy", selected)}
                                ariaLabel={t("tableEditForm.writableByHeader")}
                                disabled={hidingOnly}
                              />
                            </Table.Td>
                            <Table.Td>
                              <Select
                                aria-label={t("tableEditForm.maskingHeader")}
                                data={maskOptions(t)}
                                value={c.maskType || ""}
                                onChange={(v) => updateEditCol(i, "maskType", v ?? "")}
                                comboboxProps={{ withinPortal: true }}
                                allowDeselect={false}
                              />
                            </Table.Td>
                          </>
                        )}
                      </Table.Tr>
                      {tab === "access" && c.maskType && (
                        <Table.Tr>
                          <Table.Td
                            style={{
                              paddingInlineStart: "1.5rem",
                              color: "var(--text-muted)",
                              fontSize: "0.75rem",
                            }}
                          >
                            {t("tableEditForm.maskingTemplateLabel")}
                          </Table.Td>
                          {c.maskType === "regex" && (
                            <>
                              <Table.Td>
                                <TextInput
                                  aria-label={t("tableEditForm.regexPatternPlaceholder")}
                                  value={c.maskPattern || ""}
                                  onChange={(e) => updateEditCol(i, "maskPattern", e.target.value)}
                                  placeholder={t("tableEditForm.regexPatternPlaceholder")}
                                />
                              </Table.Td>
                              <Table.Td>
                                <TextInput
                                  aria-label={t("tableEditForm.regexReplacementPlaceholder")}
                                  value={c.maskReplace || ""}
                                  onChange={(e) => updateEditCol(i, "maskReplace", e.target.value)}
                                  placeholder={t("tableEditForm.regexReplacementPlaceholder")}
                                />
                              </Table.Td>
                            </>
                          )}
                          {c.maskType === "constant" && (
                            <Table.Td colSpan={2}>
                              <TextInput
                                aria-label={t("tableEditForm.constantValuePlaceholder")}
                                value={c.maskValue || ""}
                                onChange={(e) => updateEditCol(i, "maskValue", e.target.value)}
                                placeholder={t("tableEditForm.constantValuePlaceholder")}
                              />
                            </Table.Td>
                          )}
                          {c.maskType === "truncate" && (
                            <Table.Td colSpan={2}>
                              <Select
                                aria-label={t("tableEditForm.truncatePrecisionPlaceholder")}
                                data={[
                                  { value: "year", label: t("tableEditForm.precisionYear") },
                                  { value: "month", label: t("tableEditForm.precisionMonth") },
                                  { value: "day", label: t("tableEditForm.precisionDay") },
                                  { value: "hour", label: t("tableEditForm.precisionHour") },
                                ]}
                                placeholder={t("tableEditForm.truncatePrecisionPlaceholder")}
                                value={c.maskPrecision || null}
                                onChange={(v) => updateEditCol(i, "maskPrecision", v ?? "")}
                                comboboxProps={{ withinPortal: true }}
                              />
                            </Table.Td>
                          )}
                          <Table.Td colSpan={2}>
                            <GrantPicker
                              options={roleOptions}
                              value={shownGrants(c.unmaskedTo)}
                              onChange={(selected) => updateEditCol(i, "unmaskedTo", selected)}
                              label={t("tableEditForm.unmaskedToAria")}
                            />
                          </Table.Td>
                        </Table.Tr>
                      )}
                    </Fragment>
                  ))}
                </Table.Tbody>
              </Table>
            </Table.ScrollContainer>
          )}
        </div>
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
