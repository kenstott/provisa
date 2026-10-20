// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Select, Text, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { PlatformSettings } from "../../api/admin";
import type { RefreshPolicySummary, RegisteredTable, Source } from "../../types/admin";
import type { Role } from "../../types/auth";
import { NEVER_EXPIRES } from "../../components/form/cacheTtl";
import { FormChecks, FormField, FormLayout } from "../../components/form/FormLayout";
import { sourceProbeTypes } from "../../liveCapability";
import { MaterializedViewPanels, ViewMaterializeTick } from "./MaterializedViewPanels";
import { TableTimeTravel } from "./TimeTravelSection";
import { LiveDeliveryFieldset } from "./LiveDeliveryFieldset";
import { DeltaFieldset } from "./DeltaFieldset";
import { LandingScopeFields } from "./LandingScopeFields";
import { TableLoadManagementFields } from "./TableLoadManagementFields";
import { RefreshPolicyAlert } from "./TableSectionsRead";
import { changeSignalChoices } from "./tableSectionFields";
import type { CacheTtlEdit } from "./tableEditTypes";

/** The Load and timeliness section of a table's form: the same when created and when edited. */
export function TableLoadSection({
  editingTable,
  setEditingTable,
  saved,
  isView,
  shownPolicy,
  sources,
  editSource,
  roles,
  settings,
  editingColumnTypes,
  cacheTtlEdits,
  setCacheTtlEdits,
  floorTtl,
  ttlSignalError,
  effLoadProtected,
}: {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
  /** The table as last saved; null while it is being created. */
  saved: RegisteredTable | null;
  isView: boolean;
  shownPolicy: RefreshPolicySummary | null;
  sources: Source[];
  editSource: Source | undefined;
  roles: Role[];
  settings: PlatformSettings | null;
  editingColumnTypes: Record<string, string>;
  cacheTtlEdits: Record<number, CacheTtlEdit>;
  setCacheTtlEdits: React.Dispatch<React.SetStateAction<Record<number, CacheTtlEdit>>>;
  floorTtl: number | null;
  ttlSignalError: boolean;
  effLoadProtected: boolean;
}) {
  const { t } = useTranslation();
  return (
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
        {(editingTable.changeSignal === "probe" || editingTable.changeSignal === "ttl_probe") &&
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
        {(editingTable.changeSignal === "probe" || editingTable.changeSignal === "ttl_probe") && (
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
        {(editingTable.changeSignal === "debezium" || editingTable.changeSignal === "kafka") &&
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
                      {t("tableEditForm.noPkPre")} <strong>{t("tableEditForm.noPkBold")}</strong>{" "}
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
          <ViewMaterializeTick editingTable={editingTable} setEditingTable={setEditingTable} />
        )}
        {isView && editingTable.materialize && (
          <MaterializedViewPanels
            editingTable={editingTable}
            setEditingTable={setEditingTable}
            saved={saved}
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
        {/* REQ-2109: a table's Time Travel; a view's is with its materialization. */}
        {!isView && (
          <TableTimeTravel
            editingTable={editingTable}
            setEditingTable={setEditingTable}
            saved={saved}
          />
        )}
        {!isView && (
          <LandingScopeFields
            editingTable={editingTable}
            setEditingTable={setEditingTable}
            source={editSource}
          />
        )}
      </FormLayout>
    </>
  );
}
