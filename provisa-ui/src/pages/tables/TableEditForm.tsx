// Copyright (c) 2026 Kenneth Stott
// Canary: 4a8c1f59-6e3d-4b2a-9f7c-0d5e8b1a3c72
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Fragment } from "react";
import { Check, X, Loader2 } from "lucide-react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Badge,
  Checkbox,
  Group,
  NumberInput,
  Select,
  Table,
  Text,
  TextInput,
  Tooltip,
} from "@mantine/core";
import { MultiSelect } from "../../components/MultiSelect";
import { ColumnPresetsEditor } from "../../components/admin/ColumnPresetsEditor";
import type { RegisteredTable, Source } from "../../types/admin";
import type { Role } from "../../types/auth";
import type { PlatformSettings } from "../../api/admin";
import { sourceProbeTypes } from "../../liveCapability";
import { NAMING_CONVENTIONS } from "./constants";
import { DescriptionField } from "./DescriptionField";
import { LiveDeliveryFieldset } from "./LiveDeliveryFieldset";

interface CacheTtlEdit {
  value: string;
  dirty: boolean;
  saving: boolean;
}

interface TableEditFormProps {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
  editingColumnTypes: Record<string, string>;
  cacheTtlEdits: Record<number, CacheTtlEdit>;
  setCacheTtlEdits: React.Dispatch<React.SetStateAction<Record<number, CacheTtlEdit>>>;
  sources: Source[];
  roles: Role[];
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
  updateEditCol: (i: number, key: string, value: string | string[] | boolean) => void;
}

function FieldLabel({ text, help }: { text: string; help: string }) {
  return (
    <Group gap={4} wrap="nowrap">
      <Text component="span" size="sm">
        {text}
      </Text>
      <Tooltip label={help} multiline w={320}>
        <Text
          component="span"
          size="xs"
          c="dimmed"
          style={{ cursor: "help", lineHeight: 1 }}
        >
          ⓘ
        </Text>
      </Tooltip>
    </Group>
  );
}

export function TableEditForm({
  editingTable,
  setEditingTable,
  editingColumnTypes,
  cacheTtlEdits,
  setCacheTtlEdits,
  sources,
  roles,
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
  updateEditCol,
}: TableEditFormProps) {
  const { t } = useTranslation();
  const roleOptions = roles.map((r) => ({ id: r.id, label: r.id }));
  return (
    <>
      <div className="form-card" style={{ marginBottom: "0.75rem" }}>
        <TextInput
          label={
            <FieldLabel
              text={t("tableEditForm.sqlAliasLabel")}
              help={t("tableEditForm.sqlAliasHelp")}
            />
          }
          value={editingTable.alias || ""}
          onChange={(e) =>
            setEditingTable({
              ...editingTable,
              alias: e.target.value || null,
            })
          }
          placeholder={t("tableEditForm.sqlAliasPlaceholder")}
        />
        <Select
          label={
            <FieldLabel
              text={t("tableEditForm.namingConventionLabel")}
              help={t("tableEditForm.namingConventionHelp")}
            />
          }
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
        <NumberInput
          label={
            <FieldLabel
              text={t("tableEditForm.cacheTtlLabel")}
              help={t("tableEditForm.cacheTtlHelp")}
            />
          }
          min={0}
          value={
            cacheTtlEdits[editingTable.id]?.value ??
            (editingTable.cacheTtl != null ? editingTable.cacheTtl : "")
          }
          onChange={(v) =>
            setCacheTtlEdits((prev) => ({
              ...prev,
              [editingTable.id]: {
                ...prev[editingTable.id],
                value: v === "" ? "" : String(v),
                dirty: true,
              },
            }))
          }
          placeholder={t("tableEditForm.cacheTtlPlaceholder")}
        />
        <Select
          label={
            <FieldLabel
              text={t("tableEditForm.preferMaterializedLabel")}
              help={t("tableEditForm.preferMaterializedHelp")}
            />
          }
          data={[
            { value: "inherit", label: t("tableEditForm.inheritSource") },
            { value: "on", label: t("tableEditForm.on") },
            { value: "off", label: t("tableEditForm.off") },
          ]}
          value={
            editingTable.preferMaterialized == null
              ? "inherit"
              : editingTable.preferMaterialized
                ? "on"
                : "off"
          }
          onChange={(v) =>
            setEditingTable({
              ...editingTable,
              preferMaterialized: v === "inherit" ? null : v === "on",
            })
          }
          comboboxProps={{ withinPortal: true }}
          allowDeselect={false}
        />
        <div style={{ gridColumn: "1 / -1" }}>
          <FieldLabel
            text={t("tableEditForm.descriptionLabel")}
            help={t("tableEditForm.descriptionHelp")}
          />
          <DescriptionField
            value={editingTable.description || ""}
            onChange={(v) =>
              setEditingTable({ ...editingTable, description: v || null })
            }
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
        {editingTable.viewSql && (
          <>
            <Group
              gap="xs"
              wrap="nowrap"
              style={{ gridColumn: "1 / -1" }}
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
              <Text size="sm" c="dimmed">
                {t("tableEditForm.materializedViewDesc")}
              </Text>
              <Tooltip label={t("tableEditForm.materializedViewHelp")} multiline w={320}>
                <Text
                  component="span"
                  size="xs"
                  c="dimmed"
                  style={{ cursor: "help", lineHeight: 1 }}
                >
                  ⓘ
                </Text>
              </Tooltip>
            </Group>
            {editingTable.materialize && (
              <NumberInput
                label={
                  <FieldLabel
                    text={t("tableEditForm.refreshIntervalLabel")}
                    help={t("tableEditForm.refreshIntervalHelp")}
                  />
                }
                min={30}
                value={editingTable.mvRefreshInterval}
                onChange={(v) =>
                  setEditingTable({
                    ...editingTable,
                    mvRefreshInterval:
                      typeof v === "number" ? v : parseInt(String(v), 10) || 300,
                  })
                }
              />
            )}
            {editingTable.materialize && (
              <div title={t("tableEditForm.nrtDebounceTitle")}>
                <Text size="sm" c="dimmed" mb={4}>
                  {t("tableEditForm.nrtDebounceLabel")}
                </Text>
                <Group gap="xs">
                  <NumberInput
                    min={0}
                    step={0.5}
                    placeholder={t("tableEditForm.nrtQuietPlaceholder")}
                    aria-label={t("tableEditForm.nrtQuietAria")}
                    data-testid="mv-debounce-quiet"
                    value={editingTable.mvDebounceQuiet}
                    onChange={(v) =>
                      setEditingTable({
                        ...editingTable,
                        mvDebounceQuiet:
                          typeof v === "number" ? v : parseFloat(String(v)) || 0,
                      })
                    }
                  />
                  <NumberInput
                    min={0}
                    step={0.5}
                    placeholder={t("tableEditForm.nrtMaxDelayPlaceholder")}
                    aria-label={t("tableEditForm.nrtMaxDelayAria")}
                    data-testid="mv-debounce-max-delay"
                    value={editingTable.mvDebounceMaxDelay}
                    onChange={(v) =>
                      setEditingTable({
                        ...editingTable,
                        mvDebounceMaxDelay:
                          typeof v === "number" ? v : parseFloat(String(v)) || 0,
                      })
                    }
                  />
                </Group>
              </div>
            )}
            {editingTable.materialize && (
              <Select
                label={
                  <FieldLabel
                    text={t("tableEditForm.consistencyLabel")}
                    help={t("tableEditForm.consistencyHelp")}
                  />
                }
                aria-label={t("tableEditForm.mvConsistencyAria")}
                data-testid="mv-consistency"
                data={[
                  { value: "shared", label: t("tableEditForm.consistencyShared") },
                  {
                    value: "distributed",
                    label: t("tableEditForm.consistencyDistributed"),
                  },
                ]}
                value={editingTable.mvConsistency}
                onChange={(v) =>
                  setEditingTable({
                    ...editingTable,
                    mvConsistency: v ?? editingTable.mvConsistency,
                  })
                }
                comboboxProps={{ withinPortal: true }}
                allowDeselect={false}
              />
            )}
          </>
        )}
        <Group gap="xs" wrap="nowrap" style={{ gridColumn: "1 / -1" }}>
          <Checkbox
            checked={editingTable.dataProduct}
            onChange={(e) =>
              setEditingTable({
                ...editingTable,
                dataProduct: e.currentTarget.checked,
              })
            }
            label={t("tableEditForm.dataProductLabel")}
          />
          <Text size="sm" c="dimmed">
            {t("tableEditForm.dataProductDesc")}
          </Text>
        </Group>
        <Group gap="xs" wrap="nowrap" style={{ gridColumn: "1 / -1" }}>
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
          <Text size="sm" c="dimmed">
            {t("tableEditForm.enableAggregatesDesc")}
          </Text>
        </Group>
        <Group gap="xs" wrap="nowrap" style={{ gridColumn: "1 / -1" }}>
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
          <Text size="sm" c="dimmed">
            {t("tableEditForm.enableGroupByDesc")}
          </Text>
        </Group>
        {editingTable.apiEndpoint && (
          <TextInput
            style={{ gridColumn: "1 / -1" }}
            label={t("tableEditForm.apiEndpointLabel")}
            readOnly
            value={editingTable.apiEndpoint}
            styles={{ input: { color: "var(--text-muted)", cursor: "default" } }}
          />
        )}
        <Select
          label={
            <FieldLabel
              text={t("tableEditForm.changeSignalLabel")}
              help={t("tableEditForm.changeSignalHelp")}
            />
          }
          data={[
            { value: "", label: t("tableEditForm.csInherit") },
            { value: "ttl", label: t("tableEditForm.csTtl") },
            { value: "probe", label: t("tableEditForm.csProbe") },
            { value: "ttl_probe", label: t("tableEditForm.csTtlProbe") },
            { value: "native", label: t("tableEditForm.csNative") },
            { value: "debezium", label: t("tableEditForm.csDebezium") },
            { value: "kafka", label: t("tableEditForm.csKafka") },
          ]}
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
        {(editingTable.changeSignal === "ttl" ||
          editingTable.changeSignal === "ttl_probe") &&
          (() => {
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
              <Text
                style={{ gridColumn: "1 / -1" }}
                size="xs"
                c="var(--warning, #d19a00)"
              >
                {t("tableEditForm.ttlNeedsIntervalPre")}{" "}
                <strong>{t("tableEditForm.cacheTtlLabel")}</strong>{" "}
                {t("tableEditForm.ttlNeedsIntervalPost")}
              </Text>
            ) : (
              <Text style={{ gridColumn: "1 / -1" }} size="xs" c="dimmed">
                {t("tableEditForm.refreshesEvery", {
                  sec: effTtl,
                  source: fromTable
                    ? t("tableEditForm.sourceTable")
                    : t("tableEditForm.sourceSource"),
                })}
              </Text>
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
                  <Text style={{ gridColumn: "1 / -1" }} size="xs" c="dimmed">
                    {t("tableEditForm.cdcTransportPre")}
                    {cs!.cdc!.bootstrapServers}
                    {t("tableEditForm.cdcTransportMid")}{" "}
                    <code>{topic}</code>. {t("tableEditForm.cdcTransportPost")}
                  </Text>
                ) : (
                  <Text
                    style={{ gridColumn: "1 / -1" }}
                    size="xs"
                    c="var(--warning, #d19a00)"
                  >
                    {editingTable.changeSignal} {t("tableEditForm.cdcMissingPre")}{" "}
                    <strong>{t("tableEditForm.cdcMissingBold")}</strong>.{" "}
                    {t("tableEditForm.cdcMissingPost")}
                  </Text>
                )}
                {!hasPk && (
                  <Text
                    style={{ gridColumn: "1 / -1" }}
                    size="xs"
                    c="var(--warning, #d19a00)"
                  >
                    {t("tableEditForm.noPkPre")}{" "}
                    <strong>{t("tableEditForm.noPkBold")}</strong>{" "}
                    {t("tableEditForm.noPkPost")}
                  </Text>
                )}
              </>
            );
          })()}
        {(editingTable.changeSignal === "probe" ||
          editingTable.changeSignal === "ttl_probe") &&
          (() => {
            const src = sources.find((s) => s.id === editingTable.sourceId);
            const caps = sourceProbeTypes(src?.type);
            if (caps.length === 0) return null;
            return (
              <Select
                style={{ gridColumn: "1 / -1" }}
                label={
                  <FieldLabel
                    text={t("tableEditForm.probeTypeLabel")}
                    help={t("tableEditForm.probeTypeHelp")}
                  />
                }
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
            );
          })()}
        {(editingTable.changeSignal === "probe" ||
          editingTable.changeSignal === "ttl_probe") && (
          <TextInput
            style={{ gridColumn: "1 / -1" }}
            label={
              <FieldLabel
                text={t("tableEditForm.freshnessProbeLabel")}
                help={t("tableEditForm.freshnessProbeHelp")}
              />
            }
            value={editingTable.probeQuery ?? ""}
            onChange={(e) =>
              setEditingTable({
                ...editingTable,
                probeQuery: e.target.value || null,
              })
            }
            placeholder={t("tableEditForm.freshnessProbePlaceholder")}
          />
        )}
        <LiveDeliveryFieldset
          editingTable={editingTable}
          setEditingTable={setEditingTable}
          editingColumnTypes={editingColumnTypes}
          sources={sources}
          settings={settings}
        />
      </div>
      {(() => {
        const NOSQL = new Set(["mongodb", "cassandra"]);
        const src = sources.find((s) => s.id === editingTable.sourceId);
        // Views and materialized views are read-only — no INSERT/UPDATE path, so presets never apply.
        const isReadOnlyView = editingTable.viewSql != null;
        const isMutable = src && !NOSQL.has((src.type ?? "").toLowerCase()) && !isReadOnlyView;
        return isMutable ? (
          <ColumnPresetsEditor
            presets={editingTable.columnPresets}
            columns={editingTable.columns.map((c) => c.columnName)}
            columnTypes={editingColumnTypes}
            onChange={(presets) =>
              setEditingTable({ ...editingTable, columnPresets: presets })
            }
          />
        ) : null;
      })()}
      <Table className="data-table" style={{ margin: "0 0 0.5rem" }}>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("tableEditForm.columnHeader")}</Table.Th>
            <Table.Th>{t("tableEditForm.pkHeader")}</Table.Th>
            <Table.Th>{t("tableEditForm.sqlAliasHeader")}</Table.Th>
            <Table.Th>{t("tableEditForm.descriptionHeader")}</Table.Th>
            <Table.Th>{t("tableEditForm.visibleToHeader")}</Table.Th>
            <Table.Th>{t("tableEditForm.writableByHeader")}</Table.Th>
            <Table.Th>{t("tableEditForm.maskingHeader")}</Table.Th>
            <Table.Th>{t("tableEditForm.scopeHeader")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {editingTable.columns.map((c, i) => (
            <Fragment key={c.id}>
              <Table.Tr>
                <Table.Td>
                  <code>{c.columnName}</code>
                  {c.nativeFilterType && (
                    <Badge
                      ml={6}
                      size="xs"
                      variant="light"
                      color={c.nativeFilterType === "path_param" ? "yellow" : "blue"}
                      style={{ fontFamily: "monospace" }}
                    >
                      {c.nativeFilterType === "path_param"
                        ? t("tableEditForm.pathBadge")
                        : t("tableEditForm.queryBadge")}
                    </Badge>
                  )}
                  {c.isForeignKey && (
                    <Badge
                      ml={6}
                      size="xs"
                      variant="light"
                      color="green"
                      style={{ fontFamily: "monospace" }}
                    >
                      {t("tableEditForm.fkBadge")}
                    </Badge>
                  )}
                  {c.isAlternateKey && (
                    <Badge
                      ml={6}
                      size="xs"
                      variant="light"
                      color="yellow"
                      style={{ fontFamily: "monospace" }}
                    >
                      {t("tableEditForm.akBadge")}
                    </Badge>
                  )}
                </Table.Td>
                <Table.Td style={{ textAlign: "center" }}>
                  <Checkbox
                    aria-label={t("tableEditForm.primaryKeyAria")}
                    title={t("tableEditForm.primaryKeyAria")}
                    checked={c.isPrimaryKey || false}
                    onChange={(e) =>
                      updateEditCol(i, "isPrimaryKey", e.currentTarget.checked)
                    }
                  />
                </Table.Td>
                <Table.Td>
                  <TextInput
                    aria-label={t("tableEditForm.sqlAliasHeader")}
                    value={c.alias || c.computedSqlAlias}
                    onChange={(e) =>
                      updateEditCol(i, "alias", e.target.value)
                    }
                  />
                </Table.Td>
                <Table.Td>
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
                        console.error(
                          "generateColumnDescription failed:",
                          err,
                        );
                      } finally {
                        setGeneratingColDesc(null);
                      }
                    }}
                  />
                </Table.Td>
                <Table.Td>
                  <MultiSelect
                    options={roleOptions}
                    value={c.visibleTo}
                    onChange={(selected) =>
                      updateEditCol(i, "visibleTo", selected)
                    }
                    label={t("tableEditForm.visibleToHeader")}
                  />
                </Table.Td>
                <Table.Td>
                  <MultiSelect
                    options={roleOptions}
                    value={c.writableBy}
                    onChange={(selected) =>
                      updateEditCol(i, "writableBy", selected)
                    }
                    label={t("tableEditForm.writableByHeader")}
                  />
                </Table.Td>
                <Table.Td>
                  <Select
                    aria-label={t("tableEditForm.maskingHeader")}
                    data={[
                      { value: "", label: t("tableEditForm.maskNone") },
                      { value: "regex", label: t("tableEditForm.maskRegex") },
                      { value: "constant", label: t("tableEditForm.maskConstant") },
                      { value: "truncate", label: t("tableEditForm.maskTruncate") },
                    ]}
                    value={c.maskType || ""}
                    onChange={(v) => updateEditCol(i, "maskType", v ?? "")}
                    comboboxProps={{ withinPortal: true }}
                    allowDeselect={false}
                  />
                </Table.Td>
                <Table.Td>
                  <Select
                    aria-label={t("tableEditForm.scopeHeader")}
                    data={[
                      { value: "domain", label: t("tableEditForm.scopeDomain") },
                      { value: "public", label: t("tableEditForm.scopePublic") },
                      { value: "restricted", label: t("tableEditForm.scopeRestricted") },
                    ]}
                    value={c.scope || "domain"}
                    onChange={(v) => updateEditCol(i, "scope", v ?? "domain")}
                    comboboxProps={{ withinPortal: true }}
                    allowDeselect={false}
                  />
                </Table.Td>
              </Table.Tr>
              {c.maskType && (
                <Table.Tr>
                  <Table.Td
                    colSpan={2}
                    style={{
                      paddingLeft: "1.5rem",
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
                          onChange={(e) =>
                            updateEditCol(i, "maskPattern", e.target.value)
                          }
                          placeholder={t("tableEditForm.regexPatternPlaceholder")}
                        />
                      </Table.Td>
                      <Table.Td>
                        <TextInput
                          aria-label={t("tableEditForm.regexReplacementPlaceholder")}
                          value={c.maskReplace || ""}
                          onChange={(e) =>
                            updateEditCol(i, "maskReplace", e.target.value)
                          }
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
                        onChange={(e) =>
                          updateEditCol(i, "maskValue", e.target.value)
                        }
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
                    <MultiSelect
                      options={roleOptions}
                      value={c.unmaskedTo}
                      onChange={(selected) =>
                        updateEditCol(i, "unmaskedTo", selected)
                      }
                      label={t("tableEditForm.unmaskedToAria")}
                    />
                  </Table.Td>
                </Table.Tr>
              )}
            </Fragment>
          ))}
        </Table.Tbody>
      </Table>
      <Group justify="flex-end" gap="sm" p="0.75rem 0.5rem">
        <Tooltip label={t("tableEditForm.cancel")}>
          <ActionIcon
            variant="subtle"
            aria-label={t("tableEditForm.cancel")}
            data-testid="table-edit-cancel"
            onClick={cancelEditing}
            disabled={saving}
          >
            <X size={14} />
          </ActionIcon>
        </Tooltip>
        <Tooltip label={t("tableEditForm.save")}>
          <ActionIcon
            variant="filled"
            aria-label={t("tableEditForm.save")}
            data-testid="table-edit-save"
            onClick={handleSaveEdit}
            disabled={saving}
          >
            {saving ? (
              <Loader2
                size={14}
                style={{ animation: "spin 1s linear infinite" }}
              />
            ) : (
              <Check size={14} />
            )}
          </ActionIcon>
        </Tooltip>
      </Group>
    </>
  );
}
