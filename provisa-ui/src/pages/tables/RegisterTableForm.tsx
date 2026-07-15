// Copyright (c) 2026 Kenneth Stott
// Canary: 5f8a2d14-7b3c-4e9f-a0d1-6c4e8b2f7a31
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, Fragment } from "react";
import { useTranslation } from "react-i18next";
import {
  Button,
  Checkbox,
  Select,
  SimpleGrid,
  Stack,
  Table,
  Text,
  TextInput,
} from "@mantine/core";
import { toSnakeCase } from "../../naming";
import { MultiSelect } from "../../components/MultiSelect";
import { useAvailableSchemas, useAvailableTables } from "../../hooks/useAdminQueries";
import type { RegisteredTable, Source } from "../../types/admin";
import type { Role } from "../../types/auth";
import type { ColumnForm } from "./types";
import { CDC_TYPES } from "./constants";
import { isWatermarkEligible, normalizeDomain } from "./helpers";

interface RegisterTableFormProps {
  sources: Source[];
  domainHints: string[];
  domainAccess: string[];
  checkedDomains: Set<string>;
  domainsEnabled: boolean;
  tables: RegisteredTable[];
  roles: Role[];
  getAvailableColumnsMetadata: (
    sourceId: string,
    schemaName: string,
    tableName: string,
  ) => Promise<
    {
      name: string;
      dataType: string;
      comment?: string | null;
      nativeFilterType?: string | null;
      isPrimaryKey?: boolean | null;
    }[]
  >;
  suggestTableAlias: (tableName: string, domainId: string, sourceId: string) => Promise<string>;
  registerTable: (input: Record<string, unknown>) => Promise<{ success: boolean; message: string }>;
  onSuccess: () => void;
  setError: (e: string | null) => void;
}

export function RegisterTableForm({
  sources,
  domainHints,
  domainAccess,
  checkedDomains,
  domainsEnabled,
  tables,
  roles,
  getAvailableColumnsMetadata,
  suggestTableAlias,
  registerTable,
  onSuccess,
  setError,
}: RegisterTableFormProps) {
  const { t } = useTranslation();
  const [sourceId, setSourceId] = useState("");
  const [domainId, setDomainId] = useState("");
  const [schemaName, setSchemaName] = useState("");
  const [tableName, setTableName] = useState("");
  const [tableAlias, setTableAlias] = useState("");
  const [tableDescription, setTableDescription] = useState("");
  const [columns, setColumns] = useState<ColumnForm[]>([]);
  const [watermarkColumn, setWatermarkColumn] = useState<string>("");
  const [dataProduct, setDataProduct] = useState(false);
  const [loadingColumns, setLoadingColumns] = useState(false);

  const { schemas: availableSchemas, loading: loadingSchemas } = useAvailableSchemas(
    sourceId || null,
  );
  const isFixedSchema = availableSchemas.length === 1;
  const { tables: availableTables, loading: loadingTables } = useAvailableTables(
    sourceId && schemaName ? sourceId : null,
    schemaName || null,
  );

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- form cascade reset: dependent fields cleared when source selection changes
    setSchemaName("");
    setTableName("");
    setTableDescription("");
    setColumns([]);
  }, [sourceId]);

  useEffect(() => {
    if (availableSchemas.length === 1) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- auto-select the only available schema; not derivable without an effect because schemas load asynchronously
      setSchemaName(availableSchemas[0]);
    }
  }, [availableSchemas, sourceId]);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- form cascade reset: dependent fields cleared when schema selection changes
    setTableName("");
    setTableDescription("");
    setColumns([]);
  }, [sourceId, schemaName]);

  // Auto-populate table description from physical database comment
  useEffect(() => {
    if (!tableName) return;
    const meta = availableTables.find((t) => t.name === tableName);
    // eslint-disable-next-line react-hooks/set-state-in-effect -- auto-populate description from physical database comment when table is selected
    if (meta?.comment) setTableDescription(meta.comment);
  }, [tableName, availableTables]);

  // Auto-generate alias from table name using snake_case convention
  useEffect(() => {
    if (!tableName || !domainId || !sourceId) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- cascade reset: alias cleared when table/domain/source deselected
      setTableAlias("");
      return;
    }
    suggestTableAlias(tableName, domainId, sourceId).then(setTableAlias);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- suggestTableAlias is a stable hook callback; re-run only when the table selection changes
  }, [tableName, domainId, sourceId]);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- cascade reset: columns cleared before async fetch when table selection changes
    setColumns([]);
    setWatermarkColumn("");
    if (!sourceId || !schemaName || !tableName) return;
    setLoadingColumns(true);
    getAvailableColumnsMetadata(sourceId, schemaName, tableName)
      .then((cols) => {
        const formed = cols.map((c) => {
          const snake = toSnakeCase(c.name);
          return {
            name: c.name,
            visibleTo: roles.map((r) => r.id),
            writableBy: [],
            unmaskedTo: "",
            maskType: "",
            maskPattern: "",
            maskReplace: "",
            maskValue: "",
            maskPrecision: "",
            alias: snake !== c.name ? snake : "",
            description: c.comment || "",
            selected: true,
            nativeFilterType: c.nativeFilterType ?? null,
            dataType: c.dataType,
            isPrimaryKey: c.isPrimaryKey ?? false,
            scope: c.nativeFilterType ? "public" : "domain",
          };
        });
        setColumns(formed);
        const sourceType = sources.find((s) => s.id === sourceId)?.type ?? "";
        if (!CDC_TYPES.has(sourceType)) {
          const autoWm = formed.find(
            (c) =>
              (c.name === "updated_at" || c.name === "updated") && isWatermarkEligible(c.dataType),
          );
          if (autoWm) setWatermarkColumn(autoWm.name);
        }
      })
      .catch(() => setColumns([]))
      .finally(() => setLoadingColumns(false));
    /* eslint-disable-next-line react-hooks/exhaustive-deps --
       refetch columns only when the table selection changes; roles/sources are read for default seeding and must not retrigger a column fetch */
  }, [sourceId, schemaName, tableName]);

  const updateCol = (i: number, key: keyof ColumnForm, value: string | boolean | string[]) => {
    const next = [...columns];
    next[i] = { ...next[i], [key]: value };
    setColumns(next);
  };

  const handleSubmit = async () => {
    setError(null);
    const selectedCols = columns
      .filter((c) => c.selected)
      .map((c) => ({
        name: c.name,
        visibleTo: c.visibleTo,
        writableBy: c.writableBy,
        unmaskedTo: c.unmaskedTo.trim() ? c.unmaskedTo.split(",").map((s) => s.trim()) : [],
        maskType: c.maskType || undefined,
        maskPattern: c.maskPattern || undefined,
        maskReplace: c.maskReplace || undefined,
        maskValue: c.maskValue || undefined,
        maskPrecision: c.maskPrecision || undefined,
        alias: c.alias || undefined,
        description: c.description || undefined,
        nativeFilterType: c.nativeFilterType || undefined,
        isPrimaryKey: c.isPrimaryKey || undefined,
        scope: c.scope || "domain",
      }));
    if (!sourceId || !schemaName || !tableName) {
      setError(t("registerTableForm.errorRequiredFields"));
      return;
    }
    if (selectedCols.length === 0) {
      setError(t("registerTableForm.errorNoColumnsSelected"));
      return;
    }
    try {
      const result = await registerTable({
        sourceId,
        domainId,
        schemaName: domainId ? normalizeDomain(domainId) : schemaName,
        tableName,
        alias: tableAlias || undefined,
        description: tableDescription || undefined,
        watermarkColumn: watermarkColumn || null,
        dataProduct,
        columns: selectedCols,
      });
      if (!result.success) {
        setError(result.message);
        return;
      }
      setSourceId("");
      setDomainId("");
      setSchemaName("");
      setTableName("");
      setTableAlias("");
      setTableDescription("");
      setColumns([]);
      setWatermarkColumn("");
      setDataProduct(false);
      onSuccess();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const isRegistered = (tbl: { name: string }) =>
    tables.some(
      (rt) => rt.sourceId === sourceId && toSnakeCase(rt.tableName) === toSnakeCase(tbl.name),
    );
  const allTablesRegistered =
    !loadingTables && !!schemaName && availableTables.length > 0 && availableTables.every(isRegistered);

  const sourceType = sources.find((s) => s.id === sourceId)?.type ?? "";
  const isCdcSource = CDC_TYPES.has(sourceType);

  return (
    <Stack data-tour="tables-form" gap="md">
      <SimpleGrid cols={{ base: 1, sm: 2 }}>
        <Select
          label={t("registerTableForm.sourceLabel")}
          placeholder={t("registerTableForm.sourcePlaceholder")}
          data={sources
            .filter(
              (s) =>
                s.allowedDomains.length === 0 ||
                s.allowedDomains.some((d) => checkedDomains.has(d)),
            )
            .map((s) => ({ value: s.id, label: s.id }))}
          value={sourceId || null}
          onChange={(v) => setSourceId(v ?? "")}
          data-testid="register-table-source-select"
        />
        {domainsEnabled && (
          <Select
            label={t("registerTableForm.domainLabel")}
            placeholder={t("registerTableForm.domainPlaceholder")}
            data={domainHints
              .filter((d) => d !== "" && d !== "meta" && d !== "ops")
              .filter((d) => domainAccess.includes("*") || domainAccess.includes(d))
              .map((d) => ({ value: d, label: d }))}
            value={domainId || null}
            onChange={(v) => setDomainId(v ?? "")}
            data-testid="register-table-domain-select"
          />
        )}
        <Select
          label={t("registerTableForm.schemaLabel")}
          placeholder={
            loadingSchemas
              ? t("registerTableForm.schemaLoading")
              : t("registerTableForm.schemaPlaceholder")
          }
          data={availableSchemas.map((s) => ({ value: s, label: s }))}
          value={schemaName || null}
          onChange={(v) => setSchemaName(v ?? "")}
          disabled={!sourceId || loadingSchemas || isFixedSchema}
          data-testid="register-table-schema-select"
        />
        <Select
          label={t("registerTableForm.tableLabel")}
          placeholder={
            loadingTables
              ? t("registerTableForm.tableLoading")
              : allTablesRegistered
                ? t("registerTableForm.tableAllRegistered")
                : t("registerTableForm.tablePlaceholder")
          }
          data={availableTables.map((tbl) => ({
            value: tbl.name,
            label: tbl.name,
            disabled: isRegistered(tbl),
          }))}
          value={tableName || null}
          onChange={(v) => setTableName(v ?? "")}
          disabled={!schemaName || loadingTables || allTablesRegistered}
          data-testid="register-table-table-select"
        />
        <TextInput
          label={
            <>
              {t("registerTableForm.aliasLabel")}{" "}
              <Text span fw="normal" c="dimmed" fz="xs">
                {t("registerTableForm.aliasOptional")}
              </Text>
            </>
          }
          value={tableAlias}
          onChange={(e) => setTableAlias(e.currentTarget.value)}
          placeholder={t("registerTableForm.aliasPlaceholder")}
        />
        <TextInput
          label={
            <>
              {t("registerTableForm.descriptionLabel")}{" "}
              <Text span fw="normal" c="dimmed" fz="xs">
                {t("registerTableForm.descriptionOptional")}
              </Text>
            </>
          }
          value={tableDescription}
          onChange={(e) => setTableDescription(e.currentTarget.value)}
          placeholder={t("registerTableForm.descriptionPlaceholder")}
        />
      </SimpleGrid>
      <Checkbox
        checked={dataProduct}
        onChange={(e) => setDataProduct(e.currentTarget.checked)}
        label={
          <>
            {t("registerTableForm.dataProductLabel")}{" "}
            <Text span fw="normal" c="dimmed" fz="xs">
              {t("registerTableForm.dataProductHint")}
            </Text>
          </>
        }
      />
      {sourceId && (
        <Select
          label={
            <>
              {t("registerTableForm.watermarkLabel")}{" "}
              <Text span fw="normal" c="dimmed" fz="xs">
                {isCdcSource
                  ? t("registerTableForm.watermarkHintOptional")
                  : t("registerTableForm.watermarkHintRequired")}
              </Text>
            </>
          }
          placeholder={
            isCdcSource
              ? t("registerTableForm.watermarkNoneTriggers")
              : t("registerTableForm.watermarkNoneSubscriptions")
          }
          data={columns
            .filter((c) => c.selected && isWatermarkEligible(c.dataType))
            .map((c) => ({ value: c.name, label: `${c.name} (${c.dataType})` }))}
          value={watermarkColumn || null}
          onChange={(v) => setWatermarkColumn(v ?? "")}
          disabled={columns.length === 0}
          clearable
          data-testid="register-table-watermark-select"
        />
      )}
      <Stack gap="xs">
        <Text fw={600} fz="sm">
          {t("registerTableForm.columnsLabel")} {loadingColumns && t("registerTableForm.columnsLoading")}
        </Text>
        {columns.length > 0 && (
          <Table.ScrollContainer minWidth={900}>
            <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
              <Table.Thead>
                <Table.Tr>
                  <Table.Th></Table.Th>
                  <Table.Th>{t("registerTableForm.colHeaderColumn")}</Table.Th>
                  <Table.Th ta="center">{t("registerTableForm.colHeaderPk")}</Table.Th>
                  <Table.Th>{t("registerTableForm.colHeaderVisibleTo")}</Table.Th>
                  <Table.Th>{t("registerTableForm.colHeaderWritableBy")}</Table.Th>
                  <Table.Th>{t("registerTableForm.colHeaderMasking")}</Table.Th>
                  <Table.Th>{t("registerTableForm.colHeaderAlias")}</Table.Th>
                  <Table.Th>{t("registerTableForm.colHeaderDescription")}</Table.Th>
                  <Table.Th>{t("registerTableForm.colHeaderScope")}</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {columns.map((col, i) => (
                  <Fragment key={col.name}>
                    <Table.Tr>
                      <Table.Td>
                        <Checkbox
                          checked={col.selected}
                          onChange={(e) => updateCol(i, "selected", e.currentTarget.checked)}
                          aria-label={t("registerTableForm.includeColumnAriaLabel", {
                            name: col.name,
                          })}
                          data-testid={`register-table-col-selected-${col.name}`}
                        />
                      </Table.Td>
                      <Table.Td ff="monospace" fz="sm">
                        {col.name}
                      </Table.Td>
                      <Table.Td ta="center">
                        <Checkbox
                          checked={col.isPrimaryKey}
                          onChange={(e) => updateCol(i, "isPrimaryKey", e.currentTarget.checked)}
                          title={t("registerTableForm.primaryKeyTitle")}
                          aria-label={t("registerTableForm.primaryKeyAriaLabel", {
                            name: col.name,
                          })}
                          data-testid={`register-table-col-pk-${col.name}`}
                        />
                      </Table.Td>
                      <Table.Td>
                        <MultiSelect
                          options={roles.map((r) => ({ id: r.id, label: r.id }))}
                          value={col.visibleTo}
                          onChange={(selected) => updateCol(i, "visibleTo", selected)}
                          label={t("registerTableForm.colHeaderVisibleTo")}
                        />
                      </Table.Td>
                      <Table.Td>
                        <MultiSelect
                          options={roles.map((r) => ({ id: r.id, label: r.id }))}
                          value={col.writableBy}
                          onChange={(selected) => updateCol(i, "writableBy", selected)}
                          label={t("registerTableForm.colHeaderWritableBy")}
                        />
                      </Table.Td>
                      <Table.Td>
                        <Select
                          aria-label={t("registerTableForm.colHeaderMasking")}
                          data={[
                            { value: "", label: t("registerTableForm.maskNone") },
                            { value: "regex", label: t("registerTableForm.maskRegex") },
                            { value: "constant", label: t("registerTableForm.maskConstant") },
                            { value: "truncate", label: t("registerTableForm.maskTruncate") },
                          ]}
                          value={col.maskType}
                          onChange={(v) => updateCol(i, "maskType", v ?? "")}
                          allowDeselect={false}
                        />
                      </Table.Td>
                      <Table.Td>
                        <TextInput
                          aria-label={t("registerTableForm.colHeaderAlias")}
                          value={col.alias || ""}
                          onChange={(e) => updateCol(i, "alias", e.currentTarget.value)}
                        />
                      </Table.Td>
                      <Table.Td>
                        <TextInput
                          aria-label={t("registerTableForm.colHeaderDescription")}
                          value={col.description}
                          onChange={(e) => updateCol(i, "description", e.currentTarget.value)}
                          placeholder={t("registerTableForm.descriptionColPlaceholder")}
                        />
                      </Table.Td>
                      <Table.Td>
                        <Select
                          aria-label={t("registerTableForm.colHeaderScope")}
                          data={[
                            { value: "domain", label: t("registerTableForm.scopeDomain") },
                            { value: "public", label: t("registerTableForm.scopePublic") },
                            { value: "restricted", label: t("registerTableForm.scopeRestricted") },
                          ]}
                          value={col.scope}
                          onChange={(v) => updateCol(i, "scope", v ?? "domain")}
                          allowDeselect={false}
                        />
                      </Table.Td>
                    </Table.Tr>
                    {col.maskType && (
                      <Table.Tr>
                        <Table.Td></Table.Td>
                        <Table.Td c="dimmed" fz="xs">
                          {t("registerTableForm.maskingRowLabel")}
                        </Table.Td>
                        {col.maskType === "regex" && (
                          <Table.Td colSpan={3}>
                            <Stack gap={4}>
                              <TextInput
                                value={col.maskPattern}
                                onChange={(e) => updateCol(i, "maskPattern", e.currentTarget.value)}
                                placeholder={t("registerTableForm.maskPatternPlaceholder")}
                                aria-label={t("registerTableForm.maskPatternPlaceholder")}
                              />
                              <TextInput
                                value={col.maskReplace}
                                onChange={(e) => updateCol(i, "maskReplace", e.currentTarget.value)}
                                placeholder={t("registerTableForm.maskReplacePlaceholder")}
                                aria-label={t("registerTableForm.maskReplacePlaceholder")}
                              />
                            </Stack>
                          </Table.Td>
                        )}
                        {col.maskType === "constant" && (
                          <Table.Td colSpan={3}>
                            <TextInput
                              value={col.maskValue}
                              onChange={(e) => updateCol(i, "maskValue", e.currentTarget.value)}
                              placeholder={t("registerTableForm.maskValuePlaceholder")}
                              aria-label={t("registerTableForm.maskValuePlaceholder")}
                            />
                          </Table.Td>
                        )}
                        {col.maskType === "truncate" && (
                          <Table.Td colSpan={3}>
                            <Select
                              aria-label={t("registerTableForm.maskPrecisionPlaceholder")}
                              placeholder={t("registerTableForm.maskPrecisionPlaceholder")}
                              data={[
                                { value: "year", label: t("registerTableForm.precisionYear") },
                                { value: "month", label: t("registerTableForm.precisionMonth") },
                                { value: "day", label: t("registerTableForm.precisionDay") },
                                { value: "hour", label: t("registerTableForm.precisionHour") },
                              ]}
                              value={col.maskPrecision || null}
                              onChange={(v) => updateCol(i, "maskPrecision", v ?? "")}
                            />
                          </Table.Td>
                        )}
                        <Table.Td colSpan={4}>
                          <TextInput
                            value={col.unmaskedTo}
                            onChange={(e) => updateCol(i, "unmaskedTo", e.currentTarget.value)}
                            placeholder={t("registerTableForm.unmaskedToPlaceholder")}
                            aria-label={t("registerTableForm.unmaskedToPlaceholder")}
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
      </Stack>
      <Button onClick={handleSubmit} style={{ alignSelf: "flex-start" }} data-testid="register-table-submit">
        {t("registerTableForm.submitButton")}
      </Button>
    </Stack>
  );
}
