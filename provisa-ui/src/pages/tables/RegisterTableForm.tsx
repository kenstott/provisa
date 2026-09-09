// Copyright (c) 2026 Kenneth Stott
// Canary: 5f8a2d14-7b3c-4e9f-a0d1-6c4e8b2f7a31
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useCallback, useRef, Fragment } from "react";
import { useTranslation } from "react-i18next";
import { Button, Checkbox, Select, Stack, Table, Text, TextInput, Textarea } from "@mantine/core";
import { toSnakeCase } from "../../naming";
import { MultiSelect } from "../../components/MultiSelect";
import { useAvailableSchemas, useAvailableTables } from "../../hooks/useAdminQueries";
import { useQueryPreview } from "../../hooks/useQueryPreview";
import { UniquesPanel } from "../../components/admin/UniquesPanel";
import { fetchIrTypes, fetchTableUniqueConstraints } from "../../api/admin";
import { DQ_CHECKERS } from "../../types/admin";
import type { RegisteredTable, Source, UniqueConstraint } from "../../types/admin";
import type { Role } from "../../types/auth";
import type { ColumnForm } from "./types";
import { CDC_TYPES } from "./constants";
import { IR_TYPES_FALLBACK, toIrType } from "../../irTypes";
import { isWatermarkEligible, normalizeDomain } from "./helpers";
import { DataQualityPanel } from "./DataQualityPanel";

// REQ-1663: a checker table's results land under this schema, the same one the shipped demo uses
// (config/provisa-install.yaml `schema: quality`). It is the schema of record only when domains are
// off — with a domain picked, the domain names the schema exactly as for any other registration.
const DQ_RESULTS_SCHEMA = "quality";
// REQ-1670/REQ-1683: a query-API source lists one schema, named after its type.
const QUERY_API_TYPES = ["neo4j", "sparql"] as const;
// REQ-1443: the results envelope replaces whatever columns are declared; the one declared column
// exists to carry visible_to, exactly as the YAML demo registers it.
const DQ_PLACEHOLDER_COLUMN = { name: "scan_id", dataType: "varchar" };

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
  const [uniqueConstraints, setUniqueConstraints] = useState<UniqueConstraint[]>([]); // REQ-1093
  const [watermarkColumn, setWatermarkColumn] = useState<string>("");
  const [discover, setDiscover] = useState(false); // REQ-252: infer columns from the live source
  const [loadingColumns, setLoadingColumns] = useState(false);
  // REQ-1663: a data-quality checker source has no remote schema to pick from — its table's rows
  // are one contract's scan results. The form asks for the governed table to scan (through the
  // contract panel) and derives the rest: results table name, alias, description, columns.
  const [dqContract, setDqContract] = useState("");
  const [dqScanned, setDqScanned] = useState<{ schema: string; table: string } | null>(null);
  const [dqVisibleTo, setDqVisibleTo] = useState<string[]>([]);
  // REQ-1670: a neo4j source has no tables to list — its table IS a Cypher projection. The form
  // takes the Cypher, previews it (rows + inferred column types), and registers the named table.
  const [cypher, setCypher] = useState("");
  const [previewRows, setPreviewRows] = useState<Record<string, unknown>[]>([]);
  const [previewing, setPreviewing] = useState(false);
  const { preview: previewQuery } = useQueryPreview();

  const sourceType = sources.find((s) => s.id === sourceId)?.type?.toLowerCase() ?? "";
  const isChecker = (DQ_CHECKERS as readonly string[]).includes(sourceType);
  const isSparql = sourceType === "sparql";
  // REQ-1670/REQ-1683: a query-API source has no tables to list — its table IS a query projection.
  const isQueryApi = (QUERY_API_TYPES as readonly string[]).includes(sourceType);

  // REQ-846/REQ-1426: the canonical IR type vocabulary a steward picks from. Registration is the
  // last point at which a type can be assigned — nothing infers one afterwards — so the list comes
  // from the backend (provisa/core/ir_types.py) with a static fallback if that fetch fails.
  const [irTypes, setIrTypes] = useState<string[]>(IR_TYPES_FALLBACK);
  useEffect(() => {
    fetchIrTypes()
      .then((types) => {
        if (types.length > 0) setIrTypes(types);
      })
      .catch(() => setIrTypes(IR_TYPES_FALLBACK));
  }, []);

  // A checker source is never introspected: nothing exists upstream until a scan runs, so the
  // schema/table lookups are not made for it (REQ-1663).
  const { schemas: availableSchemas, loading: loadingSchemas } = useAvailableSchemas(
    sourceId && !isChecker && !isQueryApi ? sourceId : null,
  );
  const isFixedSchema = availableSchemas.length === 1;
  const { tables: availableTables, loading: loadingTables } = useAvailableTables(
    sourceId && schemaName && !isChecker && !isQueryApi ? sourceId : null,
    schemaName || null,
  );

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- form cascade reset: dependent fields cleared when source selection changes
    setSchemaName("");
    setTableName("");
    setTableDescription("");
    setColumns([]);
    setDqContract("");
    setDqScanned(null);
    setDqVisibleTo(roles.map((r) => r.id));
    setCypher("");
    setPreviewRows([]);
  }, [sourceId, roles]);

  // REQ-1663: declared after the cascade reset so it lands on the same commit and wins — the schema
  // is fixed for a checker table, not picked.
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- the results schema is a constant of the checker registration, set once the source is known to be a checker
    if (isChecker) setSchemaName(DQ_RESULTS_SCHEMA);
    // REQ-1670: a neo4j table registers under the source's one schema, "neo4j".
    if (isQueryApi) setSchemaName(sourceType);
  }, [isChecker, isQueryApi, sourceType, sourceId]);

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
    setUniqueConstraints([]);
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
    // REQ-1663: a results table's semantic name says what it is — the scanned table's quality —
    // as the shipped demo names it (pets → pets_quality).
    if (isChecker && dqScanned !== null) {
      setTableAlias(`${dqScanned.table}_quality`);
      return;
    }
    suggestTableAlias(tableName, domainId, sourceId).then(setTableAlias);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- suggestTableAlias is a stable hook callback; re-run only when the table selection changes
  }, [tableName, domainId, sourceId, isChecker, dqScanned]);

  // REQ-1663: the contract names what it scans; the results table is named after it. A change of
  // scanned table re-derives the name and description, which stay editable afterwards.
  // Every contract edit (a rule added, an arg changed) re-parses and reports the dataset again;
  // only a CHANGE of dataset re-derives, so a name the operator typed over the default survives
  // authoring the rules.
  const lastDqDataset = useRef<string | null>(null);
  const onDqDatasetChange = useCallback(
    (dataset: string | null) => {
      if (dataset === lastDqDataset.current) return;
      lastDqDataset.current = dataset;
      if (dataset === null) {
        setDqScanned(null);
        return;
      }
      const [, schema, table] = dataset.split("/");
      setDqScanned({ schema, table });
      setTableName(`${table}_scan`);
      setTableDescription(t("registerTableForm.dqDescription", { table: `${schema}.${table}` }));
    },
    [t],
  );

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- cascade reset: columns cleared before async fetch when table selection changes
    setColumns([]);
    setUniqueConstraints([]);
    setWatermarkColumn("");
    if (!sourceId || !schemaName || !tableName || isChecker || isQueryApi) return;
    // REQ-1093: seed the Uniques panel from the source's declared UNIQUE constraints.
    fetchTableUniqueConstraints(sourceId, schemaName, tableName)
      .then(setUniqueConstraints)
      .catch(() => setUniqueConstraints([]));
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

  // REQ-1670: run the Cypher (LIMIT 5); the columns and their types come from what it returns.
  const handleNeo4jPreview = async () => {
    setError(null);
    if (!sourceId || !cypher.trim()) {
      setError(t("registerTableForm.errorNeo4jCypher"));
      return;
    }
    setPreviewing(true);
    try {
      const res = await previewQuery({
        sourceType: isSparql ? "sparql" : "neo4j",
        sourceId,
        query: cypher.trim(),
      });
      if (res.error) {
        setError(res.error);
        setPreviewRows([]);
        setColumns([]);
        return;
      }
      if (res.columns.length === 0) {
        setError(t("registerTableForm.neo4jPreviewNoRows"));
        setPreviewRows([]);
        setColumns([]);
        return;
      }
      setPreviewRows(res.rows);
      setColumns(
        res.columns.map((c) => {
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
            description: "",
            selected: true,
            nativeFilterType: null,
            dataType: c.dataType,
            isPrimaryKey: false,
            scope: "domain",
          };
        }),
      );
    } finally {
      setPreviewing(false);
    }
  };

  const updateCol = (i: number, key: keyof ColumnForm, value: string | boolean | string[]) => {
    const next = [...columns];
    next[i] = { ...next[i], [key]: value };
    setColumns(next);
  };

  const handleSubmit = async () => {
    setError(null);
    if (isChecker) {
      await submitChecker();
      return;
    }
    const selectedCols = columns
      .filter((c) => c.selected)
      .map((c) => ({
        name: c.name,
        dataType: c.dataType,
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
    if (isQueryApi) {
      if (!cypher.trim()) {
        setError(t("registerTableForm.errorNeo4jCypher"));
        return;
      }
      if (columns.length === 0) {
        setError(t("registerTableForm.errorNeo4jPreviewFirst"));
        return;
      }
    }
    // REQ-252: with discover on, columns are inferred from the live source, so none need be selected.
    if (!discover && selectedCols.length === 0) {
      setError(t("registerTableForm.errorNoColumnsSelected"));
      return;
    }
    // REQ-1426: a design is not complete until every column carries a data type. Discovery types
    // what the source can describe; whatever it could not type the steward assigns here, because
    // nothing infers a type after registration.
    const untyped = selectedCols.filter((c) => !c.dataType).map((c) => c.name);
    if (untyped.length > 0) {
      setError(t("registerTableForm.errorUntypedColumns", { columns: untyped.join(", ") }));
      return;
    }
    try {
      const result = await registerTable({
        sourceId,
        domainId,
        // REQ-1673: the PHYSICAL schema the table was picked from (or the source's fixed schema),
        // never the domain. The domain is `domainId`; registering the domain as the schema lost
        // the physical location of every source whose schema is not named after the domain —
        // the engine then attached "pet_store"."product_reviews" on a Mongo database called
        // "provisa" and every query failed with "schema does not exist".
        schemaName,
        tableName,
        alias: tableAlias || undefined,
        description: tableDescription || undefined,
        watermarkColumn: isQueryApi ? null : watermarkColumn || null,
        discover: isQueryApi ? false : discover, // REQ-252
        queryTemplate: isQueryApi ? cypher.trim() : undefined, // REQ-1670/REQ-1683
        columns: selectedCols,
        // REQ-1093: drop empty/incomplete rows — a constraint needs a name and >=1 column.
        uniqueConstraints: uniqueConstraints
          .filter((u) => u.name.trim() && u.columns.length > 0)
          .map((u) => ({ name: u.name.trim(), columns: u.columns })),
      });
      if (!result.success) {
        setError(result.message);
        return;
      }
      resetForm();
      onSuccess();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const resetForm = () => {
    setSourceId("");
    setDomainId("");
    setSchemaName("");
    setTableName("");
    setTableAlias("");
    setTableDescription("");
    setColumns([]);
    setUniqueConstraints([]);
    setCypher("");
    setPreviewRows([]);
    setWatermarkColumn("");
    setDiscover(false);
    setDqContract("");
    setDqScanned(null);
  };

  // REQ-1663: a checker registration is the contract plus a governance intent (visible_to); every
  // other input the server derives from the checker's fixed envelope (provisa.dq.registration).
  const submitChecker = async () => {
    if (!sourceId || !tableName) {
      setError(t("registerTableForm.errorRequiredFields"));
      return;
    }
    if (dqScanned === null || dqContract.trim() === "") {
      setError(t("registerTableForm.errorDqContract"));
      return;
    }
    if (dqVisibleTo.length === 0) {
      setError(t("registerTableForm.errorDqVisibleTo"));
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
        watermarkColumn: null,
        discover: false,
        columns: [
          {
            ...DQ_PLACEHOLDER_COLUMN,
            visibleTo: dqVisibleTo,
            writableBy: [],
            unmaskedTo: [],
            scope: "domain",
          },
        ],
        uniqueConstraints: [],
        dqContract,
      });
      if (!result.success) {
        setError(result.message);
        return;
      }
      resetForm();
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
    !loadingTables &&
    !!schemaName &&
    availableTables.length > 0 &&
    availableTables.every(isRegistered);

  const isCdcSource = CDC_TYPES.has(sourceType);

  return (
    <div data-tour="tables-form" className="form-card">
      <label>
        {t("registerTableForm.sourceLabel")}
        <select
          value={sourceId}
          onChange={(e) => setSourceId(e.target.value)}
          data-testid="register-table-source-select"
        >
          <option value="">{t("registerTableForm.sourcePlaceholder")}</option>
          {sources
            .filter(
              (s) =>
                s.allowedDomains.length === 0 ||
                s.allowedDomains.some((d) => checkedDomains.has(d)),
            )
            .map((s) => (
              <option key={s.id} value={s.id}>
                {s.id}
              </option>
            ))}
        </select>
      </label>
      {domainsEnabled && (
        <label>
          {t("registerTableForm.domainLabel")}
          <select
            value={domainId}
            onChange={(e) => setDomainId(e.target.value)}
            data-testid="register-table-domain-select"
          >
            <option value="">{t("registerTableForm.domainPlaceholder")}</option>
            {domainHints
              .filter((d) => d !== "" && d !== "meta" && d !== "ops")
              .filter((d) => domainAccess.includes("*") || domainAccess.includes(d))
              .map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
          </select>
        </label>
      )}
      {isChecker && (
        <>
          {/* REQ-1663: the contract panel IS the table picker for a checker source — its dataset
              select names the governed table to scan, and the rules are authored right here. */}
          <div style={{ gridColumn: "1 / -1" }} data-testid="register-table-dq">
            <DataQualityPanel
              checker={sourceType}
              sourceId={sourceId}
              schemaName={domainId ? normalizeDomain(domainId) : schemaName}
              tableName={tableName}
              contractText={dqContract}
              onChange={setDqContract}
              registered={false}
              onDatasetChange={onDqDatasetChange}
            />
          </div>
          <TextInput
            label={
              <>
                {t("registerTableForm.dqResultsTableLabel")}{" "}
                <Text span fw="normal" c="dimmed" fz="xs">
                  {t("registerTableForm.dqResultsTableHint")}
                </Text>
              </>
            }
            value={tableName}
            onChange={(e) => setTableName(e.currentTarget.value)}
            placeholder={t("registerTableForm.dqResultsTablePlaceholder")}
            data-testid="register-table-dq-results-table"
          />
          <MultiSelect
            options={roles.map((r) => ({ id: r.id, label: r.id }))}
            value={dqVisibleTo}
            onChange={setDqVisibleTo}
            label={t("registerTableForm.dqVisibleToLabel")}
          />
        </>
      )}
      {isQueryApi && (
        <>
          <TextInput
            required
            label={t("registerTableForm.neo4jTableNameLabel")}
            value={tableName}
            onChange={(e) => setTableName(e.currentTarget.value)}
            placeholder={t("registerTableForm.neo4jTableNamePlaceholder")}
            data-testid={`register-table-${sourceType}-table-name`}
          />
          <Textarea
            required
            style={{ gridColumn: "1 / -1" }}
            autosize
            minRows={3}
            label={
              <>
                {t(
                  isSparql
                    ? "registerTableForm.sparqlQueryLabel"
                    : "registerTableForm.neo4jCypherLabel",
                )}{" "}
                <Text span fw="normal" c="dimmed" fz="xs">
                  {t(
                    isSparql
                      ? "registerTableForm.sparqlQueryHint"
                      : "registerTableForm.neo4jCypherHint",
                  )}
                </Text>
              </>
            }
            value={cypher}
            onChange={(e) => setCypher(e.currentTarget.value)}
            placeholder={t(
              isSparql
                ? "registerTableForm.sparqlQueryPlaceholder"
                : "registerTableForm.neo4jCypherPlaceholder",
            )}
            data-testid={isSparql ? "register-table-sparql-query" : "register-table-neo4j-cypher"}
          />
          <Button
            variant="default"
            onClick={handleNeo4jPreview}
            disabled={!sourceId || !cypher.trim() || previewing}
            data-testid={`register-table-${sourceType}-preview`}
          >
            {previewing
              ? t("registerTableForm.neo4jPreviewing")
              : t("registerTableForm.neo4jPreviewButton")}
          </Button>
          {previewRows.length > 0 && (
            <Stack gap="xs" style={{ gridColumn: "1 / -1" }}>
              <Text fw={600} fz="sm">
                {t("registerTableForm.neo4jPreviewRows", { count: previewRows.length })}
              </Text>
              <Table.ScrollContainer minWidth={400}>
                <Table striped withTableBorder verticalSpacing="xs" fz="xs">
                  <Table.Thead>
                    <Table.Tr>
                      {columns.map((c) => (
                        <Table.Th key={c.name}>{c.name}</Table.Th>
                      ))}
                    </Table.Tr>
                  </Table.Thead>
                  <Table.Tbody data-testid={`register-table-${sourceType}-preview-rows`}>
                    {previewRows.map((row, i) => (
                      <Table.Tr key={i}>
                        {columns.map((c) => (
                          <Table.Td key={c.name}>{String(row[c.name] ?? "")}</Table.Td>
                        ))}
                      </Table.Tr>
                    ))}
                  </Table.Tbody>
                </Table>
              </Table.ScrollContainer>
            </Stack>
          )}
        </>
      )}
      {!isChecker && !isQueryApi && (
        <>
          <label>
            {t("registerTableForm.schemaLabel")}
            <select
              value={schemaName}
              onChange={(e) => setSchemaName(e.target.value)}
              disabled={!sourceId || loadingSchemas || isFixedSchema}
              data-testid="register-table-schema-select"
            >
              <option value="">
                {loadingSchemas
                  ? t("registerTableForm.schemaLoading")
                  : t("registerTableForm.schemaPlaceholder")}
              </option>
              {availableSchemas.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </label>
          <label>
            {t("registerTableForm.tableLabel")}
            <select
              value={tableName}
              onChange={(e) => setTableName(e.target.value)}
              disabled={!schemaName || loadingTables || allTablesRegistered}
              data-testid="register-table-table-select"
            >
              <option value="">
                {loadingTables
                  ? t("registerTableForm.tableLoading")
                  : allTablesRegistered
                    ? t("registerTableForm.tableAllRegistered")
                    : t("registerTableForm.tablePlaceholder")}
              </option>
              {availableTables.map((tbl) => (
                <option key={tbl.name} value={tbl.name} disabled={isRegistered(tbl)}>
                  {tbl.name}
                </option>
              ))}
            </select>
          </label>
        </>
      )}
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
      {!isChecker && !isQueryApi && (
        <Checkbox
          checked={discover}
          onChange={(e) => setDiscover(e.currentTarget.checked)}
          data-testid="discover-columns-checkbox"
          label={
            <>
              {t("registerTableForm.discoverLabel")}{" "}
              <Text span fw="normal" c="dimmed" fz="xs">
                {t("registerTableForm.discoverHint")}
              </Text>
            </>
          }
        />
      )}
      {sourceId && !isChecker && !isQueryApi && (
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
      {!isChecker && (
        <Stack gap="xs" style={{ gridColumn: "1 / -1" }}>
          <Text fw={600} fz="sm">
            {t("registerTableForm.columnsLabel")}{" "}
            {loadingColumns && t("registerTableForm.columnsLoading")}
          </Text>
          {columns.length > 0 && (
            <Table.ScrollContainer minWidth={1050}>
              <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th></Table.Th>
                    <Table.Th>{t("registerTableForm.colHeaderColumn")}</Table.Th>
                    <Table.Th>{t("registerTableForm.colHeaderDataType")}</Table.Th>
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
                        <Table.Td>
                          <Select
                            aria-label={t("registerTableForm.colHeaderDataType")}
                            placeholder={t("registerTableForm.dataTypePlaceholder")}
                            data={Array.from(
                              new Set([
                                ...(col.dataType ? [toIrType(col.dataType)] : []),
                                ...irTypes,
                              ]),
                            )}
                            value={col.dataType ? toIrType(col.dataType) : null}
                            onChange={(v) => updateCol(i, "dataType", v ?? "")}
                            error={col.selected && !col.dataType}
                            searchable
                            allowDeselect={false}
                            data-testid={`register-table-col-datatype-${col.name}`}
                          />
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
                              {
                                value: "restricted",
                                label: t("registerTableForm.scopeRestricted"),
                              },
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
                                  onChange={(e) =>
                                    updateCol(i, "maskPattern", e.currentTarget.value)
                                  }
                                  placeholder={t("registerTableForm.maskPatternPlaceholder")}
                                  aria-label={t("registerTableForm.maskPatternPlaceholder")}
                                />
                                <TextInput
                                  value={col.maskReplace}
                                  onChange={(e) =>
                                    updateCol(i, "maskReplace", e.currentTarget.value)
                                  }
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
                          <Table.Td colSpan={5}>
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
      )}
      {columns.length > 0 && (
        <div style={{ gridColumn: "1 / -1" }}>
          <UniquesPanel
            uniques={uniqueConstraints}
            columns={columns.map((c) => c.name)}
            onChange={setUniqueConstraints}
          />
        </div>
      )}
      <Button
        onClick={handleSubmit}
        style={{ gridColumn: "1 / -1", alignSelf: "flex-start" }}
        data-testid="register-table-submit"
      >
        {t("registerTableForm.submitButton")}
      </Button>
    </div>
  );
}
