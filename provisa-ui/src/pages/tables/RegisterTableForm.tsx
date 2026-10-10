// Copyright (c) 2026 Kenneth Stott
// Canary: 5f8a2d14-7b3c-4e9f-a0d1-6c4e8b2f7a31
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useCallback, useRef } from "react";
import { ColumnsTable } from "./ColumnsTable";
import { FilesGlobFieldset } from "./FilesGlobFieldset";
import { useTranslation } from "react-i18next";
import { Button, Checkbox, Select, Stack, Table, Text, TextInput, Textarea } from "@mantine/core";
import { domainToSqlName, toSnakeCase } from "../../naming";
import { NlTableSearch } from "./NlTableSearch";
import { MultiSelect } from "../../components/MultiSelect";
import { useAvailableSchemas, useAvailableTables } from "../../hooks/useAdminQueries";
import { RegionSelect } from "../../components/admin/RegionSelect";
import { useRegionChoice, useRegionChoices } from "../../hooks/useRegionQueries";
import { useQueryPreview } from "../../hooks/useQueryPreview";
import { UniquesPanel } from "../../components/admin/UniquesPanel";
import { fetchIrTypes, fetchTableUniqueConstraints } from "../../api/admin";
import {
  fetchProfilerCatalog,
  type ProfilerCatalogEntry,
  type ProfilerRowRule,
} from "../../api/profiler";
import { useUpsertRlsRule } from "../../hooks/useSecurityQueries";
import { DQ_CHECKERS } from "../../types/admin";
import type { Paging, RegisteredTable, Source, UniqueConstraint } from "../../types/admin";
import type { Role } from "../../types/auth";
import type { ColumnForm } from "./types";
import { CDC_TYPES } from "./constants";
import { IR_TYPES_FALLBACK } from "../../irTypes";
import { isWatermarkEligible } from "./helpers";
import { DataQualityPanel } from "./DataQualityPanel";
import { useDomainFilter } from "../../context/DomainFilterContext";
import { PagingField } from "./PagingField";
import { declaredPaging, pagingInput, pagingProblem } from "./paging";

// REQ-1663: a checker table's results land under this schema, the same one the shipped demo uses
// (config/provisa-install.yaml `schema: quality`). It is the schema of record only when domains are
// off — with a domain picked, the domain names the schema exactly as for any other registration.
const DQ_RESULTS_SCHEMA = "quality";
// REQ-1934: a profiler's result relations live in the org's control-plane schema, which the server
// stamps on registration (as for ingest); the form carries this placeholder until then.
const PROFILER_RESULTS_SCHEMA = "default";
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
      nativeFilterRequired?: boolean | null;
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
  const { ensureDomainChecked } = useDomainFilter();
  const [schemaName, setSchemaName] = useState("");
  const [tableName, setTableName] = useState("");
  const [tableAlias, setTableAlias] = useState("");
  const [tableDescription, setTableDescription] = useState("");
  // REQ-318: the table's paging, starting from what its source suggests; the steward accepts
  // or edits it here, and registration stores it on the table.
  const [pagination, setPagination] = useState<Paging | null>(null);
  const [columns, setColumns] = useState<ColumnForm[]>([]);
  const [uniqueConstraints, setUniqueConstraints] = useState<UniqueConstraint[]>([]); // REQ-1093
  const [watermarkColumn, setWatermarkColumn] = useState<string>("");
  const [fileGlob, setFileGlob] = useState(""); // REQ-788
  const [sourceFileColumn, setSourceFileColumn] = useState(""); // REQ-788
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

  // REQ-1921: a new table starts in its source's region, else the region the operator is
  // connected to; the operator may change it or choose No region. No regions: no field, and the
  // table is registered with none.
  const regionChoices = useRegionChoices();
  const sourceRegion = sources.find((s) => s.id === sourceId)?.region ?? null;
  const [region, setRegion] = useRegionChoice(sourceRegion ?? regionChoices.connected, sourceId);
  const regionInput = regionChoices.regions.length > 0 ? { region } : {};

  const sourceType = sources.find((s) => s.id === sourceId)?.type?.toLowerCase() ?? "";
  const isChecker = (DQ_CHECKERS as readonly string[]).includes(sourceType);
  // REQ-1934: a Data Profiler source's tables are the result relations it produces per member;
  // its catalog lists them, so the source is never introspected.
  const isProfiler = sourceType === "data_profiler";
  const [profilerCatalog, setProfilerCatalog] = useState<ProfilerCatalogEntry[] | null>(null);
  // The row rules the picked result table is registered with, prefilled from the profiled table's
  // rules and editable here; saved after the table, as the table's own rules.
  const [profilerRowRules, setProfilerRowRules] = useState<ProfilerRowRule[]>([]);
  const { upsertRlsRule } = useUpsertRlsRule();
  const isSparql = sourceType === "sparql";
  const isFilesSource = ["files", "csv", "parquet"].includes(sourceType); // REQ-788
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
    sourceId && !isChecker && !isQueryApi && !isProfiler ? sourceId : null,
  );
  const isFixedSchema = availableSchemas.length === 1;
  const {
    tables: availableTables,
    loading: loadingTables,
    starting: startingTables,
    startingTimedOut: startingTablesTimedOut,
  } = useAvailableTables(
    sourceId && schemaName && !isChecker && !isQueryApi && !isProfiler ? sourceId : null,
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
    if (isProfiler) setSchemaName(PROFILER_RESULTS_SCHEMA);
    // REQ-1670: a neo4j table registers under the source's one schema, "neo4j".
    if (isQueryApi) setSchemaName(sourceType);
  }, [isChecker, isQueryApi, isProfiler, sourceType, sourceId]);

  useEffect(() => {
    if (!isProfiler || !sourceId) return;
    // eslint-disable-next-line react-hooks/set-state-in-effect -- cleared before the source's catalog loads
    setProfilerCatalog(null);
    fetchProfilerCatalog(sourceId)
      .then(setProfilerCatalog)
      .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  }, [isProfiler, sourceId, setError]);

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
    setPagination(meta?.pagination ?? null);
  }, [tableName, availableTables]);
  const offered = availableTables.find((tbl) => tbl.name === tableName);
  const pagingKind = offered?.pagingKind ?? null;
  const pagingCeilingRows = offered?.pagingCeilingRows ?? null;

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
    if (isProfiler && tableName) {
      // REQ-1934: the result relation's fields, granted like any table's columns; the server fixes
      // their types and the watermark.
      const picked = profilerCatalog
        ?.flatMap((entry) => entry.tables)
        .find((tb) => tb.tableName === tableName);
      setProfilerRowRules(picked?.rowRules ?? []);
      setColumns(
        (picked?.columns ?? []).map((c) => ({
          name: c.name,
          visibleTo: c.visibleTo,
          writableBy: [],
          unmaskedTo: c.unmaskedTo.join(", "),
          maskType: c.maskType ?? "",
          maskPattern: "",
          maskReplace: "",
          maskValue: "",
          maskPrecision: "",
          alias: "",
          description: c.description,
          selected: true,
          nativeFilterType: null,
          dataType: c.dataType,
          isPrimaryKey: false,
          scope: "domain",
          path: null,
        })),
      );
      return;
    }
    if (!sourceId || !schemaName || !tableName || isChecker || isQueryApi || isProfiler) return;
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
            nativeFilterRequired: c.nativeFilterRequired ?? null,
            dataType: c.dataType,
            isPrimaryKey: c.isPrimaryKey ?? false,
            // REQ-1959: nothing is public by default. A parameter column is an argument, not
            // data, and its scope publishes nothing whatever it says.
            scope: "domain",
            path: null, // REQ-1739: set below only for ingest sources
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
            path: null,
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
        nativeFilterRequired: c.nativeFilterRequired ?? undefined,
        isPrimaryKey: c.isPrimaryKey || undefined,
        scope: c.scope || "domain",
        path: c.path?.trim() || undefined, // REQ-1739: ingest per-column JSON extraction path
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
    if (pagingKind !== null && pagingProblem(pagingKind, pagination, pagingCeilingRows) !== null) {
      setError(t("tableEditForm.pagingFixErrors"));
      return;
    }
    try {
      const result = await registerTable({
        sourceId,
        domainId,
        ...regionInput,
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
        fileGlob: isFilesSource && fileGlob.trim() ? fileGlob.trim() : undefined, // REQ-788
        sourceFileColumn:
          isFilesSource && sourceFileColumn.trim() ? sourceFileColumn.trim() : undefined, // REQ-788
        // REQ-318: only what is declared; nothing declared registers the table with no paging.
        pagination:
          pagingKind !== null && declaredPaging(pagination) !== null
            ? pagingInput(pagination as Paging)
            : undefined,
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
      // A table registered into a domain the filter has not seen (one with no tables until now)
      // would otherwise stay hidden from the tables list until a reload.
      ensureDomainChecked(domainId);
      if (isProfiler) {
        for (const rule of profilerRowRules.filter((r) => r.filter.trim())) {
          const saved = await upsertRlsRule({
            tableId: tableName,
            roleId: rule.roleId,
            filterExpr: rule.filter.trim(),
          });
          if (!saved.success) {
            setError(
              t("registerTableForm.profilerRowRuleFailed", {
                role: rule.roleId,
                message: saved.message,
              }),
            );
            return;
          }
        }
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
        ...regionInput,
        schemaName: domainId ? domainToSqlName(domainId) : schemaName,
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
      // A table registered into a domain the filter has not seen (one with no tables until now)
      // would otherwise stay hidden from the tables list until a reload.
      ensureDomainChecked(domainId);
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
      {sourceId && (
        <RegionSelect
          value={region}
          onChange={setRegion}
          regions={regionChoices.regions}
          scope="table"
          testId="register-table-region-select"
        />
      )}
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
              contractText={dqContract}
              onChange={setDqContract}
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
      {isProfiler && (
        <label>
          {t("registerTableForm.profilerResultLabel")}
          <select
            value={tableName}
            onChange={(e) => setTableName(e.target.value)}
            disabled={profilerCatalog == null}
            data-testid="register-table-profiler-select"
          >
            <option value="">
              {profilerCatalog == null
                ? t("registerTableForm.schemaLoading")
                : profilerCatalog.length === 0
                  ? t("registerTableForm.profilerNoMembers")
                  : t("registerTableForm.profilerResultPlaceholder")}
            </option>
            {(profilerCatalog ?? []).flatMap((entry) =>
              entry.tables.map((tb) => (
                <option key={tb.tableName} value={tb.tableName}>
                  {t("registerTableForm.profilerResultOption", {
                    member: entry.member,
                    kind: tb.kind,
                  })}
                </option>
              )),
            )}
          </select>
        </label>
      )}
      {isProfiler && tableName && (
        <fieldset data-testid="register-table-profiler-row-rules">
          <legend>{t("registerTableForm.profilerRowRulesLabel")}</legend>
          {profilerRowRules.length === 0 && (
            <span>{t("registerTableForm.profilerRowRulesNone")}</span>
          )}
          {profilerRowRules.map((rule, i) => (
            <label key={rule.roleId}>
              {rule.roleId}
              <input
                value={rule.filter}
                onChange={(e) =>
                  setProfilerRowRules(
                    profilerRowRules.map((r, j) =>
                      j === i ? { ...r, filter: e.target.value } : r,
                    ),
                  )
                }
                data-testid={`register-table-profiler-row-rule-${rule.roleId}`}
              />
            </label>
          ))}
        </fieldset>
      )}
      {!isChecker && !isQueryApi && !isProfiler && (
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
                {startingTables
                  ? t("registerTableForm.tableStarting")
                  : loadingTables
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
            {startingTablesTimedOut && (
              <Text size="xs" c="red" mt={4} data-testid="register-table-starting-timeout">
                {t("registerTableForm.tableStartingTimedOut")}
              </Text>
            )}
          </label>
          {sourceId && schemaName && !allTablesRegistered && (
            <NlTableSearch
              sourceId={sourceId}
              schemaName={schemaName}
              isRegistered={isRegistered}
              onPick={setTableName}
            />
          )}
        </>
      )}
      <TextInput
        data-testid="register-table-alias"
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
      {pagingKind !== null && (
        <PagingField
          kind={pagingKind}
          paging={pagination}
          onChange={setPagination}
          ceilingRows={pagingCeilingRows}
          rowsFieldEditable
        />
      )}
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
      {isFilesSource && (
        <FilesGlobFieldset
          fileGlob={fileGlob}
          setFileGlob={setFileGlob}
          sourceFileColumn={sourceFileColumn}
          setSourceFileColumn={setSourceFileColumn}
        />
      )}
      {!isChecker && (
        <Stack gap="xs" style={{ gridColumn: "1 / -1" }}>
          <Text fw={600} fz="sm">
            {t("registerTableForm.columnsLabel")}{" "}
            {loadingColumns && t("registerTableForm.columnsLoading")}
          </Text>
          {columns.length > 0 && (
            <ColumnsTable
              columns={columns}
              updateCol={updateCol}
              roles={roles}
              irTypes={irTypes}
              sourceType={sourceType}
            />
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
