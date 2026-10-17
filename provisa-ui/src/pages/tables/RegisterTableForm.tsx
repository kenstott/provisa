// Copyright (c) 2026 Kenneth Stott
// Canary: 5f8a2d14-7b3c-4e9f-a0d1-6c4e8b2f7a31
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { MaskEntry } from "../../lib/masks";
import { NEVER_EXPIRES, cacheTtlHint } from "../../components/form/cacheTtl";
import { useState, useEffect, useCallback, useRef } from "react";
import { UntypedColumns } from "./UntypedColumns";
import { ProfilerRowRulesField } from "./ProfilerRowRulesField";
import { QueryTableFields } from "./QueryTableFields";
import {
  argumentName,
  isPipelineArgument,
  NATIVE_QUERY_MODES,
  previewedColumn,
} from "./queryTables";
import { QDRANT, QDRANT_SCHEMA, offeredUnselected, searchQueryArgument } from "../sources/qdrant";
import { QdrantVectorsNote } from "./QdrantVectorsNote";
import { RegisterColumnsField } from "./RegisterColumnsField";
import { QdrantVectorColumns } from "./QdrantVectorColumns";
import { FilesGlobFieldset } from "./FilesGlobFieldset";
import { isFilesSourceType } from "./tableSectionFields";
import { useTranslation } from "react-i18next";
import { Checkbox, SegmentedControl, Select, Text, TextInput, NumberInput } from "@mantine/core";
import { domainToSqlName, toSnakeCase } from "../../naming";
import { NlTableSearch } from "./NlTableSearch";
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
import { registeredTable } from "./registeredTable";
import type {
  MutationResult,
  Paging,
  RegisteredTable,
  Source,
  UniqueConstraint,
} from "../../types/admin";
import type { Role } from "../../types/auth";
import type { ColumnForm } from "./types";
import { CDC_TYPES } from "./constants";
import { IR_TYPES_FALLBACK } from "../../irTypes";
import { isWatermarkEligible } from "./helpers";
import { CheckerFields } from "./CheckerFields";
import { useDomainFilter } from "../../context/DomainFilterContext";
import { PagingField } from "./PagingField";
import { declaredPaging, pagingInput, pagingProblem } from "./paging";
import {
  FormChecks,
  FormField,
  FormFooter,
  FormIdentifiers,
  FormLayout,
} from "../../components/form/FormLayout";
import { useSourceKind } from "../sources/sourceKind";
import { ttlSignalMissingCacheTtl } from "../sources/loadManagement";
import { serverMessage } from "../../i18n/serverMessage";
import { usePanelDirty } from "../../components/list/panelLeave";
import { TableOptions } from "./TableOptions";
import { DeclaredColumns } from "./DeclaredColumns";
import { ImportSetField } from "./ImportSetField";

// REQ-1443: a checker's scans append to a relation in the org's control-plane schema, which the
// server stamps on registration (as for a profiler's results); the form carries this placeholder.
const DQ_RESULTS_SCHEMA = "default";
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
      path?: string | null;
    }[]
  >;
  suggestTableAlias: (tableName: string, domainId: string, sourceId: string) => Promise<string>;
  registerTable: (input: Record<string, unknown>) => Promise<MutationResult>;
  /**
   * The registered table, by the id the server gave it -- not by the address the form sent, which
   * the server may store otherwise (an ingest table's schema is the org's control-plane schema).
   * Null when nothing is registered yet (a request queued for approval).
   */
  onSuccess: (registered: { id: number } | null) => void;
  /** Closes the form without registering anything. */
  onCancel: () => void;
  setError: (e: string | null) => void;
  /** What the page has to say of a registration that did not go through. */
  problem?: string | null;
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
  onCancel,
  setError,
  problem,
}: RegisterTableFormProps) {
  const { t } = useTranslation();
  const [sourceId, setSourceId] = useState("");
  const [chosenDomain, setDomainId] = useState("");
  const { ensureDomainChecked } = useDomainFilter();
  const [schemaName, setSchemaName] = useState("");
  const [tableName, setTableName] = useState("");
  // Leaving the panel once something is chosen asks first (the panel's one mechanism).
  usePanelDirty(Boolean(sourceId || schemaName || tableName), tableName || schemaName || sourceId);
  const [tableAlias, setTableAlias] = useState("");
  const [tableDescription, setTableDescription] = useState("");
  // REQ-318: the table's paging, starting from what its source suggests; the steward accepts
  // or edits it here, and registration stores it on the table.
  const [pagination, setPagination] = useState<Paging | null>(null);
  const [columns, setColumns] = useState<ColumnForm[]>([]);
  const [uniqueConstraints, setUniqueConstraints] = useState<UniqueConstraint[]>([]); // REQ-1093
  const [watermarkColumn, setWatermarkColumn] = useState<string>("");
  // REQ-1907 (#208): the table's own Cache TTL, asked for when the table will be a replica and
  // its source states none -- without one the registration is refused.
  const [cacheTtl, setCacheTtl] = useState<string>("");
  const [fileGlob, setFileGlob] = useState(""); // REQ-788
  const [sourceFileColumn, setSourceFileColumn] = useState(""); // REQ-788
  const [importSet, setImportSet] = useState<string | null>(null); // REQ-1954
  const [discover, setDiscover] = useState(false); // REQ-252: infer columns from the live source
  const [loadingColumns, setLoadingColumns] = useState(false);
  // The columns discovery gave no type: the short step asks a type for each.
  const [untypedAtDiscovery, setUntypedAtDiscovery] = useState<string[]>([]);
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
  // REQ-1980: a MongoDB table is a collection, or a pipeline with arguments run on each read.
  const [mongoMode, setMongoMode] = useState<"collection" | "pipeline">("collection");
  const [samples, setSamples] = useState<Record<string, string>>({});

  // REQ-1921: a new table starts in its source's region, else the region the operator is
  // connected to; the operator may change it or choose No region. No regions: no field, and the
  // table is registered with none.
  const regionChoices = useRegionChoices();
  const sourceRegion = sources.find((s) => s.id === sourceId)?.region ?? null;
  const [region, setRegion] = useRegionChoice(sourceRegion ?? regionChoices.connected, sourceId);
  const regionInput = regionChoices.regions.length > 0 ? { region } : {};

  const sourceType = sources.find((s) => s.id === sourceId)?.type?.toLowerCase() ?? "";
  const isChecker = (DQ_CHECKERS as readonly string[]).includes(sourceType);
  const chosenSource = sources.find((s) => s.id === sourceId);
  // The server says whether a table of this kind is served from a replica under the engine
  // bound here; then a ttl signal with no Cache TTL on the source needs one on the table.
  const needsCacheTtl =
    useSourceKind(sourceType)?.tables_land === true &&
    chosenSource !== undefined &&
    ttlSignalMissingCacheTtl(chosenSource.changeSignal, chosenSource.cacheTtl, true);
  // REQ-1934: a Data Profiler source's tables are the result relations it produces per member;
  // its catalog lists them, so the source is never introspected.
  const isProfiler = sourceType === "data_profiler";
  const [profilerCatalog, setProfilerCatalog] = useState<ProfilerCatalogEntry[] | null>(null);
  // The row rules the picked result table is registered with, prefilled from the profiled table's
  // rules and editable here; saved after the table, as the table's own rules.
  const [profilerRowRules, setProfilerRowRules] = useState<ProfilerRowRule[]>([]);
  const { upsertRlsRule } = useUpsertRlsRule();
  const isSparql = sourceType === "sparql";
  const isFilesSource = isFilesSourceType(sourceType); // REQ-788
  // REQ-1670/REQ-1683: a query-API source has no tables to list — its table IS a query projection.
  // REQ-1980/REQ-1982: a document source's table is a collection or container, or a native query.
  // REQ-1983: a Qdrant table is a collection, or a search. REQ-2017: an Elasticsearch table is an
  // index, or a native search.
  const isNativeQuerySource =
    sourceType === "mongodb" ||
    sourceType === "cosmosdb" ||
    sourceType === QDRANT ||
    sourceType === "elasticsearch";
  const isPipeline = isNativeQuerySource && mongoMode === "pipeline";
  const isQueryApi = (QUERY_API_TYPES as readonly string[]).includes(sourceType) || isPipeline;
  const pipelineArgs = columns.filter(isPipelineArgument);
  // The short step: what names the table, and only what the server will not do without. The rest
  // is the edit form's, which opens on the registered draft. A query API's table (its query and
  // preview), a checker's (its contract) and a profiler's results take the longer path: the same
  // form, more of it shown.
  const short = !isChecker && !isQueryApi && !isProfiler;
  // The domains the caller may register in. With one, it is the domain and is not asked.
  const offeredDomains = domainHints
    .filter((d) => d !== "" && d !== "meta" && d !== "ops")
    .filter((d) => domainAccess.includes("*") || domainAccess.includes(d));
  const domainAsked = domainsEnabled && (!short || offeredDomains.length !== 1);
  const domainId =
    chosenDomain ||
    (domainsEnabled && short && offeredDomains.length === 1 ? offeredDomains[0] : "");

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
    // REQ-1670: a neo4j table registers under the source's one schema, "neo4j"; a pipeline table
    // (REQ-1980) under its source's database.
    // A Qdrant search under its source's one schema (REQ-1983).
    if (isQueryApi)
      setSchemaName(
        isPipeline
          ? sourceType === QDRANT || sourceType === "elasticsearch"
            ? QDRANT_SCHEMA // the source's one schema, "default"
            : (chosenSource?.database ?? "")
          : sourceType,
      );
  }, [isChecker, isQueryApi, isPipeline, isProfiler, sourceType, sourceId, chosenSource?.database]);

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
      setTableName(`${table}_quality`);
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
          masks: c.masks,
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
            masks: [],
            alias: snake !== c.name ? snake : "",
            description: c.comment || "",
            // REQ-1983: a Qdrant collection's vectors are offered, left unselected.
            selected: !offeredUnselected(sourceType, c.path),
            nativeFilterType: c.nativeFilterType ?? null,
            nativeFilterRequired: c.nativeFilterRequired ?? null,
            dataType: c.dataType,
            isPrimaryKey: c.isPrimaryKey ?? false,
            // REQ-1959: nothing is public by default. A parameter column is an argument, not
            // data, and its scope publishes nothing whatever it says.
            scope: "domain",
            // REQ-1982: a document source says where a nested field is read from; REQ-1739: an
            // ingest source's path is set below.
            path: c.path ?? null,
          };
        });
        setColumns(formed);
        setUntypedAtDiscovery(formed.filter((c) => !c.dataType).map((c) => c.name));
        const pickedType = sources.find((s) => s.id === sourceId)?.type ?? "";
        if (!CDC_TYPES.has(pickedType)) {
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
        sourceType: isPipeline
          ? (sourceType as "mongodb" | "cosmosdb" | "qdrant" | "elasticsearch")
          : isSparql
            ? "sparql"
            : "neo4j",
        sourceId,
        query: cypher.trim(),
        ...(isPipeline && {
          arguments: pipelineArgs.map((a) => {
            const name = a.name.replace(/^_nf_/, "");
            return { name, dataType: a.dataType, value: samples[name] || null };
          }),
        }),
      });
      if (res.error) {
        setError(res.error);
        setPreviewRows([]);
        setColumns(pipelineArgs); // a pipeline's arguments are kept
        return;
      }
      if (res.columns.length === 0) {
        setError(t("registerTableForm.neo4jPreviewNoRows"));
        setPreviewRows([]);
        setColumns(pipelineArgs); // a pipeline's arguments are kept
        return;
      }
      setPreviewRows(res.rows);
      const roleIds = roles.map((r) => r.id);
      setColumns([...pipelineArgs, ...res.columns.map((c) => previewedColumn(c, roleIds))]);
    } finally {
      setPreviewing(false);
    }
  };

  const updateCol = (
    i: number,
    key: keyof ColumnForm,
    value: string | boolean | string[] | MaskEntry[],
  ) => {
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
    // REQ-1983: a Qdrant search's query is the one argument Qdrant requires.
    const searchQuery =
      isPipeline && sourceType === QDRANT ? searchQueryArgument(cypher.trim()) : null;
    const selectedCols = columns
      .filter((c) => c.selected)
      .map((c) => ({
        name: c.name,
        dataType: c.dataType,
        visibleTo: c.visibleTo,
        masks: c.masks,
        alias: c.alias || undefined,
        description: c.description || undefined,
        nativeFilterType: c.nativeFilterType || undefined,
        nativeFilterRequired:
          searchQuery !== null && isPipelineArgument(c) && argumentName(c) === searchQuery
            ? true
            : (c.nativeFilterRequired ?? undefined),
        requiredBySteward: c.requiredBySteward || undefined, // REQ-1986
        isPrimaryKey: c.isPrimaryKey || undefined,
        scope: c.scope || "domain",
        path: c.path?.trim() || undefined, // REQ-1739: ingest per-column JSON extraction path
        // REQ-2000: a vector column's registered model.
        embedding: c.embeddingModel ? true : undefined,
        embeddingModel: c.embeddingModel || undefined,
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
      if (columns.filter((c) => !isPipelineArgument(c)).length === 0) {
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
    // The short step declares no paging: it is not shown there, so nothing of it is sent; it is
    // set on the editor's Load and timeliness tab.
    if (
      !short &&
      pagingKind !== null &&
      pagingProblem(pagingKind, pagination, pagingCeilingRows) !== null
    ) {
      setError(t("tableEditForm.pagingFixErrors"));
      return;
    }
    if (needsCacheTtl && cacheTtl.trim() === "") {
      setError(t("registerTableForm.errorCacheTtlRequired"));
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
        cacheTtl: needsCacheTtl && cacheTtl.trim() !== "" ? Number(cacheTtl) : undefined,
        discover: isQueryApi ? false : discover, // REQ-252
        queryTemplate: isQueryApi ? cypher.trim() : undefined, // REQ-1670/REQ-1683
        fileGlob: isFilesSource && fileGlob.trim() ? fileGlob.trim() : undefined, // REQ-788
        sourceFileColumn:
          isFilesSource && sourceFileColumn.trim() ? sourceFileColumn.trim() : undefined, // REQ-788
        importSet: sourceType === "servicenow" ? importSet : undefined, // REQ-1954
        // REQ-318: only what is declared; nothing declared registers the table with no paging.
        pagination:
          !short && pagingKind !== null && declaredPaging(pagination) !== null
            ? pagingInput(pagination as Paging)
            : undefined,
        columns: selectedCols,
        // REQ-1093: drop empty/incomplete rows — a constraint needs a name and >=1 column.
        uniqueConstraints: uniqueConstraints
          .filter((u) => u.name.trim() && u.columns.length > 0)
          .map((u) => ({ name: u.name.trim(), columns: u.columns })),
      });
      if (!result.success) {
        setError(serverMessage(result, result.message));
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
      onSuccess(registeredTable(result));
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
    setCacheTtl("");
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
    // REQ-1443: the table scans every this many seconds; the server refuses it without one.
    if (!(Number(cacheTtl) >= 1)) {
      setError(t("registerTableForm.errorDqScanInterval"));
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
            masks: [],
            scope: "domain",
          },
        ],
        uniqueConstraints: [],
        dqContract,
        cacheTtl: Number(cacheTtl),
      });
      if (!result.success) {
        setError(serverMessage(result, result.message));
        return;
      }
      // A table registered into a domain the filter has not seen (one with no tables until now)
      // would otherwise stay hidden from the tables list until a reload.
      ensureDomainChecked(domainId);
      resetForm();
      onSuccess(registeredTable(result));
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
    <FormLayout data-tour="tables-form" className="form-card">
      {/* REQ-1968: what names the table (its source, domain, schema, table and alias) sits apart at
          the top; the rest flows beneath. */}
      <FormIdentifiers>
        <FormField size="medium">
          <label>
            {t("registerTableForm.sourceLabel")}
            <select
              className="form-select"
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
        </FormField>
        {domainAsked && (
          <FormField size="medium">
            <label>
              {t("registerTableForm.domainLabel")}
              <select
                className="form-select"
                value={domainId}
                onChange={(e) => setDomainId(e.target.value)}
                data-testid="register-table-domain-select"
              >
                <option value="">{t("registerTableForm.domainPlaceholder")}</option>
                {offeredDomains.map((d) => (
                  <option key={d} value={d}>
                    {d}
                  </option>
                ))}
              </select>
            </label>
          </FormField>
        )}
        {isChecker && (
          <FormField size="medium">
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
          </FormField>
        )}
        {isNativeQuerySource && (
          <FormField size="medium">
            <SegmentedControl
              aria-label={t("registerTableForm.mongoModeLabel")}
              value={mongoMode}
              onChange={(v) => setMongoMode(v as "collection" | "pipeline")}
              data={NATIVE_QUERY_MODES[sourceType as keyof typeof NATIVE_QUERY_MODES].map(
                ([value, key]) => ({ value, label: t(`registerTableForm.${key}`) }),
              )}
              data-testid={`register-table-${sourceType}-mode`}
            />
          </FormField>
        )}
        {isQueryApi && (
          <FormField size="medium">
            <TextInput
              required
              label={t("registerTableForm.neo4jTableNameLabel")}
              value={tableName}
              onChange={(e) => setTableName(e.currentTarget.value)}
              placeholder={t("registerTableForm.neo4jTableNamePlaceholder")}
              data-testid={`register-table-${sourceType}-table-name`}
            />
          </FormField>
        )}
        {isProfiler && (
          <FormField size="medium">
            <label>
              {t("registerTableForm.profilerResultLabel")}
              <select
                className="form-select"
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
          </FormField>
        )}
        {!isChecker && !isQueryApi && !isProfiler && (
          <>
            <FormField size="medium">
              <label>
                {t("registerTableForm.schemaLabel")}
                <select
                  className="form-select"
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
            </FormField>
            <FormField size="medium">
              <label>
                {t("registerTableForm.tableLabel")}
                <select
                  className="form-select"
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
                  <TableOptions tables={availableTables} isRegistered={isRegistered} />
                </select>
                {startingTablesTimedOut && (
                  <Text size="xs" c="red" mt={4} data-testid="register-table-starting-timeout">
                    {t("registerTableForm.tableStartingTimedOut")}
                  </Text>
                )}
              </label>
            </FormField>
            {sourceType === "servicenow" && tableName && (
              <ImportSetField
                {...{ sourceId, schemaName, tableName, value: importSet, onChange: setImportSet }}
              />
            )}
          </>
        )}
        <FormField size="medium">
          <TextInput
            data-testid="register-table-alias"
            label={t("registerTableForm.aliasLabel")}
            value={tableAlias}
            onChange={(e) => setTableAlias(e.currentTarget.value)}
            placeholder={t("registerTableForm.aliasPlaceholder")}
          />
        </FormField>
        {/* The describe-a-table search comes after the five fields it helps to fill, not between them. */}
        {!isChecker &&
          !isQueryApi &&
          !isProfiler &&
          sourceId &&
          schemaName &&
          !allTablesRegistered && (
            <NlTableSearch
              sourceId={sourceId}
              schemaName={schemaName}
              isRegistered={isRegistered}
              onPick={setTableName}
            />
          )}
      </FormIdentifiers>
      {needsCacheTtl && (
        <FormField
          hint={cacheTtlHint(cacheTtl, t)}
          help={t("registerTableForm.cacheTtlHint")}
          size="short"
        >
          <NumberInput
            min={NEVER_EXPIRES}
            data-testid="register-table-cache-ttl"
            label={t("registerTableForm.cacheTtlLabel")}
            required
            value={cacheTtl === "" ? "" : Number(cacheTtl)}
            onChange={(v) => setCacheTtl(v === "" ? "" : String(v))}
          />
        </FormField>
      )}
      {sourceId && !short && (
        <FormField size="medium">
          <RegionSelect
            value={region}
            onChange={setRegion}
            regions={regionChoices.regions}
            scope="table"
            testId="register-table-region-select"
          />
        </FormField>
      )}
      {isChecker && (
        <CheckerFields
          checker={sourceType}
          sourceId={sourceId}
          contractText={dqContract}
          onContractChange={setDqContract}
          onDatasetChange={onDqDatasetChange}
          scanInterval={cacheTtl}
          onScanIntervalChange={setCacheTtl}
          roles={roles}
          visibleTo={dqVisibleTo}
          onVisibleToChange={setDqVisibleTo}
        />
      )}
      {isQueryApi && (
        <QueryTableFields
          sourceType={sourceType}
          pipeline={isPipeline}
          query={cypher}
          setQuery={setCypher}
          args={pipelineArgs}
          setArgs={(next) =>
            setColumns([...next, ...columns.filter((c) => !isPipelineArgument(c))])
          }
          samples={samples}
          setSamples={setSamples}
          previewing={previewing}
          canPreview={Boolean(sourceId && cypher.trim())}
          onPreview={handleNeo4jPreview}
          previewRows={previewRows}
          columns={columns.filter((c) => !isPipelineArgument(c))}
        />
      )}
      {isPipeline && sourceType === QDRANT && (
        <QdrantVectorsNote sourceId={sourceId} template={cypher} />
      )}
      {isProfiler && tableName && (
        <ProfilerRowRulesField rules={profilerRowRules} onChange={setProfilerRowRules} />
      )}
      {!short && (
        <FormField size="long">
          <TextInput
            label={t("registerTableForm.descriptionLabel")}
            value={tableDescription}
            onChange={(e) => setTableDescription(e.currentTarget.value)}
            placeholder={t("registerTableForm.descriptionPlaceholder")}
          />
        </FormField>
      )}
      {pagingKind !== null && !short && (
        <PagingField
          kind={pagingKind}
          paging={pagination}
          onChange={setPagination}
          ceilingRows={pagingCeilingRows}
          rowsFieldEditable
        />
      )}
      {sourceId && !isChecker && !isQueryApi && !short && (
        <FormField
          size="medium"
          hint={
            isCdcSource
              ? t("registerTableForm.watermarkHintOptional")
              : t("registerTableForm.watermarkHintRequired")
          }
        >
          <Select
            label={t("registerTableForm.watermarkLabel")}
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
        </FormField>
      )}
      {isFilesSource && (
        <FilesGlobFieldset
          fileGlob={fileGlob}
          setFileGlob={setFileGlob}
          sourceFileColumn={sourceFileColumn}
          setSourceFileColumn={setSourceFileColumn}
        />
      )}
      {/* The ticks sit together beneath the fields. */}
      <FormChecks>
        {!isChecker &&
          !isQueryApi &&
          (!short || (tableName !== "" && !loadingColumns && columns.length === 0)) && (
            <FormField size="short" help={t("registerTableForm.discoverHint")}>
              <Checkbox
                checked={discover}
                onChange={(e) => setDiscover(e.currentTarget.checked)}
                data-testid="discover-columns-checkbox"
                label={t("registerTableForm.discoverLabel")}
              />
            </FormField>
          )}
      </FormChecks>
      {availableTables.find((tb) => tb.name === tableName)?.columnsDeclared && (
        <DeclaredColumns
          {...{ sourceId, tableName, columns, setColumns, irTypes }}
          visibleTo={roles.map((r) => r.id)}
        />
      )}
      {/* The heading shows once there is something under it: columns, or their loading. */}
      {short && (
        // Every discovered column is registered with its discovered type; a column discovery could
        // not type is the one thing about the columns asked here (the server refuses it untyped).
        <UntypedColumns
          columns={columns}
          untypedAtDiscovery={untypedAtDiscovery}
          irTypes={irTypes}
          loading={loadingColumns}
          onType={(i, v) => updateCol(i, "dataType", v)}
        />
      )}
      {short && sourceType === QDRANT && (
        <QdrantVectorColumns
          {...{ sourceId, columns }}
          collection={tableName}
          // A vector is offered unselected; naming its model registers it.
          onModel={(i, m) =>
            setColumns((cs) =>
              cs.map((c, j) => (j === i ? { ...c, embeddingModel: m, selected: m !== "" } : c)),
            )
          }
        />
      )}
      {!isChecker && !short && (columns.length > 0 || loadingColumns) && (
        <RegisterColumnsField
          {...{ columns, loadingColumns, updateCol, roles, irTypes, sourceType }}
        />
      )}
      {columns.length > 0 && !short && (
        <FormField size="full">
          <UniquesPanel
            uniques={uniqueConstraints}
            columns={columns.map((c) => c.name)}
            onChange={setUniqueConstraints}
          />
        </FormField>
      )}
      {/* Save registers the table, Cancel closes the form. (The page's "+ Table" only opens it.) */}
      <FormFooter
        panel
        problem={problem}
        onSave={handleSubmit}
        onCancel={onCancel}
        saveProps={{ "data-testid": "register-table-submit" }}
        cancelProps={{ "data-testid": "register-table-cancel" }}
      />
    </FormLayout>
  );
}
