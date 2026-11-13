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
import { isPipelineArgument, NATIVE_QUERY_MODES, previewedColumn } from "./queryTables";
import { QDRANT, QDRANT_SCHEMA, searchQueryArgument } from "../sources/qdrant";
import { QdrantVectorsNote } from "./QdrantVectorsNote";
import { RegisterColumnsField } from "./RegisterColumnsField";
import { defaultWatermark, discoveredColumns, registrationOf } from "./registrationDefaults";
import { ManyTablesList, ManyTablesMode } from "./ManyTablesPicker";
import { useManyTables } from "./useManyTables";
import type { TableRegistrationOutcome } from "../../hooks/useTableQueries";
import { DQ_PLACEHOLDER_COLUMN, registeredColumns } from "./registrationColumns";
import { QdrantVectorColumns } from "./QdrantVectorColumns";
import { FilesGlobFieldset } from "./FilesGlobFieldset";
import { isFilesSourceType } from "./tableSectionFields";
import { useTranslation } from "react-i18next";
import {
  Button,
  NumberInput,
  SegmentedControl,
  Select,
  Text,
  TextInput,
} from "@mantine/core";
import { domainToSqlName, toSnakeCase } from "../../naming";
import { NlTableSearch } from "./NlTableSearch";
import { useAvailableSchemas, useAvailableTables } from "../../hooks/useAdminQueries";
import type { LateColumns } from "../../hooks/useAvailableColumnsMetadataLazy";
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
import { DQ_CHECKERS } from "../../types/admin";
import type { Registration } from "./draftTable";
import { DERIVED_SOURCE_ID } from "../../types/admin";
import type { Paging, RegisteredTable, Source, UniqueConstraint } from "../../types/admin";
import type { Role } from "../../types/auth";
import type { ColumnForm } from "./types";
import { CDC_TYPES } from "./constants";
import { IR_TYPES_FALLBACK } from "../../irTypes";
import { isWatermarkEligible } from "./helpers";
import { CheckerFields } from "./CheckerFields";
import { PagingField } from "./PagingField";
import { pagingProblem } from "./paging";
import {
  FormChecks,
  FormField,
  FormFooter,
  FormIdentifiers,
  FormLayout,
} from "../../components/form/FormLayout";
import { useSourceKind } from "../sources/sourceKind";
import { ttlSignalMissingCacheTtl } from "../sources/loadManagement";
import { usePanelDirty } from "../../components/list/panelLeave";
import { ProfilerResultSelect } from "./ProfilerResultSelect";
import { requiredParametersOf } from "./lookups";
import { SchemaTableSelects } from "./SchemaTableSelects";
import { DeclaredColumns } from "./DeclaredColumns";
import { ImportSetField } from "./ImportSetField";
import { DiscoverColumnsField } from "./DiscoverColumnsField";
import { assignableDomainIds } from "../../lib/internalDomains";
import { DbtPublicProducts } from "./DbtPublicProducts";
import {
  confirmable,
  messageOf,
  registerDbtProducts,
  type DbtProductsPreview,
} from "./dbtProducts";

// REQ-1443: a checker's scans append to a relation in the org's control-plane schema, which the
// server stamps on registration (as for a profiler's results); the form carries this placeholder.
const DQ_RESULTS_SCHEMA = "default";
// REQ-1934: a profiler's result relations live in the org's control-plane schema, which the server
// stamps on registration (as for ingest); the form carries this placeholder until then.
const PROFILER_RESULTS_SCHEMA = "default";
// REQ-1670/REQ-1683: a query-API source lists one schema, named after its type.
const QUERY_API_TYPES = ["neo4j", "sparql"] as const;

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
    | {
        name: string;
        dataType: string;
        subtype?: string | null; // REQ-2171
        comment?: string | null;
        nativeFilterType?: string | null;
        nativeFilterRequired?: boolean | null;
        isPrimaryKey?: boolean | null;
        path?: string | null;
      }[]
    | null // null: the source is still starting (REQ-1824); the answer arrives as `lateColumns`
  >;
  /** The columns read answered after STARTING, with the table they belong to. */
  lateColumns?: LateColumns | null;
  columnsStarting?: boolean;
  columnsStartingTimedOut?: boolean;
  suggestTableAlias: (tableName: string, domainId: string, sourceId: string) => Promise<string>;
  /**
   * REQ-2109 (settled): what is registered is chosen here; every setting of the table follows in
   * the same sections as its editor, and Save there registers it.
   */
  onContinue: (registration: Registration) => void;
  /** Closes the form without registering anything. */
  onCancel: () => void;
  /** REQ-2228: a dbt source's public models were registered and these draft products created. */
  onProductsRegistered: (productIds: string[]) => void;
  setError: (e: string | null) => void;
  /** What the page has to say of a registration that did not go through. */
  problem?: string | null;
  /** REQ-2234: the source the form opens with already chosen (a source just saved). */
  initialSourceId?: string;
  /** REQ-2234: the form opens on picking several tables, not one. */
  severalFirst?: boolean;
  /** REQ-2234: several tables were registered in one action; what each one answered. */
  onRegisteredMany: (outcomes: TableRegistrationOutcome[]) => void;
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
  lateColumns,
  columnsStarting = false,
  columnsStartingTimedOut = false,
  suggestTableAlias,
  onContinue,
  onCancel,
  onProductsRegistered,
  setError,
  problem,
  initialSourceId = "",
  severalFirst = false,
  onRegisteredMany,
}: RegisterTableFormProps) {
  const { t } = useTranslation();
  const [sourceId, setSourceId] = useState(initialSourceId);
  // REQ-2234: several tables of the schema picked at once, each registered with its defaults.
  const [several, setSeveral] = useState(severalFirst);
  const [chosenDomain, setDomainId] = useState("");
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
  // REQ-2228: a dbt source registers its tables one by one, or all its public models with
  // enforced contracts at once as draft data products.
  const [dbtMode, setDbtMode] = useState<"tables" | "products">("tables");
  const productsMode = sourceType === "dbt" && dbtMode === "products";
  const [previewed, setPreviewed] = useState<{
    sourceId: string;
    preview: DbtProductsPreview;
  } | null>(null);
  const productsPreview = previewed?.sourceId === sourceId ? previewed.preview : null;
  const onPreview = useCallback(
    (of: string, preview: DbtProductsPreview) => setPreviewed({ sourceId: of, preview }),
    [],
  );
  const [registeringProducts, setRegisteringProducts] = useState(false);
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
  const offeredDomains = assignableDomainIds(domainHints).filter(
    (d) => domainAccess.includes("*") || domainAccess.includes(d),
  );
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
    setCypher("");
    setPreviewRows([]);
  }, [sourceId]);
  // A checker's results are seen by every role until the operator narrows them. The roles may
  // arrive after the source is chosen (a form opened on a source, REQ-2234): only this follows
  // them, so the schema picked meanwhile is kept.
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- the default grant follows the roles as they load
    setDqVisibleTo(roles.map((r) => r.id));
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
  const onDqDatasetChange = useCallback((dataset: string | null) => {
    if (dataset === lastDqDataset.current) return;
    lastDqDataset.current = dataset;
    if (dataset === null) {
      setDqScanned(null);
      return;
    }
    const [, schema, table] = dataset.split("/");
    setDqScanned({ schema, table });
    setTableName(`${table}_quality`);
    // REQ-2132: a checker's description is its quality claim, why the check exists -- the
    // registrant's to state, so nothing generic is put there for them.
  }, []);

  const applyColumns = (
    cols: NonNullable<Awaited<ReturnType<typeof getAvailableColumnsMetadata>>>,
  ) => {
    const formed = discoveredColumns(
      cols,
      roles.map((r) => r.id),
      sourceType,
    );
    setColumns(formed);
    setUntypedAtDiscovery(formed.filter((c) => !c.dataType).map((c) => c.name));
    const autoWm = defaultWatermark(formed, sources.find((s) => s.id === sourceId)?.type ?? "");
    if (autoWm) setWatermarkColumn(autoWm);
  };

  // The columns of a source that was still starting arrive here, once its server answers.
  useEffect(() => {
    if (
      lateColumns &&
      lateColumns.sourceId === sourceId &&
      lateColumns.schemaName === schemaName &&
      lateColumns.tableName === tableName
    ) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- the late answer of a read that began before this render
      applyColumns(lateColumns.columns);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- applied once per answer
  }, [lateColumns]);

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
        if (cols) applyColumns(cols);
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
    // REQ-2171: a subtype describes the type it came with; a new type has none.
    next[i] = { ...next[i], [key]: value, ...(key === "dataType" ? { subtype: null } : {}) };
    setColumns(next);
  };

  const handleSubmit = () => {
    setError(null);
    if (isChecker) {
      submitChecker();
      return;
    }
    // REQ-1983: a Qdrant search's query is the one argument Qdrant requires.
    const searchQuery =
      isPipeline && sourceType === QDRANT ? searchQueryArgument(cypher.trim()) : null;
    const selectedCols = registeredColumns(columns, searchQuery);
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
    // The short step does not show paging; the paging the source proposes is carried to the
    // editor's Load and timeliness tab, where it is accepted or edited (REQ-318).
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
    onContinue({
      ...registrationOf({
        sourceId,
        domainId,
        regionInput,
        schemaName,
        tableName,
        alias: tableAlias,
        description: tableDescription,
        watermarkColumn: isQueryApi ? "" : watermarkColumn,
        columns: selectedCols,
        uniqueConstraints,
        pagingKind,
        pagination,
        pagingCeilingRows,
        extra: {
          cacheTtl: needsCacheTtl && cacheTtl.trim() !== "" ? Number(cacheTtl) : undefined,
          discover: isQueryApi ? false : discover, // REQ-252
          queryTemplate: isQueryApi ? cypher.trim() : undefined, // REQ-1670/REQ-1683
          fileGlob: isFilesSource && fileGlob.trim() ? fileGlob.trim() : undefined, // REQ-788
          sourceFileColumn:
            isFilesSource && sourceFileColumn.trim() ? sourceFileColumn.trim() : undefined, // REQ-788
          importSet: sourceType === "servicenow" ? importSet : undefined, // REQ-1954
        },
      }),
      rowRules: isProfiler ? profilerRowRules : [],
    });
  };

  // REQ-1663: a checker registration is the contract plus a governance intent (visible_to); every
  // other input the server derives from the checker's fixed envelope (provisa.dq.registration).
  const submitChecker = () => {
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
    onContinue({
      input: {
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
      },
      pagination: null,
      pagingKind: null,
      pagingCeilingRows: null,
      rowRules: [],
    });
  };

  const isRegistered = (tbl: { name: string }) =>
    tables.some(
      (rt) => rt.sourceId === sourceId && toSnakeCase(rt.tableName) === toSnakeCase(tbl.name),
    );
  // REQ-1986: what each offered table needs to be read, a steward's requirements included once
  // it is registered.
  const requiredOf = (tbl: { name: string }) =>
    requiredParametersOf(
      availableTables.find((a) => a.name === tbl.name) ?? {},
      tables.find(
        (rt) => rt.sourceId === sourceId && toSnakeCase(rt.tableName) === toSnakeCase(tbl.name),
      ),
    );
  const allTablesRegistered =
    !loadingTables &&
    !!schemaName &&
    availableTables.length > 0 &&
    availableTables.every(isRegistered);

  // REQ-2234: a source whose tables are listed can have several picked at once.
  const listsTables = !isChecker && !isQueryApi && !isProfiler && !productsMode;
  const many = several && listsTables;
  const manyTables = useManyTables({
    sourceId,
    sourceType,
    domainId,
    schemaName,
    regionInput,
    roleIds: roles.map((r) => r.id),
    // Nothing is read until several are being picked.
    offered: many ? availableTables : [],
    isRegistered,
    suggestTableAlias,
    needsCacheTtl,
  });
  const isCdcSource = CDC_TYPES.has(sourceType);

  const registerProducts = async () => {
    setRegisteringProducts(true);
    setError(null);
    try {
      onProductsRegistered(await registerDbtProducts(sourceId, domainId));
    } catch (err) {
      setError(messageOf(err));
    } finally {
      setRegisteringProducts(false);
    }
  };

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
                    // Views come from + View, and a system source is Provisa's own.
                    s.id !== DERIVED_SOURCE_ID &&
                    !s.isSystem &&
                    (s.allowedDomains.length === 0 ||
                    s.allowedDomains.some((d) => checkedDomains.has(d))),
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
          <ProfilerResultSelect
            catalog={profilerCatalog}
            value={tableName}
            onChange={setTableName}
          />
        )}
        {sourceType === "dbt" && (
          <FormField size="medium">
            <SegmentedControl
              aria-label={t("dbtPublicProducts.modeLabel")}
              value={dbtMode}
              onChange={(v) => setDbtMode(v as "tables" | "products")}
              data={[
                { value: "tables", label: t("dbtPublicProducts.modeTables") },
                { value: "products", label: t("dbtPublicProducts.modeProducts") },
              ]}
              data-testid="register-table-dbt-mode"
            />
          </FormField>
        )}
        {listsTables && sourceId && <ManyTablesMode several={several} onChange={setSeveral} />}
        {listsTables && (
          <>
            <SchemaTableSelects
              {...{ sourceId, schemaName, setSchemaName, tableName, setTableName, isRegistered }}
              withTable={!many}
              requiredOf={requiredOf}
              schemas={availableSchemas}
              loadingSchemas={loadingSchemas}
              fixedSchema={isFixedSchema}
              tables={availableTables}
              loadingTables={loadingTables}
              allRegistered={allTablesRegistered}
              starting={startingTables}
              startingTimedOut={startingTablesTimedOut}
            />
            {!many && tableName && (columnsStarting || columnsStartingTimedOut) && (
              <Text
                size="xs"
                c={columnsStartingTimedOut ? "red" : "dimmed"}
                data-testid="register-table-columns-starting"
              >
                {columnsStartingTimedOut
                  ? t("registerTableForm.tableStartingTimedOut")
                  : t("registerTableForm.tableStarting")}
              </Text>
            )}
            {!many && sourceType === "servicenow" && tableName && (
              <ImportSetField
                {...{ sourceId, schemaName, tableName, value: importSet, onChange: setImportSet }}
              />
            )}
          </>
        )}
        {!productsMode && !many && (
          <FormField size="medium">
            <TextInput
              data-testid="register-table-alias"
              label={t("registerTableForm.aliasLabel")}
              value={tableAlias}
              onChange={(e) => setTableAlias(e.currentTarget.value)}
              placeholder={t("registerTableForm.aliasPlaceholder")}
            />
          </FormField>
        )}
        {/* The describe-a-table search comes after the five fields it helps to fill, not between them. */}
        {!isChecker &&
          !isQueryApi &&
          !isProfiler &&
          !productsMode &&
          !many &&
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
      {productsMode && (
        <DbtPublicProducts key={sourceId} sourceId={sourceId} onPreview={onPreview} />
      )}
      {many && (
        <ManyTablesList
          offered={availableTables}
          isRegistered={isRegistered}
          readings={manyTables.readings}
          ticked={manyTables.ticked}
          setTicked={manyTables.setTicked}
          loading={!schemaName || loadingTables}
          needsCacheTtl={manyTables.needsCacheTtl}
          // A table that needs input is registered on its own, opened in edit to be given it.
          onRegisterOne={(name) => void manyTables.registerAsDraft(name, onContinue, setError)}
          opening={manyTables.opening}
        />
      )}
      {!productsMode && !many && (
        <>
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
          {/* REQ-2132: on a checker source the description is the check's claim, why it exists. */}
          {!short && isChecker && (
            <FormField size="long" hint={t("registerTableForm.checkReasonHint")}>
              <TextInput
                label={t("registerTableForm.checkReasonLabel")}
                value={tableDescription}
                onChange={(e) => setTableDescription(e.currentTarget.value)}
                placeholder={t("registerTableForm.checkReasonPlaceholder")}
                data-testid="register-table-description"
              />
            </FormField>
          )}
          {!short && !isChecker && (
            <FormField size="long">
              <TextInput
                label={t("registerTableForm.descriptionLabel")}
                value={tableDescription}
                onChange={(e) => setTableDescription(e.currentTarget.value)}
                placeholder={t("registerTableForm.descriptionPlaceholder")}
                data-testid="register-table-description"
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
                <DiscoverColumnsField
                  checked={discover}
                  onChange={setDiscover}
                  help={t("registerTableForm.discoverHint")}
                />
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
        </>
      )}
      {/* Continue takes the chosen table to its settings, Cancel closes the form. */}
      <FormFooter
        panel
        problem={problem}
        onCancel={onCancel}
        cancelProps={{ "data-testid": "register-table-cancel" }}
        // Choosing what is registered is not a save: every setting of it follows (REQ-2109).
        action={
          many ? (
            <Button
              onClick={() => void manyTables.registerAll(domainId, onRegisteredMany, setError)}
              loading={manyTables.registering}
              disabled={manyTables.ticked.length === 0}
              data-testid="register-many-tables-submit"
            >
              {t("registerManyTables.register", { count: manyTables.ticked.length })}
            </Button>
          ) : productsMode ? (
            <Button
              onClick={() => void registerProducts()}
              loading={registeringProducts}
              disabled={!confirmable(productsPreview) || !domainId}
              data-testid="register-table-dbt-products-submit"
            >
              {t("dbtPublicProducts.confirm", { count: productsPreview?.products.length ?? 0 })}
            </Button>
          ) : (
            <Button onClick={handleSubmit} data-testid="register-table-submit">
              {t("registerTableForm.continue")}
            </Button>
          )
        }
      />
    </FormLayout>
  );
}
