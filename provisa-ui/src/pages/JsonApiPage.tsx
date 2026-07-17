// Copyright (c) 2026 Kenneth Stott
// Canary: f865ebd8-0f9b-4673-8e1e-e635503594ab
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useMemo, useEffect, useCallback, useRef } from "react";
import { useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Anchor,
  Badge,
  Button,
  Card,
  Checkbox,
  Collapse,
  Group,
  NumberInput,
  Select,
  Table,
  Tabs,
  Text,
  TextInput,
  Title,
  Tooltip,
} from "@mantine/core";
import {
  AlertCircle,
  Check,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  ChevronUp,
  Copy,
  Download,
} from "lucide-react";
import { useAuth } from "../context/AuthContext";
import { useDomainFilter } from "../context/DomainFilterContext";
import { useDomains, useTables } from "../hooks/useAdminQueries";
import "./JsonApiPage.css";

interface JsonApiRelationshipRef {
  type: string;
  id: string;
}

interface JsonApiRelationship {
  data?: JsonApiRelationshipRef | JsonApiRelationshipRef[] | null;
}

interface JsonApiResource {
  type: string;
  id?: string;
  attributes?: Record<string, unknown>;
  relationships?: Record<string, JsonApiRelationship>;
}

interface JsonApiDocument {
  data?: JsonApiResource | JsonApiResource[];
  included?: JsonApiResource[];
  meta?: Record<string, unknown>;
  links?: Record<string, string | null>;
  errors?: Array<{ detail?: string }>;
}

function toGqlName(col: { columnName: string; alias?: string | null }): string {
  if (col.alias) return col.alias;
  return col.columnName.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase());
}

interface PaginationLinks {
  first: string | null;
  prev: string | null;
  next: string | null;
  last: string | null;
}

function ResourceCard({
  item,
  i,
  includedSet,
  relationshipTitle,
}: {
  item: JsonApiResource;
  i: number;
  includedSet?: Set<string>;
  relationshipTitle: (rel: string, ref: JsonApiRelationshipRef) => string;
}) {
  const rels = item.relationships
    ? Object.entries(item.relationships)
    : [];
  return (
    <Card
      id={`res-${item.type}-${item.id ?? i}`}
      withBorder
      padding={0}
      radius="sm"
      className="jsonapi-resource-card"
    >
      <div className="jsonapi-resource-header">
        <span className="jsonapi-resource-type">{item.type}</span>
        <span className="jsonapi-resource-id">#{item.id}</span>
        {rels.length > 0 && (
          <span className="jsonapi-rel-links">
            {rels.map(([relName, relData]) => {
              const ref = relData?.data;
              const refs: JsonApiRelationshipRef[] = Array.isArray(ref) ? ref : ref ? [ref] : [];
              return refs
                .filter((r: JsonApiRelationshipRef) => !includedSet || includedSet.has(`${r.type}::${r.id}`))
                .map((r: JsonApiRelationshipRef) => (
                  <Anchor
                    key={`${relName}-${r.id}`}
                    className="jsonapi-rel-link"
                    href={`#res-${r.type}-${r.id}`}
                    title={relationshipTitle(relName, r)}
                  >
                    {relName} #{r.id}
                  </Anchor>
                ));
            })}
          </span>
        )}
      </div>
      {item.attributes && (
        <div className="jsonapi-attr-scroll">
          <Table className="jsonapi-attr-table">
            <Table.Tbody>
              {Object.entries(item.attributes as Record<string, unknown>)
                .filter(([k]) => !k.startsWith("_"))
                .map(([k, v]) => (
                  <Table.Tr key={k}>
                    <Table.Td className="jsonapi-attr-key">{k}</Table.Td>
                    <Table.Td className="jsonapi-attr-val">
                      {v === null || v === undefined
                        ? <span className="jsonapi-attr-null">null</span>
                        : typeof v === "object"
                          ? JSON.stringify(v)
                          : String(v)}
                    </Table.Td>
                  </Table.Tr>
                ))}
            </Table.Tbody>
          </Table>
        </div>
      )}
    </Card>
  );
}

function SummaryView({
  doc,
  noResultsLabel,
  includedLabel,
  relationshipTitle,
}: {
  doc: JsonApiDocument;
  noResultsLabel: string;
  includedLabel: string;
  relationshipTitle: (rel: string, ref: JsonApiRelationshipRef) => string;
}) {
  const items: JsonApiResource[] = Array.isArray(doc.data)
    ? doc.data
    : doc.data
      ? [doc.data]
      : [];
  const included: JsonApiResource[] = Array.isArray(doc.included) ? doc.included : [];
  const includedSet = new Set<string>(included.map((r) => `${r.type}::${r.id}`));
  if (items.length === 0) {
    return <div className="jsonapi-summary-empty">{noResultsLabel}</div>;
  }
  return (
    <div className="jsonapi-summary">
      {items.map((item, i) => (
        <ResourceCard
          key={`${item.type}-${item.id ?? i}`}
          item={item}
          i={i}
          includedSet={includedSet}
          relationshipTitle={relationshipTitle}
        />
      ))}
      {included.length > 0 && (
        <>
          <div className="jsonapi-included-divider">{includedLabel}</div>
          {included.map((item, i) => (
            <ResourceCard
              key={`inc-${item.type}-${item.id ?? i}`}
              item={item}
              i={i}
              relationshipTitle={relationshipTitle}
            />
          ))}
        </>
      )}
    </div>
  );
}

const JSONAPI_SETTINGS_KEY = "provisa.jsonapi.settings";
const FILTER_OPS = ["eq", "neq", "gt", "gte", "lt", "lte", "like"];

export function JsonApiPage() {
  const { t } = useTranslation();
  const location = useLocation();
  const { role } = useAuth();
  const roleId = role?.id ?? "";
  const { checkedDomains } = useDomainFilter();
  const [navUrl] = useState(
    () => (location.state as { jsonapiUrl?: string } | null)?.jsonapiUrl ?? "",
  );
  const [navAutoRun] = useState(
    () => (location.state as { autoRun?: boolean } | null)?.autoRun === true,
  );
  const { tables, loading } = useTables();
  const { domains } = useDomains();
  const domainDescMap = useMemo(
    () => Object.fromEntries(domains.map((d) => [d.id, d.description])),
    [domains],
  );

  const parsedNav = useMemo(() => {
    if (!navUrl) return null;
    const match = navUrl.match(/^\/data\/jsonapi\/([^/]+)\/([^?]+)/);
    if (!match) return null;
    const qs = navUrl.includes("?") ? navUrl.split("?")[1] : "";
    const params = new URLSearchParams(qs);
    return { domainId: match[1], tableName: match[2], pageSize: params.get("page[size]") ?? "20" };
  }, [navUrl]);

  // Persisted explorer settings (survive across sessions). Table selection is restored after the
  // table list loads (below); page size is stable so it initializes directly.
  const savedSettings = useMemo<{ selectedTable?: string; pageSize?: string }>(() => {
    try {
      return JSON.parse(localStorage.getItem(JSONAPI_SETTINGS_KEY) || "{}");
    } catch {
      return {};
    }
  }, []);

  const [selectedTable, setSelectedTable] = useState<string>("");
  const [checkedFields, setCheckedFields] = useState<Set<string>>(new Set());
  const [checkedIncludes, setCheckedIncludes] = useState<Set<string>>(new Set());
  const [filterField, setFilterField] = useState<string>("");
  const [filterOp, setFilterOp] = useState<string>("eq");
  const [filterValue, setFilterValue] = useState<string>("");
  const [sortField, setSortField] = useState<string>("");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [pageSize, setPageSize] = useState<string>(savedSettings.pageSize ?? "20");
  const [parsedDoc, setParsedDoc] = useState<JsonApiDocument | null>(null);
  const [activeUrl, setActiveUrl] = useState<string>("");
  const [viewTab, setViewTab] = useState<"summary" | "raw">("summary");
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string>("");
  const [copied, setCopied] = useState(false);
  const [fieldsOpen, setFieldsOpen] = useState(false);
  const [includeOpen, setIncludeOpen] = useState(false);

  // Filter tables by checked domains
  const filteredTables = useMemo(() => {
    if (checkedDomains.size === 0) return tables;
    return tables.filter((t) => !t.domainId || checkedDomains.has(t.domainId));
  }, [tables, checkedDomains]);

  // Group filtered tables by domainId
  const domainGroups = useMemo(() => {
    const groups: Record<string, typeof filteredTables> = {};
    for (const t of filteredTables) {
      const key = t.domainId || "(no domain)";
      if (!groups[key]) groups[key] = [];
      groups[key].push(t);
    }
    return groups;
  }, [filteredTables]);

  const tableSelectData = useMemo(
    () =>
      Object.entries(domainGroups).map(([domain, domainTables]) => {
        const domainDesc = domainDescMap[domain];
        return {
          group: domainDesc ? `${domain} — ${domainDesc}` : domain,
          items: domainTables.map((tbl) => ({
            value: `${tbl.domainId}/${tbl.tableName}`,
            label: tbl.alias ?? tbl.tableName,
          })),
        };
      }),
    [domainGroups, domainDescMap],
  );

  // selectedTable is "domainId/tableName"
  const [selectedDomainId, selectedTableName] = selectedTable.includes("/")
    ? selectedTable.split("/", 2)
    : ["", selectedTable];

  const tableObj = useMemo(
    () =>
      filteredTables.find(
        (t) => t.domainId === selectedDomainId && t.tableName === selectedTableName,
      ),
    [filteredTables, selectedDomainId, selectedTableName],
  );

  // Derived: "" when selectedTable is no longer present in filteredTables, avoiding a
  // setState-in-effect to reset it. tableObj encodes the "found in filtered list" check.
  const effectiveSelectedTable = selectedTable && tableObj ? selectedTable : "";

  const columnNames = useMemo(
    () => tableObj?.columns.map(toGqlName) ?? [],
    [tableObj],
  );

  const columnSelectData = useMemo(
    () =>
      (tableObj?.columns ?? []).map((c) => ({
        value: toGqlName(c),
        label: c.alias ?? c.columnName,
      })),
    [tableObj],
  );

  // Relationship names derived from FK columns (strip _id suffix)
  const relationshipNames = useMemo(() => {
    if (!tableObj) return [];
    return tableObj.columns
      .filter((c) => c.isForeignKey && c.columnName.endsWith("_id"))
      .map((c) => c.columnName.slice(0, -3));
  }, [tableObj]);

  // Pagination links from parsed response
  const paginationLinks = useMemo((): PaginationLinks | null => {
    if (!parsedDoc?.links) return null;
    const { first, prev, next, last } = parsedDoc.links as Record<string, string | null>;
    if (!first && !prev && !next && !last) return null;
    return { first: first ?? null, prev: prev ?? null, next: next ?? null, last: last ?? null };
  }, [parsedDoc]);

  // Result count + range from parsed response
  const resultMeta = useMemo(() => {
    if (!parsedDoc) return null;
    const total = parsedDoc.meta?.total as number | undefined;
    const count = Array.isArray(parsedDoc.data)
      ? parsedDoc.data.length
      : parsedDoc.data ? 1 : 0;
    let rangeStart: number | undefined;
    let rangeEnd: number | undefined;
    if (activeUrl) {
      const qs = activeUrl.includes("?") ? activeUrl.split("?")[1] : "";
      const p = new URLSearchParams(qs);
      const pNum = parseInt(p.get("page[number]") ?? "1", 10);
      const pSize = parseInt(p.get("page[size]") ?? "20", 10);
      rangeStart = (pNum - 1) * pSize + 1;
      rangeEnd = rangeStart + count - 1;
    }
    return { total, count, rangeStart, rangeEnd };
  }, [parsedDoc, activeUrl]);

  // When table changes, reset all dependent state
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- cascade reset of per-table interactive state (checked fields, filters, sort, results) when selected table changes; these are user-controlled inputs that cannot be derived from render
    setCheckedFields(new Set());
    setCheckedIncludes(new Set());
    setFilterField("");
    setSortField("");
    setParsedDoc(null);
    setActiveUrl("");
    setError("");
    setViewTab("summary");
    setFieldsOpen(false);
    setIncludeOpen(false);
  }, [selectedTable]);

  const navInitDoneRef = useRef(false);
  useEffect(() => {
    if (!parsedNav || tables.length === 0 || navInitDoneRef.current) return;
    const match = tables.find(
      (t) => t.domainId === parsedNav.domainId && t.tableName === parsedNav.tableName,
    );
    if (!match) return;
    navInitDoneRef.current = true;
    // eslint-disable-next-line react-hooks/set-state-in-effect -- initializes table selection from navigation URL state after async tables list load; cannot synchronize before tables are available
    setSelectedTable(`${match.domainId}/${match.tableName}`);
    setPageSize(parsedNav.pageSize);
  }, [tables, parsedNav]);

  // Restore the persisted table selection once the table list has loaded (nav-from-NL wins). Gating
  // on tables avoids the "reset selected table if not in list" effect wiping it before load.
  const restoreDoneRef = useRef(false);
  useEffect(() => {
    if (restoreDoneRef.current || parsedNav || tables.length === 0) return;
    restoreDoneRef.current = true;
    const saved = savedSettings.selectedTable;
    if (saved && tables.find((t) => `${t.domainId}/${t.tableName}` === saved)) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- restores persisted table selection after async tables list load; cannot initialize synchronously before tables are available
      setSelectedTable(saved);
    }
  }, [tables, parsedNav, savedSettings]);

  // Persist stable explorer settings across sessions.
  useEffect(() => {
    localStorage.setItem(JSONAPI_SETTINGS_KEY, JSON.stringify({ selectedTable, pageSize }));
  }, [selectedTable, pageSize]);

  function toggleField(col: string) {
    setCheckedFields((prev) => {
      const next = new Set(prev);
      if (next.has(col)) next.delete(col);
      else next.add(col);
      return next;
    });
  }

  function toggleInclude(rel: string) {
    setCheckedIncludes((prev) => {
      const next = new Set(prev);
      if (next.has(rel)) next.delete(rel);
      else next.add(rel);
      return next;
    });
  }

  const sparseFieldsParam = useMemo(() => {
    if (checkedFields.size === 0 || checkedFields.size === columnNames.length) return "";
    return [...checkedFields].join(",");
  }, [checkedFields, columnNames.length]);

  const includeParam = useMemo(
    () => (checkedIncludes.size > 0 ? [...checkedIncludes].join(",") : ""),
    [checkedIncludes],
  );

  const domainsParam = useMemo(
    () => (checkedDomains.size > 0 ? [...checkedDomains].join(",") : ""),
    [checkedDomains],
  );

  const url = useMemo(() => {
    if (!effectiveSelectedTable || !selectedDomainId || !selectedTableName) return "";
    const params = new URLSearchParams();
    if (roleId) params.set("role", roleId);
    if (filterField && filterValue) {
      const key =
        filterOp === "eq"
          ? `filter[${filterField}]`
          : `filter[${filterField}][${filterOp}]`;
      params.set(key, filterValue);
    }
    if (sortField) {
      params.set("sort", sortDir === "desc" ? `-${sortField}` : sortField);
    }
    if (pageSize) params.set("page[size]", pageSize);
    if (sparseFieldsParam) params.set(`fields[${selectedTableName}]`, sparseFieldsParam);
    if (includeParam) params.set("include", includeParam);
    const qs = params.toString();
    return `/data/jsonapi/${selectedDomainId}/${selectedTableName}${qs ? "?" + qs : ""}`;
  }, [effectiveSelectedTable, selectedDomainId, selectedTableName, roleId, filterField, filterOp, filterValue, sortField, sortDir, pageSize, sparseFieldsParam, includeParam]);

  const specUrl = useMemo(() => {
    const params = new URLSearchParams();
    if (roleId) params.set("role", roleId);
    if (domainsParam) params.set("domains", domainsParam);
    params.set("download", "1");
    return `/data/jsonapi/openapi.json?${params.toString()}`;
  }, [roleId, domainsParam]);

  const fetchUrl = useCallback(async (fetchTarget: string) => {
    setRunning(true);
    setError("");
    try {
      const res = await fetch(fetchTarget, {
        headers: {
          Accept: "application/vnd.api+json",
          ...(roleId ? { "x-provisa-role": roleId } : {}),
        },
      });
      const json = await res.json();
      if (!res.ok) {
        setError(json.errors?.[0]?.detail ?? JSON.stringify(json));
        setParsedDoc(null);
      } else {
        setParsedDoc(json);
        setActiveUrl(fetchTarget);
      }
    } catch (e) {
      setError(String(e));
      setParsedDoc(null);
    } finally {
      setRunning(false);
    }
  }, [roleId]);

  const navAutoRunDoneRef = useRef(false);
  useEffect(() => {
    if (!navAutoRun || !parsedNav || !navInitDoneRef.current || navAutoRunDoneRef.current) return;
    if (!url) return;
    navAutoRunDoneRef.current = true;
    // eslint-disable-next-line react-hooks/set-state-in-effect -- triggers async fetch for auto-run-on-navigation; setState occurs inside fetchUrl's promise chain, not synchronously in the effect body
    void fetchUrl(url);
  }, [navAutoRun, parsedNav, url, fetchUrl]);

  async function handleRun() {
    if (!url) return;
    await fetchUrl(url);
  }

  async function handlePaginate(target: string | null) {
    if (!target) return;
    await fetchUrl(target);
  }

  function handleCopy() {
    const copyTarget = activeUrl || url;
    if (!copyTarget) return;
    void navigator.clipboard.writeText(window.location.origin + copyTarget);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  }

  const relationshipTitle = useCallback(
    (rel: string, ref: JsonApiRelationshipRef) =>
      t("jsonApiPage.relationshipTitle", { rel, type: ref.type, id: ref.id }),
    [t],
  );

  const displayUrl = activeUrl || url;

  const fieldsSummary =
    checkedFields.size === 0 || checkedFields.size === columnNames.length
      ? t("jsonApiPage.fieldsAll")
      : t("jsonApiPage.fieldsSelected", { count: checkedFields.size });

  const includeSummary =
    checkedIncludes.size === 0
      ? t("jsonApiPage.includeNone")
      : t("jsonApiPage.includeSelected", { count: checkedIncludes.size });

  const resultCountText = resultMeta
    ? resultMeta.rangeStart !== undefined && resultMeta.rangeEnd !== undefined
      ? resultMeta.total !== undefined
        ? t("jsonApiPage.resultRangeOfTotal", {
            start: resultMeta.rangeStart,
            end: resultMeta.rangeEnd,
            total: resultMeta.total,
          })
        : t("jsonApiPage.resultRange", { start: resultMeta.rangeStart, end: resultMeta.rangeEnd })
      : resultMeta.total !== undefined
        ? t("jsonApiPage.resultCountOfTotal", { count: resultMeta.count, total: resultMeta.total })
        : t("jsonApiPage.resultCount", { count: resultMeta.count })
    : null;

  return (
    <div className="jsonapi-page page">
      <div className="jsonapi-layout">
        <div className="jsonapi-sidebar">
          <div className="jsonapi-sidebar-header">
            <Title order={2} className="jsonapi-section-title">
              {t("jsonApiPage.title")}
            </Title>
            <Anchor
              className="jsonapi-spec-link"
              href={specUrl}
              download="jsonapi-openapi.json"
              aria-label={t("jsonApiPage.downloadSpecAria")}
            >
              <Group gap={4} wrap="nowrap">
                <Download size={12} />
                {t("jsonApiPage.specLink")}
              </Group>
            </Anchor>
          </div>
          <p className="jsonapi-desc">{t("jsonApiPage.description")}</p>

          <Select
            label={t("jsonApiPage.tableLabel")}
            size="xs"
            placeholder={loading ? t("jsonApiPage.loadingOption") : t("jsonApiPage.selectTablePlaceholder")}
            data={tableSelectData}
            value={effectiveSelectedTable || null}
            onChange={(v) => setSelectedTable(v ?? "")}
            disabled={loading}
            searchable
            data-testid="jsonapi-table-select"
          />
          {tableObj?.description && (
            <p className="jsonapi-table-desc">{tableObj.description}</p>
          )}

          {columnNames.length > 0 && (
            <>
              <div className="jsonapi-dropdown-group">
                <button
                  type="button"
                  className="jsonapi-dropdown-trigger"
                  onClick={() => setFieldsOpen((o) => !o)}
                  aria-expanded={fieldsOpen}
                  data-testid="jsonapi-fields-trigger"
                >
                  <span>{t("jsonApiPage.fieldsLabel")}</span>
                  <span className="jsonapi-dropdown-meta">
                    {fieldsSummary}{" "}
                    {fieldsOpen ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
                  </span>
                </button>
                <Collapse in={fieldsOpen}>
                  <div className="jsonapi-field-list">
                    {(tableObj?.columns ?? []).map((col) => {
                      const gqlName = toGqlName(col);
                      return (
                        <Checkbox
                          key={col.columnName}
                          className="jsonapi-field-item"
                          size="xs"
                          title={col.description ?? undefined}
                          checked={checkedFields.has(gqlName)}
                          onChange={() => toggleField(gqlName)}
                          label={<span className="jsonapi-field-name">{col.alias ?? col.columnName}</span>}
                        />
                      );
                    })}
                  </div>
                </Collapse>
              </div>

              {relationshipNames.length > 0 && (
                <div className="jsonapi-dropdown-group">
                  <button
                    type="button"
                    className="jsonapi-dropdown-trigger"
                    onClick={() => setIncludeOpen((o) => !o)}
                    aria-expanded={includeOpen}
                    data-testid="jsonapi-include-trigger"
                  >
                    <span>{t("jsonApiPage.includeLabel")}</span>
                    <span className="jsonapi-dropdown-meta">
                      {includeSummary}{" "}
                      {includeOpen ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
                    </span>
                  </button>
                  <Collapse in={includeOpen}>
                    <div className="jsonapi-field-list">
                      {relationshipNames.map((rel) => (
                        <Checkbox
                          key={rel}
                          className="jsonapi-field-item"
                          size="xs"
                          checked={checkedIncludes.has(rel)}
                          onChange={() => toggleInclude(rel)}
                          label={<span className="jsonapi-field-name">{rel}</span>}
                        />
                      ))}
                    </div>
                  </Collapse>
                </div>
              )}

              <div className="jsonapi-section-divider">
                {t("jsonApiPage.filterSectionTitle")}{" "}
                <span className="jsonapi-section-hint">{t("jsonApiPage.optionalHint")}</span>
              </div>
              <div className="jsonapi-row">
                <Select
                  className="jsonapi-select-flex"
                  size="xs"
                  aria-label={t("jsonApiPage.filterSectionTitle")}
                  placeholder={t("jsonApiPage.fieldPlaceholder")}
                  data={columnSelectData}
                  value={filterField || null}
                  onChange={(v) => setFilterField(v ?? "")}
                  clearable
                  data-testid="jsonapi-filter-field-select"
                />
                <Select
                  className="jsonapi-select-op"
                  size="xs"
                  aria-label={t("jsonApiPage.filterSectionTitle")}
                  data={FILTER_OPS}
                  value={filterOp}
                  onChange={(v) => setFilterOp(v ?? "eq")}
                  disabled={!filterField}
                  allowDeselect={false}
                  data-testid="jsonapi-filter-op-select"
                />
              </div>
              <TextInput
                size="xs"
                placeholder={t("jsonApiPage.filterValuePlaceholder")}
                aria-label={t("jsonApiPage.filterValuePlaceholder")}
                value={filterValue}
                onChange={(e) => setFilterValue(e.currentTarget.value)}
                disabled={!filterField}
                data-testid="jsonapi-filter-value-input"
              />

              <div className="jsonapi-section-divider">
                {t("jsonApiPage.sortSectionTitle")}{" "}
                <span className="jsonapi-section-hint">{t("jsonApiPage.optionalHint")}</span>
              </div>
              <div className="jsonapi-row">
                <Select
                  className="jsonapi-select-flex"
                  size="xs"
                  aria-label={t("jsonApiPage.sortSectionTitle")}
                  placeholder={t("jsonApiPage.fieldPlaceholder")}
                  data={columnSelectData}
                  value={sortField || null}
                  onChange={(v) => setSortField(v ?? "")}
                  clearable
                  data-testid="jsonapi-sort-field-select"
                />
                <Select
                  className="jsonapi-select-op"
                  size="xs"
                  aria-label={t("jsonApiPage.sortSectionTitle")}
                  data={[
                    { value: "asc", label: t("jsonApiPage.sortAsc") },
                    { value: "desc", label: t("jsonApiPage.sortDesc") },
                  ]}
                  value={sortDir}
                  onChange={(v) => setSortDir((v as "asc" | "desc") ?? "asc")}
                  disabled={!sortField}
                  allowDeselect={false}
                  data-testid="jsonapi-sort-dir-select"
                />
              </div>
            </>
          )}

          <div className="jsonapi-section-divider" style={{ marginTop: columnNames.length ? undefined : "0.5rem" }}>
            {t("jsonApiPage.paginationSectionTitle")}
          </div>
          <NumberInput
            label={t("jsonApiPage.pageSizeLabel")}
            size="xs"
            min={1}
            max={1000}
            value={pageSize === "" ? "" : Number(pageSize)}
            onChange={(v) => setPageSize(v === "" ? "" : String(v))}
            data-testid="jsonapi-page-size-input"
          />

          <Button
            className="jsonapi-run-btn"
            onClick={handleRun}
            disabled={!effectiveSelectedTable}
            loading={running}
            fullWidth
            mt="sm"
            data-testid="jsonapi-run-button"
          >
            {running ? t("jsonApiPage.runningButton") : t("jsonApiPage.runButton")}
          </Button>
        </div>

        <div className="jsonapi-main">
          <div className="jsonapi-url-bar">
            <Badge className="jsonapi-method" color="green" variant="light" radius="sm">
              {t("jsonApiPage.methodGet")}
            </Badge>
            <span className="jsonapi-url">{displayUrl || "—"}</span>
            {displayUrl && (
              <Tooltip label={copied ? t("jsonApiPage.copied") : t("jsonApiPage.copyUrlAria")}>
                <ActionIcon
                  className="jsonapi-copy-btn"
                  variant="default"
                  size="sm"
                  aria-label={copied ? t("jsonApiPage.copied") : t("jsonApiPage.copyUrlAria")}
                  onClick={handleCopy}
                  data-testid="jsonapi-copy-button"
                >
                  {copied ? <Check size={14} /> : <Copy size={14} />}
                </ActionIcon>
              </Tooltip>
            )}
          </div>

          <div className="jsonapi-panel">
            <div className="jsonapi-panel-header">
              <div className="jsonapi-panel-header-left">
                <Text size="xs" fw={600} tt="uppercase">
                  {t("jsonApiPage.responseLabel")}
                </Text>
                {resultCountText && (
                  <span className="jsonapi-result-count">{resultCountText}</span>
                )}
              </div>
              {parsedDoc && (
                <Tabs
                  value={viewTab}
                  onChange={(v) => setViewTab((v as "summary" | "raw") ?? "summary")}
                  className="jsonapi-tab-bar"
                >
                  <Tabs.List>
                    <Tabs.Tab value="summary" data-testid="jsonapi-summary-tab">
                      {t("jsonApiPage.summaryTab")}
                    </Tabs.Tab>
                    <Tabs.Tab value="raw" data-testid="jsonapi-raw-tab">
                      {t("jsonApiPage.rawTab")}
                    </Tabs.Tab>
                  </Tabs.List>
                </Tabs>
              )}
            </div>

            {(paginationLinks?.prev || paginationLinks?.next) && (
              <div className="jsonapi-pagination-bar">
                <ActionIcon
                  className="jsonapi-page-btn"
                  variant="default"
                  size="sm"
                  disabled={!paginationLinks.first}
                  onClick={() => handlePaginate(paginationLinks!.first)}
                  aria-label={t("jsonApiPage.firstPage")}
                  data-testid="jsonapi-page-first"
                >
                  <ChevronsLeft size={14} />
                </ActionIcon>
                <Button
                  className="jsonapi-page-btn"
                  variant="default"
                  size="compact-xs"
                  leftSection={<ChevronLeft size={12} />}
                  disabled={!paginationLinks.prev}
                  onClick={() => handlePaginate(paginationLinks!.prev)}
                  data-testid="jsonapi-page-prev"
                >
                  {t("jsonApiPage.previousPage")}
                </Button>
                <Button
                  className="jsonapi-page-btn"
                  variant="default"
                  size="compact-xs"
                  rightSection={<ChevronRight size={12} />}
                  disabled={!paginationLinks.next}
                  onClick={() => handlePaginate(paginationLinks!.next)}
                  data-testid="jsonapi-page-next"
                >
                  {t("jsonApiPage.nextPage")}
                </Button>
                <ActionIcon
                  className="jsonapi-page-btn"
                  variant="default"
                  size="sm"
                  disabled={!paginationLinks.last}
                  onClick={() => handlePaginate(paginationLinks!.last)}
                  aria-label={t("jsonApiPage.lastPage")}
                  data-testid="jsonapi-page-last"
                >
                  <ChevronsRight size={14} />
                </ActionIcon>
              </div>
            )}

            {error && (
              <Alert
                className="jsonapi-error"
                color="red"
                icon={<AlertCircle size={16} />}
                variant="light"
              >
                {error}
              </Alert>
            )}

            {parsedDoc && viewTab === "summary" ? (
              <SummaryView
                doc={parsedDoc}
                noResultsLabel={t("jsonApiPage.noResults")}
                includedLabel={t("jsonApiPage.includedDivider")}
                relationshipTitle={relationshipTitle}
              />
            ) : (
              <pre className="jsonapi-response-text">
                {parsedDoc
                  ? JSON.stringify(parsedDoc, null, 2)
                  : running
                    ? t("jsonApiPage.rawPlaceholderRunning")
                    : t("jsonApiPage.rawPlaceholderEmpty")}
              </pre>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
