// Copyright (c) 2026 Kenneth Stott
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

function ResourceCard({ item, i, includedSet }: { item: JsonApiResource; i: number; includedSet?: Set<string> }) {
  const rels = item.relationships
    ? Object.entries(item.relationships)
    : [];
  return (
    <div id={`res-${item.type}-${item.id ?? i}`} className="jsonapi-resource-card">
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
                  <a
                    key={`${relName}-${r.id}`}
                    className="jsonapi-rel-link"
                    href={`#res-${r.type}-${r.id}`}
                    title={`${relName} → ${r.type} #${r.id}`}
                  >
                    {relName} #{r.id}
                  </a>
                ));
            })}
          </span>
        )}
      </div>
      {item.attributes && (
        <div className="jsonapi-attr-scroll">
          <table className="jsonapi-attr-table">
            <tbody>
              {Object.entries(item.attributes as Record<string, unknown>)
                .filter(([k]) => !k.startsWith("_"))
                .map(([k, v]) => (
                  <tr key={k}>
                    <td className="jsonapi-attr-key">{k}</td>
                    <td className="jsonapi-attr-val">
                      {v === null || v === undefined
                        ? <span className="jsonapi-attr-null">null</span>
                        : typeof v === "object"
                          ? JSON.stringify(v)
                          : String(v)}
                    </td>
                  </tr>
                ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function SummaryView({ doc }: { doc: JsonApiDocument }) {
  const items: JsonApiResource[] = Array.isArray(doc.data)
    ? doc.data
    : doc.data
      ? [doc.data]
      : [];
  const included: JsonApiResource[] = Array.isArray(doc.included) ? doc.included : [];
  const includedSet = new Set<string>(included.map((r) => `${r.type}::${r.id}`));
  if (items.length === 0) {
    return <div className="jsonapi-summary-empty">No results</div>;
  }
  return (
    <div className="jsonapi-summary">
      {items.map((item, i) => (
        <ResourceCard key={`${item.type}-${item.id ?? i}`} item={item} i={i} includedSet={includedSet} />
      ))}
      {included.length > 0 && (
        <>
          <div className="jsonapi-included-divider">Included</div>
          {included.map((item, i) => (
            <ResourceCard key={`inc-${item.type}-${item.id ?? i}`} item={item} i={i} />
          ))}
        </>
      )}
    </div>
  );
}

const JSONAPI_SETTINGS_KEY = "provisa.jsonapi.settings";

export function JsonApiPage() {
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

  const displayUrl = activeUrl || url;

  return (
    <div className="jsonapi-page page">
      <div className="jsonapi-layout">
        <div className="jsonapi-sidebar">
          <div className="jsonapi-sidebar-header">
            <h2 className="jsonapi-section-title">JSON:API Explorer</h2>
            <a className="jsonapi-spec-link" href={specUrl} download="jsonapi-openapi.json">
              ⬇ Spec
            </a>
          </div>
          <p className="jsonapi-desc">
            Sparse fieldsets, filtering, sorting, and pagination. Governance
            applies uniformly regardless of wire protocol.
          </p>

          <label className="jsonapi-label">Table</label>
          <select
            className="jsonapi-select"
            value={effectiveSelectedTable}
            onChange={(e) => setSelectedTable(e.target.value)}
            disabled={loading}
          >
            <option value="">{loading ? "Loading…" : "Select a table"}</option>
            {Object.entries(domainGroups).map(([domain, domainTables]) => {
              const domainDesc = domainDescMap[domain];
              const groupLabel = domainDesc ? `${domain} — ${domainDesc}` : domain;
              return (
                <optgroup key={domain} label={groupLabel}>
                  {domainTables.map((t) => (
                    <option
                      key={t.id}
                      value={`${t.domainId}/${t.tableName}`}
                      title={t.description ?? undefined}
                    >
                      {t.alias ?? t.tableName}
                    </option>
                  ))}
                </optgroup>
              );
            })}
          </select>
          {tableObj?.description && (
            <p className="jsonapi-table-desc">{tableObj.description}</p>
          )}

          {columnNames.length > 0 && (
            <>
              <div className="jsonapi-dropdown-group">
                <button
                  className="jsonapi-dropdown-trigger"
                  onClick={() => setFieldsOpen((o) => !o)}
                >
                  <span>Fields</span>
                  <span className="jsonapi-dropdown-meta">
                    {checkedFields.size === 0 || checkedFields.size === columnNames.length
                      ? "all"
                      : `${checkedFields.size} selected`}
                    {" "}{fieldsOpen ? "▴" : "▾"}
                  </span>
                </button>
                {fieldsOpen && (
                  <div className="jsonapi-field-list">
                    {(tableObj?.columns ?? []).map((col) => {
                      const gqlName = toGqlName(col);
                      return (
                        <label
                          key={col.columnName}
                          className="jsonapi-field-item"
                          title={col.description ?? undefined}
                        >
                          <input
                            type="checkbox"
                            checked={checkedFields.has(gqlName)}
                            onChange={() => toggleField(gqlName)}
                          />
                          <span className="jsonapi-field-name">{col.alias ?? col.columnName}</span>
                        </label>
                      );
                    })}
                  </div>
                )}
              </div>

              {relationshipNames.length > 0 && (
                <div className="jsonapi-dropdown-group">
                  <button
                    className="jsonapi-dropdown-trigger"
                    onClick={() => setIncludeOpen((o) => !o)}
                  >
                    <span>Include</span>
                    <span className="jsonapi-dropdown-meta">
                      {checkedIncludes.size === 0 ? "none" : `${checkedIncludes.size} selected`}
                      {" "}{includeOpen ? "▴" : "▾"}
                    </span>
                  </button>
                  {includeOpen && (
                    <div className="jsonapi-field-list">
                      {relationshipNames.map((rel) => (
                        <label key={rel} className="jsonapi-field-item">
                          <input
                            type="checkbox"
                            checked={checkedIncludes.has(rel)}
                            onChange={() => toggleInclude(rel)}
                          />
                          <span className="jsonapi-field-name">{rel}</span>
                        </label>
                      ))}
                    </div>
                  )}
                </div>
              )}

              <div className="jsonapi-section-divider">
                Filter <span className="jsonapi-section-hint">optional</span>
              </div>
              <div className="jsonapi-row">
                <select
                  className="jsonapi-select jsonapi-select-flex"
                  value={filterField}
                  onChange={(e) => setFilterField(e.target.value)}
                >
                  <option value="">— field —</option>
                  {(tableObj?.columns ?? []).map((c) => (
                    <option
                      key={c.columnName}
                      value={toGqlName(c)}
                      title={c.description ?? undefined}
                    >
                      {c.alias ?? c.columnName}
                    </option>
                  ))}
                </select>
                <select
                  className="jsonapi-select jsonapi-select-op"
                  value={filterOp}
                  onChange={(e) => setFilterOp(e.target.value)}
                  disabled={!filterField}
                >
                  {["eq", "neq", "gt", "gte", "lt", "lte", "like"].map((op) => (
                    <option key={op} value={op}>{op}</option>
                  ))}
                </select>
              </div>
              <input
                className="jsonapi-input"
                placeholder="value"
                value={filterValue}
                onChange={(e) => setFilterValue(e.target.value)}
                disabled={!filterField}
              />

              <div className="jsonapi-section-divider">
                Sort <span className="jsonapi-section-hint">optional</span>
              </div>
              <div className="jsonapi-row">
                <select
                  className="jsonapi-select jsonapi-select-flex"
                  value={sortField}
                  onChange={(e) => setSortField(e.target.value)}
                >
                  <option value="">— field —</option>
                  {(tableObj?.columns ?? []).map((c) => (
                    <option
                      key={c.columnName}
                      value={toGqlName(c)}
                      title={c.description ?? undefined}
                    >
                      {c.alias ?? c.columnName}
                    </option>
                  ))}
                </select>
                <select
                  className="jsonapi-select jsonapi-select-op"
                  value={sortDir}
                  onChange={(e) => setSortDir(e.target.value as "asc" | "desc")}
                  disabled={!sortField}
                >
                  <option value="asc">asc</option>
                  <option value="desc">desc</option>
                </select>
              </div>
            </>
          )}

          <div className="jsonapi-section-divider" style={{ marginTop: columnNames.length ? undefined : "0.5rem" }}>
            Pagination
          </div>
          <label className="jsonapi-label">Page size</label>
          <input
            className="jsonapi-input"
            type="number"
            min={1}
            max={1000}
            value={pageSize}
            onChange={(e) => setPageSize(e.target.value)}
          />

          <button
            className="jsonapi-run-btn"
            onClick={handleRun}
            disabled={running || !effectiveSelectedTable}
          >
            {running ? "Running…" : "▶ Execute"}
          </button>
        </div>

        <div className="jsonapi-main">
          <div className="jsonapi-url-bar">
            <span className="jsonapi-method">GET</span>
            <span className="jsonapi-url">{displayUrl || "—"}</span>
            {displayUrl && (
              <button className="jsonapi-copy-btn" onClick={handleCopy}>
                {copied ? "✓" : "Copy"}
              </button>
            )}
          </div>

          <div className="jsonapi-panel">
            <div className="jsonapi-panel-header">
              <div className="jsonapi-panel-header-left">
                <span>Response</span>
                {resultMeta && (
                  <span className="jsonapi-result-count">
                    {resultMeta.rangeStart !== undefined && resultMeta.rangeEnd !== undefined
                      ? `${resultMeta.rangeStart}–${resultMeta.rangeEnd}`
                      : resultMeta.count}
                    {resultMeta.total !== undefined ? ` of ${resultMeta.total}` : ""}
                  </span>
                )}
              </div>
              {parsedDoc && (
                <div className="jsonapi-tab-bar">
                  <button
                    className={`jsonapi-tab${viewTab === "summary" ? " active" : ""}`}
                    onClick={() => setViewTab("summary")}
                  >
                    Summary
                  </button>
                  <button
                    className={`jsonapi-tab${viewTab === "raw" ? " active" : ""}`}
                    onClick={() => setViewTab("raw")}
                  >
                    Raw
                  </button>
                </div>
              )}
            </div>

            {(paginationLinks?.prev || paginationLinks?.next) && (
              <div className="jsonapi-pagination-bar">
                <button
                  className="jsonapi-page-btn"
                  disabled={!paginationLinks.first}
                  onClick={() => handlePaginate(paginationLinks!.first)}
                  title="First page"
                >⟪</button>
                <button
                  className="jsonapi-page-btn"
                  disabled={!paginationLinks.prev}
                  onClick={() => handlePaginate(paginationLinks!.prev)}
                  title="Previous page"
                >‹ Prev</button>
                <button
                  className="jsonapi-page-btn"
                  disabled={!paginationLinks.next}
                  onClick={() => handlePaginate(paginationLinks!.next)}
                  title="Next page"
                >Next ›</button>
                <button
                  className="jsonapi-page-btn"
                  disabled={!paginationLinks.last}
                  onClick={() => handlePaginate(paginationLinks!.last)}
                  title="Last page"
                >⟫</button>
              </div>
            )}

            {error && <div className="jsonapi-error">{error}</div>}

            {parsedDoc && viewTab === "summary" ? (
              <SummaryView doc={parsedDoc} />
            ) : (
              <pre className="jsonapi-response-text">
                {parsedDoc
                  ? JSON.stringify(parsedDoc, null, 2)
                  : running
                    ? "Running…"
                    : "Execute a query to see results."}
              </pre>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
