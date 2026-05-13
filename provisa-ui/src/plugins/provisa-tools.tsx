// Copyright (c) 2026 Kenneth Stott
// Canary: ea9667ae-371a-414f-be7a-2601c4eb9dfd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * GraphiQL plugin: Provisa Tools
 *
 * Adds View SQL, Submit for Approval (with optional Kafka sink),
 * inside GraphiQL's plugin panel.
 */

import { useState, useCallback, useEffect, useRef, useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { useOperationsEditorState } from "@graphiql/react";
import { compileQuery } from "../api/admin";
import type { CompileResult } from "../api/admin";
import { format as formatSql } from "sql-formatter";
import type { GraphiQLPlugin } from "@graphiql/react";
import CodeMirror from "@uiw/react-codemirror";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import * as _neo4jCypherMod from "@neo4j-cypher/codemirror";
import "@neo4j-cypher/codemirror/css/cypher-codemirror.css";
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const { getCypherLanguageExtensions: _getToolsCypherExts, cypherLinter: _toolsCypherLinter } = _neo4jCypherMod as any;
const _toolsCypherLangExts = _getToolsCypherExts({ cypherLanguage: true } as any);
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView } from "@codemirror/view";

function SqlPanel({
  compiled,
  hideSql,
  aggregated,
  onFlatSqlChange,
}: {
  compiled: CompileResult;
  hideSql?: boolean;
  aggregated?: boolean;
  onFlatSqlChange?: (v: boolean) => void;
}) {
  const [copied, setCopied] = useState(false);
  const [sqlExpanded, setSqlExpanded] = useState(true);
  const [aliasesExpanded, setAliasesExpanded] = useState(false);
  const navigate = useNavigate();
  const sqlExtensions = useMemo(
    () => [sql({ dialect: PostgreSQL }), EditorView.lineWrapping],
    []
  );
  const rawSql = compiled.semantic_sql ?? compiled.sql;
  const sqlLabel = "Semantic SQL";
  let formatted: string;
  try {
    formatted = formatSql(rawSql, {
      language: "postgresql",
      tabWidth: 2,
      keywordCase: "upper",
    });
  } catch {
    formatted = rawSql;
  }

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(formatted).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, [formatted]);

  const isCasingOnly = (a: string, b: string) =>
    a.toLowerCase().replace(/_/g, "") === b.toLowerCase().replace(/_/g, "");

  const isAliased = compiled.root_field && compiled.canonical_field &&
    compiled.root_field !== compiled.canonical_field;
  const isSignificantAlias = isAliased &&
    !isCasingOnly(compiled.root_field, compiled.canonical_field);
  const hasColumnAliases = compiled.column_aliases?.length > 0;
  const hasSignificantColumnAliases = compiled.column_aliases?.some(
    (a: { column: string; field_name: string }) => !isCasingOnly(a.column, a.field_name)
  ) ?? false;

  return (
    <div className="provisa-tools-sql">
      <div className="provisa-tools-meta">
        {compiled.root_field && (
          <div>
            <strong>Table:</strong> {compiled.root_field.includes("__") ? compiled.root_field.split("__").slice(1).join("__") : compiled.root_field}
            {isAliased && (
              <span className="provisa-tools-alias"> (alias for <em>{compiled.canonical_field}</em>)</span>
            )}
          </div>
        )}
        {(isSignificantAlias || hasSignificantColumnAliases) && (
          <div className="provisa-tools-alias-warn">
            Warning: alias adds semantic complexity — not recommended for approved queries.
          </div>
        )}
        {hasColumnAliases && (
          <div className="provisa-tools-column-aliases">
            <div
              className="provisa-tools-expandable"
              style={{ display: "flex", justifyContent: "space-between", alignItems: "center", cursor: "pointer" }}
              onClick={() => setAliasesExpanded(v => !v)}
            >
              <strong>Column Aliases</strong>
              <span className="provisa-tools-chevron">{aliasesExpanded ? "▾" : "▸"}</span>
            </div>
            {aliasesExpanded && (
              <ul style={{ margin: "4px 0 0 0", paddingLeft: 16, listStyle: "none" }}>
                {compiled.column_aliases.map((a: { column: string; field_name: string }) => (
                  <li key={a.column}>{a.column} → {a.field_name}</li>
                ))}
              </ul>
            )}
          </div>
        )}
        <div>
          <strong>Route:</strong> {compiled.route === 'virtual' ? 'federated' : compiled.route}
        </div>
        {compiled.route_reason && !compiled.route_reason.startsWith("steward override") && (
          <div className="provisa-tools-reason">{compiled.route_reason}</div>
        )}
        <div>
          <strong>Sources:</strong> {compiled.sources.join(", ")}
        </div>
      </div>
      {compiled.warnings && compiled.warnings.length > 0 && (
        <div className="provisa-tools-warnings">
          <span className="provisa-tools-label">Warnings</span>
          <ul className="provisa-tools-warnings-list">
            {compiled.warnings.map((w, i) => (
              <li key={i}>{w}</li>
            ))}
          </ul>
        </div>
      )}
      {compiled.optimizations && compiled.optimizations.filter(o => !o.startsWith("route-override")).length > 0 && (
        <div className="provisa-tools-optimizations">
          <span className="provisa-tools-label">Optimizations</span>
          <ul className="provisa-tools-optimizations-list">
            {compiled.optimizations.filter(o => !o.startsWith("route-override")).map((opt, i) => (
              <li key={i}>{opt}</li>
            ))}
          </ul>
        </div>
      )}
      {!hideSql && (
        <>
          <div
            className="provisa-tools-code-header provisa-tools-expandable"
            onClick={() => setSqlExpanded(v => !v)}
          >
            <span style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <span className="provisa-tools-label">{sqlLabel}</span>
              {onFlatSqlChange && (
                <label className="provisa-tools-option" onClick={e => e.stopPropagation()}>
                  <input type="checkbox" checked={aggregated ?? false} onChange={e => onFlatSqlChange(e.target.checked)} />
                  GraphQL-Shape (Aggregated)
                </label>
              )}
            </span>
            <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
              <button
                className="provisa-tools-copy"
                onClick={(e) => { e.stopPropagation(); handleCopy(); }}
                title="Copy SQL"
              >
                {copied ? (
                  <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2">
                    <polyline points="20 6 9 17 4 12" />
                  </svg>
                ) : (
                  <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2">
                    <rect x="9" y="9" width="13" height="13" rx="2" />
                    <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
                  </svg>
                )}
              </button>
              <button
                className="provisa-tools-copy"
                onClick={(e) => {
                  e.stopPropagation();
                  navigate("/sql", { state: { sql: formatted, autoRun: true } });
                }}
                title="Open in SQL"
              >
                <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2">
                  <line x1="5" y1="12" x2="19" y2="12" />
                  <polyline points="12 5 19 12 12 19" />
                </svg>
              </button>
              <span className="provisa-tools-chevron">{sqlExpanded ? "▾" : "▸"}</span>
            </span>
          </div>
          {sqlExpanded && (
            <>
              <CodeMirror
                value={formatted}
                extensions={sqlExtensions}
                theme={oneDark}
                editable={false}
                basicSetup={{ lineNumbers: false, foldGutter: true }}
                className="provisa-tools-code"
              />
              {(compiled.params?.length ?? 0) > 0 && (
                <div className="provisa-tools-params">
                  <strong>Params:</strong> {JSON.stringify(compiled.params)}
                </div>
              )}
            </>
          )}
        </>
      )}
    </div>
  );
}

function CombinedSqlPanel({
  compiledList,
  aggregated,
  onFlatSqlChange,
}: {
  compiledList: CompileResult[];
  aggregated?: boolean;
  onFlatSqlChange?: (v: boolean) => void;
}) {
  const [copied, setCopied] = useState(false);
  const [sqlExpanded, setSqlExpanded] = useState(true);
  const sqlExtensions = useMemo(
    () => [sql({ dialect: PostgreSQL }), EditorView.lineWrapping],
    []
  );
  const combined = compiledList
    .map((c) => {
      const raw = c.semantic_sql ?? c.sql;
      try {
        return formatSql(raw, { language: "postgresql", tabWidth: 2, keywordCase: "upper" });
      } catch {
        return raw;
      }
    })
    .join(";\n\n");

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(combined).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, [combined]);

  return (
    <div className="provisa-tools-sql">
      <div
        className="provisa-tools-code-header provisa-tools-expandable"
        onClick={() => setSqlExpanded(v => !v)}
      >
        <span style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span className="provisa-tools-label">Semantic SQL</span>
          {onFlatSqlChange && (
            <label className="provisa-tools-option" onClick={e => e.stopPropagation()}>
              <input type="checkbox" checked={aggregated ?? false} onChange={e => onFlatSqlChange(e.target.checked)} />
              GraphQL-Shape (Aggregated)
            </label>
          )}
        </span>
        <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
          <button className="provisa-tools-copy" onClick={(e) => { e.stopPropagation(); handleCopy(); }} title="Copy SQL">
            {copied ? (
              <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2">
                <polyline points="20 6 9 17 4 12" />
              </svg>
            ) : (
              <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2">
                <rect x="9" y="9" width="13" height="13" rx="2" />
                <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
              </svg>
            )}
          </button>
          <span className="provisa-tools-chevron">{sqlExpanded ? "▾" : "▸"}</span>
        </span>
      </div>
      {sqlExpanded && (
        <CodeMirror
          value={combined}
          extensions={sqlExtensions}
          theme={oneDark}
          editable={false}
          basicSetup={{ lineNumbers: false, foldGutter: true }}
          className="provisa-tools-code"
        />
      )}
    </div>
  );
}



function ProvisaToolsContent({ roleId }: { roleId: string }) {
  const [query] = useOperationsEditorState();
  const [compiled, setCompiled] = useState<CompileResult[] | null>(null);
  const [cypherQuery, setCypherQuery] = useState("");
  const [cypherError, setCypherError] = useState<string | null>(null);
  const [cypherCopied, setCypherCopied] = useState(false);
  const [cypherExpanded, setCypherExpanded] = useState(true);
  const [aggregatedSql, setAggregatedSql] = useState(true);
  const [aggregatedCypher, setAggregatedCypher] = useState(true);
  const [includeFields, setIncludeFields] = useState(false);
  const navigate = useNavigate();

  const cypherExtensions = useMemo(
    () => [..._toolsCypherLangExts, _toolsCypherLinter({ showErrors: false }), EditorView.lineWrapping],
    []
  );

  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  useEffect(() => {
    if (!query.trim()) {
      setCompiled(null);
      return;
    }
    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(async () => {
      try {
        const raw = await compileQuery(roleId, query, undefined, !aggregatedSql, includeFields ? !aggregatedCypher : false, !includeFields);
        // Normalize: single result or multi-root { queries: [...] }
        const results: CompileResult[] = Array.isArray(raw)
          ? raw
          : "queries" in raw
          ? raw.queries
          : [raw];
        setCompiled(results);
        const cypher = results.map(r => r.compiled_cypher).find(c => c);
        setCypherQuery(cypher ?? "");
        const cerr = results.map(r => r.cypher_error).find(e => e) ?? null;
        setCypherError(cypher ? null : cerr);
      } catch {
        setCompiled(null);
      }
    }, 600);
    return () => clearTimeout(debounceRef.current);
  }, [roleId, query, aggregatedSql, aggregatedCypher, includeFields]);

  return (
    <div className="provisa-tools">
      {compiled && compiled.length === 1 && (
        <SqlPanel compiled={compiled[0]} aggregated={aggregatedSql} onFlatSqlChange={setAggregatedSql} />
      )}
      {compiled && compiled.length > 1 && (
        <>
          {compiled.map((c, i) => (
            <SqlPanel key={c.root_field ?? i} compiled={c} hideSql />
          ))}
          <CombinedSqlPanel compiledList={compiled} aggregated={aggregatedSql} onFlatSqlChange={setAggregatedSql} />
        </>
      )}
      {(cypherQuery || cypherError) && (() => {
        const displayedCypher = cypherQuery;
        return (
          <div className="provisa-tools-cypher">
            <div
              className="provisa-tools-code-header provisa-tools-expandable"
              onClick={() => setCypherExpanded(v => !v)}
            >
              <span style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span className="provisa-tools-label">Cypher</span>
                <label className="provisa-tools-option" onClick={e => e.stopPropagation()}>
                  <input type="checkbox" checked={includeFields} onChange={e => setIncludeFields(e.target.checked)} />
                  Include fields
                </label>
                {includeFields && (
                  <label className="provisa-tools-option" onClick={e => e.stopPropagation()}>
                    <input type="checkbox" checked={aggregatedCypher} onChange={e => setAggregatedCypher(e.target.checked)} />
                    GraphQL-Shape (Aggregated)
                  </label>
                )}
              </span>
              <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
                {cypherQuery && (
                  <>
                    <button
                      className="provisa-tools-copy"
                      onClick={(e) => {
                        e.stopPropagation();
                        navigator.clipboard.writeText(displayedCypher).then(() => {
                          setCypherCopied(true);
                          setTimeout(() => setCypherCopied(false), 2000);
                        });
                      }}
                      title="Copy Cypher"
                    >
                      {cypherCopied ? (
                        <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2">
                          <polyline points="20 6 9 17 4 12" />
                        </svg>
                      ) : (
                        <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2">
                          <rect x="9" y="9" width="13" height="13" rx="2" />
                          <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
                        </svg>
                      )}
                    </button>
                    <button
                      className="provisa-tools-copy"
                      onClick={(e) => {
                        e.stopPropagation();
                        localStorage.setItem("provisa.graph.pending_query", displayedCypher);
                        navigate("/graph");
                      }}
                      title="Open in Graph"
                    >
                      <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2">
                        <line x1="5" y1="12" x2="19" y2="12" />
                        <polyline points="12 5 19 12 12 19" />
                      </svg>
                    </button>
                  </>
                )}
                <span className="provisa-tools-chevron">{cypherExpanded ? "▾" : "▸"}</span>
              </span>
            </div>
            {cypherExpanded && cypherQuery && (
              <CodeMirror
                value={displayedCypher}
                extensions={cypherExtensions}
                theme={oneDark}
                editable={false}
                basicSetup={{ lineNumbers: false, foldGutter: true }}
                className="provisa-tools-cypher-editor"
              />
            )}
            {cypherExpanded && cypherError && !cypherQuery && (
              <div className="provisa-tools-cypher-error">{cypherError}</div>
            )}
          </div>
        );
      })()}
    </div>
  );
}

export function provisaToolsPlugin(roleId: string): GraphiQLPlugin {
  return {
    title: "Provisa",
    icon: () => (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <path d="M12 2L2 7l10 5 10-5-10-5z" />
        <path d="M2 17l10 5 10-5" />
        <path d="M2 12l10 5 10-5" />
      </svg>
    ),
    content: () => <ProvisaToolsContent roleId={roleId} />,
  };
}
