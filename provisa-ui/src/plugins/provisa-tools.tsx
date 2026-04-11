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
import { createPortal } from "react-dom";
import { useOperationsEditorState } from "@graphiql/react";
import { compileQuery, submitQuery } from "../api/admin";
import type { CompileResult } from "../api/admin";
import { format as formatSql } from "sql-formatter";
import type { GraphiQLPlugin } from "graphiql";
import CodeMirror from "@uiw/react-codemirror";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import { cypherLanguage } from "@neo4j-cypher/codemirror";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView } from "@codemirror/view";

function SqlPanel({ compiled, hideSql }: { compiled: CompileResult; hideSql?: boolean }) {
  const [copied, setCopied] = useState(false);
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

  const isAliased = compiled.root_field && compiled.canonical_field &&
    compiled.root_field !== compiled.canonical_field;
  const hasColumnAliases = compiled.column_aliases?.length > 0;

  return (
    <div className="provisa-tools-sql">
      <div className="provisa-tools-meta">
        {compiled.root_field && (
          <div>
            <strong>Table:</strong> {compiled.root_field}
            {isAliased && (
              <span className="provisa-tools-alias"> (alias for <em>{compiled.canonical_field}</em>)</span>
            )}
          </div>
        )}
        {(isAliased || hasColumnAliases) && (
          <div className="provisa-tools-alias-warn">
            Warning: alias adds semantic complexity — not recommended for sanctioned queries.
          </div>
        )}
        {hasColumnAliases && (
          <div className="provisa-tools-column-aliases">
            Column aliases: {compiled.column_aliases.map(a => `${a.column} → ${a.field_name}`).join(", ")}
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
          <div className="provisa-tools-code-header">
            <span className="provisa-tools-label">{sqlLabel}</span>
            <button
              className="provisa-tools-copy"
              onClick={handleCopy}
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
          </div>
          <CodeMirror
            value={formatted}
            extensions={sqlExtensions}
            theme={oneDark}
            editable={false}
            basicSetup={{ lineNumbers: false, foldGutter: true }}
            className="provisa-tools-code"
          />
          {compiled.params.length > 0 && (
            <div className="provisa-tools-params">
              <strong>Params:</strong> {JSON.stringify(compiled.params)}
            </div>
          )}
        </>
      )}
    </div>
  );
}

function CombinedSqlPanel({ compiledList }: { compiledList: CompileResult[] }) {
  const [copied, setCopied] = useState(false);
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
      <div className="provisa-tools-code-header">
        <span className="provisa-tools-label">Semantic SQL</span>
        <button className="provisa-tools-copy" onClick={handleCopy} title="Copy SQL">
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
      </div>
      <CodeMirror
        value={combined}
        extensions={sqlExtensions}
        theme={oneDark}
        editable={false}
        basicSetup={{ lineNumbers: false, foldGutter: true }}
        className="provisa-tools-code"
      />
    </div>
  );
}


function ProvisaToolsContent({ roleId }: { roleId: string }) {
  const [query] = useOperationsEditorState();
  const [compiled, setCompiled] = useState<CompileResult[] | null>(null);
  const [submitMsg, setSubmitMsg] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [cypherQuery, setCypherQuery] = useState("");

  // Submission metadata
  const [showSubmitForm, setShowSubmitForm] = useState(false);
  const [businessPurpose, setBusinessPurpose] = useState("");
  const [useCases, setUseCases] = useState("");
  const [deliveryMethods, setDeliveryMethods] = useState<Set<string>>(new Set());
  const [dataSensitivity, setDataSensitivity] = useState("internal");
  const [refreshFrequency, setRefreshFrequency] = useState("ad-hoc");
  const [expectedRowCount, setExpectedRowCount] = useState("<1K");
  const [ownerTeam, setOwnerTeam] = useState("");

  // Sink options
  const [showSink, setShowSink] = useState(false);
  const [sinkTopic, setSinkTopic] = useState("");
  const [sinkTrigger, setSinkTrigger] = useState("change_event");
  const [sinkKeyColumn, setSinkKeyColumn] = useState("");

  // Schedule options
  const [showSchedule, setShowSchedule] = useState(false);
  const [scheduleCron, setScheduleCron] = useState("0 8 * * 1-5");
  const [scheduleOutputType, setScheduleOutputType] = useState("redirect");
  const [scheduleOutputFormat, setScheduleOutputFormat] = useState("parquet");
  const [scheduleDestination, setScheduleDestination] = useState("");

  const cypherExtensions = useMemo(
    () => [cypherLanguage(), EditorView.lineWrapping],
    []
  );
  const [cypherExpanded, setCypherExpanded] = useState(false);

  // Last submission result (persists after modal closes)
  const [lastSubmission, setLastSubmission] = useState<{
    queryId: number;
    operationName: string;
    message: string;
  } | null>(null);

  // Auto-compile on query change (debounced)
  const debounceRef = useRef<ReturnType<typeof setTimeout>>();
  useEffect(() => {
    if (!query.trim()) {
      setCompiled(null);
      return;
    }
    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(async () => {
      try {
        const raw = await compileQuery(roleId, query);
        // Normalize: single result or multi-root { queries: [...] }
        const results: CompileResult[] = Array.isArray(raw)
          ? raw
          : raw.queries
          ? raw.queries
          : [raw];
        setCompiled(results);
        const cypher = results.map(r => r.compiled_cypher).find(c => c);
        if (cypher) setCypherQuery(cypher);
        setError("");
      } catch {
        setCompiled(null);
      }
    }, 600);
    return () => clearTimeout(debounceRef.current);
  }, [roleId, query]);

  const handleSubmit = useCallback(async () => {
    if (!query.trim()) return;
    setError("");
    setSubmitMsg("");
    setLoading(true);
    try {
      const sink =
        showSink && sinkTopic.trim()
          ? {
              topic: sinkTopic.trim(),
              trigger: sinkTrigger,
              key_column: sinkKeyColumn.trim() || undefined,
            }
          : undefined;
      const schedule =
        showSchedule && scheduleCron.trim()
          ? {
              cron: scheduleCron.trim(),
              output_type: scheduleOutputType,
              output_format: scheduleOutputType === "redirect" ? scheduleOutputFormat : undefined,
              destination: scheduleDestination.trim() || undefined,
            }
          : undefined;
      const metadata = {
        business_purpose: businessPurpose.trim() || undefined,
        use_cases: useCases.trim() || undefined,
        data_sensitivity: dataSensitivity,
        refresh_frequency: refreshFrequency,
        expected_row_count: expectedRowCount,
        owner_team: ownerTeam.trim() || undefined,
      };
      const result = await submitQuery(roleId, query, undefined, sink, metadata, schedule, cypherQuery.trim() || undefined);
      const fullMsg =
        result.message +
        (sink ? ` (sink → ${sink.topic}, trigger: ${sink.trigger})` : "") +
        (schedule ? ` (scheduled: ${schedule.cron} → ${schedule.output_type})` : "");
      setSubmitMsg(fullMsg);
      setLastSubmission({
        queryId: result.query_id,
        operationName: result.operation_name,
        message: fullMsg,
      });
      setTimeout(closeModal, 1500);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [roleId, query, showSink, sinkTopic, sinkTrigger, sinkKeyColumn,
      showSchedule, scheduleCron, scheduleOutputType, scheduleOutputFormat, scheduleDestination,
      businessPurpose, useCases, deliveryMethods, dataSensitivity, refreshFrequency, expectedRowCount, ownerTeam, cypherQuery]);

  const closeModal = useCallback(() => {
    setShowSubmitForm(false);
    setBusinessPurpose("");
    setUseCases("");
    setDeliveryMethods(new Set());
    setDataSensitivity("internal");
    setRefreshFrequency("ad-hoc");
    setExpectedRowCount("<1K");
    setOwnerTeam("");
    setShowSink(false);
    setSinkTopic("");
    setSinkTrigger("change_event");
    setSinkKeyColumn("");
    setShowSchedule(false);
    setScheduleCron("0 8 * * 1-5");
    setScheduleOutputType("redirect");
    setScheduleOutputFormat("parquet");
    setScheduleDestination("");
  }, []);

  const modal = showSubmitForm ? createPortal(
    <div className="modal-overlay" onClick={closeModal}>
      <div className="modal modal--wide" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3>Submit for Approval</h3>
          <button className="modal-close" onClick={closeModal} title="Close">×</button>
        </div>
        <div className="modal-body provisa-tools-metadata">
          <label>
            <span>Business Purpose <span className="required">*</span></span>
            <textarea
              value={businessPurpose}
              onChange={(e) => setBusinessPurpose(e.target.value)}
              placeholder="Why is this query needed? What decision does it support?"
              rows={2}
            />
          </label>
          <label>
            Expected Use Cases
            <textarea
              value={useCases}
              onChange={(e) => setUseCases(e.target.value)}
              placeholder="Dashboards, reports, APIs, or teams that will consume this"
              rows={2}
            />
          </label>
          <div className="provisa-tools-meta-row">
            <label>
              Data Sensitivity
              <select value={dataSensitivity} onChange={(e) => setDataSensitivity(e.target.value)}>
                <option value="public">Public</option>
                <option value="internal">Internal</option>
                <option value="confidential">Confidential</option>
                <option value="restricted">Restricted</option>
              </select>
            </label>
            <label>
              Refresh Frequency
              <select value={refreshFrequency} onChange={(e) => setRefreshFrequency(e.target.value)}>
                <option value="real-time">Real-time</option>
                <option value="hourly">Hourly</option>
                <option value="daily">Daily</option>
                <option value="weekly">Weekly</option>
                <option value="ad-hoc">Ad-hoc</option>
              </select>
            </label>
            <label>
              Expected Size
              <select value={expectedRowCount} onChange={(e) => setExpectedRowCount(e.target.value)}>
                <option value="<1K">&lt;1K rows</option>
                <option value="1K-100K">1K-100K</option>
                <option value="100K+">100K+</option>
              </select>
            </label>
          </div>
          <label>
            Owner Team
            <input
              value={ownerTeam}
              onChange={(e) => setOwnerTeam(e.target.value)}
              placeholder="Team responsible for this query"
            />
          </label>

          <div className="provisa-tools-delivery">
            <span className="provisa-tools-delivery-label">Expected Delivery</span>
            <div className="provisa-tools-delivery-grid">
              {[
                { id: "json", label: "JSON (REST)" },
                { id: "arrow", label: "Arrow Flight" },
                { id: "grpc", label: "Protobuf gRPC" },
                { id: "jdbc", label: "JDBC" },
                { id: "parquet", label: "Parquet (S3)" },
                { id: "kafka", label: "Kafka Sink" },
              ].map((d) => (
                <label key={d.id} className="provisa-tools-delivery-item">
                  <input
                    type="checkbox"
                    checked={d.id === "kafka" ? showSink : deliveryMethods.has(d.id)}
                    onChange={(e) => {
                      if (d.id === "kafka") {
                        setShowSink(e.target.checked);
                      } else {
                        setDeliveryMethods((prev) => {
                          const next = new Set(prev);
                          e.target.checked ? next.add(d.id) : next.delete(d.id);
                          return next;
                        });
                      }
                    }}
                  />
                  {d.label}
                </label>
              ))}
            </div>
          </div>

          {showSink && (
            <div className="provisa-tools-sink">
              <label>
                Topic
                <input value={sinkTopic} onChange={(e) => setSinkTopic(e.target.value)} placeholder="e.g., order-report-updates" />
              </label>
              <label>
                Trigger
                <select value={sinkTrigger} onChange={(e) => setSinkTrigger(e.target.value)}>
                  <option value="change_event">On data change</option>
                  <option value="schedule">On schedule</option>
                  <option value="manual">Manual</option>
                </select>
              </label>
              <label>
                Key Column <span style={{ fontWeight: "normal" }}>(optional)</span>
                <input value={sinkKeyColumn} onChange={(e) => setSinkKeyColumn(e.target.value)} placeholder="e.g., region" />
              </label>
            </div>
          )}

          <div className="provisa-tools-schedule-toggle">
            <label className="provisa-tools-delivery-item">
              <input type="checkbox" checked={showSchedule} onChange={(e) => setShowSchedule(e.target.checked)} />
              Schedule Delivery
            </label>
          </div>

          {showSchedule && (
            <div className="provisa-tools-sink">
              <label>
                Cron Expression
                <input value={scheduleCron} onChange={(e) => setScheduleCron(e.target.value)} placeholder="e.g., 0 8 * * 1-5 (8AM Mon–Fri)" />
              </label>
              <label>
                Output Type
                <select value={scheduleOutputType} onChange={(e) => setScheduleOutputType(e.target.value)}>
                  <option value="redirect">File (S3/redirect)</option>
                  <option value="webhook">Endpoint (webhook)</option>
                  <option value="kafka">Kafka topic</option>
                </select>
              </label>
              {scheduleOutputType === "redirect" && (
                <label>
                  Format
                  <select value={scheduleOutputFormat} onChange={(e) => setScheduleOutputFormat(e.target.value)}>
                    <option value="parquet">Parquet</option>
                    <option value="csv">CSV</option>
                    <option value="json">JSON</option>
                    <option value="ndjson">NDJSON</option>
                    <option value="arrow">Arrow</option>
                  </select>
                </label>
              )}
              <label>
                Destination <span style={{ fontWeight: "normal" }}>
                  {scheduleOutputType === "redirect" ? "(S3 key prefix, optional)" :
                   scheduleOutputType === "webhook" ? "(URL)" : "(Kafka topic)"}
                </span>
                <input
                  value={scheduleDestination}
                  onChange={(e) => setScheduleDestination(e.target.value)}
                  placeholder={
                    scheduleOutputType === "redirect" ? "e.g., reports/daily" :
                    scheduleOutputType === "webhook" ? "https://..." : "e.g., my-topic"
                  }
                />
              </label>
            </div>
          )}

          {error && <div className="provisa-tools-error">{error}</div>}
          {submitMsg && <div className="provisa-tools-success">{submitMsg}</div>}
        </div>
        <div className="modal-actions" style={{ marginTop: "1rem", paddingTop: "0.75rem", borderTop: "1px solid var(--border)" }}>
          <button onClick={closeModal}>Cancel</button>
          <button
            className="submit-btn"
            onClick={handleSubmit}
            disabled={loading || !query.trim() || !businessPurpose.trim()}
          >
            {loading ? "Submitting..." : "Submit"}
          </button>
        </div>
      </div>
    </div>,
    document.body,
  ) : null;

  return (
    <div className="provisa-tools">
      <div className="provisa-tools-actions">
        <button
          onClick={() => setShowSubmitForm(true)}
          disabled={!query.trim()}
          className="submit-btn"
        >
          Submit for Approval
        </button>
      </div>
      {modal}
      {lastSubmission && (
        <div className="provisa-tools-last-submission">
          <div className="provisa-tools-last-submission-header">
            <span className="provisa-tools-label">Last Submission</span>
            <button
              className="provisa-tools-dismiss"
              onClick={() => setLastSubmission(null)}
              title="Dismiss"
            >×</button>
          </div>
          <div className="provisa-tools-last-submission-body">
            <div><strong>Query:</strong> {lastSubmission.operationName}</div>
            <div><strong>ID:</strong> {lastSubmission.queryId}</div>
            <div className="provisa-tools-last-submission-msg">{lastSubmission.message}</div>
          </div>
        </div>
      )}
      {compiled && compiled.length === 1 && (
        <SqlPanel compiled={compiled[0]} />
      )}
      {compiled && compiled.length > 1 && (
        <>
          {compiled.map((c, i) => (
            <SqlPanel key={c.root_field ?? i} compiled={c} hideSql />
          ))}
          <CombinedSqlPanel compiledList={compiled} />
        </>
      )}
      {cypherQuery && (
        <div className="provisa-tools-cypher">
          <div
            className="provisa-tools-code-header provisa-tools-expandable"
            onClick={() => setCypherExpanded(v => !v)}
          >
            <span className="provisa-tools-label">Cypher</span>
            <span className="provisa-tools-chevron">{cypherExpanded ? "▾" : "▸"}</span>
          </div>
          {cypherExpanded && (
            <CodeMirror
              value={cypherQuery}
              extensions={cypherExtensions}
              theme={oneDark}
              onChange={(val) => setCypherQuery(val)}
              basicSetup={{ lineNumbers: false, foldGutter: true }}
              className="provisa-tools-cypher-editor"
            />
          )}
        </div>
      )}
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
