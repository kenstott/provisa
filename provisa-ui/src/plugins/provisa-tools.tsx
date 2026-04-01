/**
 * GraphiQL plugin: Provisa Tools
 *
 * Adds View SQL, Submit for Approval (with optional Kafka sink),
 * inside GraphiQL's plugin panel.
 */

import { useState, useCallback, useEffect, useRef } from "react";
import { useOperationsEditorState } from "@graphiql/react";
import { compileQuery, submitQuery } from "../api/admin";
import { format as formatSql } from "sql-formatter";
import type { GraphiQLPlugin } from "graphiql";

interface CompileResult {
  sql: string;
  trino_sql: string | null;
  direct_sql: string | null;
  route: string;
  route_reason: string;
  sources: string[];
  params: unknown[];
}

function SqlPanel({ compiled }: { compiled: CompileResult }) {
  const [copied, setCopied] = useState(false);
  const rawSql = compiled.direct_sql || compiled.trino_sql || compiled.sql;
  const formatted = formatSql(rawSql, {
    language: compiled.trino_sql ? "trino" : "postgresql",
    tabWidth: 2,
    keywordCase: "upper",
  });

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(formatted).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, [formatted]);

  const label = "SQL";

  return (
    <div className="provisa-tools-sql">
      <div className="provisa-tools-meta">
        <div>
          <strong>Route:</strong> {compiled.route === "trino" ? "virtual" : compiled.route}
        </div>
        <div className="provisa-tools-reason">{compiled.route_reason}</div>
        <div>
          <strong>Sources:</strong> {compiled.sources.join(", ")}
        </div>
      </div>
      <div className="provisa-tools-code-header">
        <span className="provisa-tools-label">{label}</span>
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
      <pre className="provisa-tools-code">{formatted}</pre>
      {compiled.params.length > 0 && (
        <div className="provisa-tools-params">
          <strong>Params:</strong> {JSON.stringify(compiled.params)}
        </div>
      )}
    </div>
  );
}


function ProvisaToolsContent({ roleId }: { roleId: string }) {
  const [query] = useOperationsEditorState();
  const [compiled, setCompiled] = useState<CompileResult | null>(null);
  const [submitMsg, setSubmitMsg] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  // Submission metadata
  const [showSubmitForm, setShowSubmitForm] = useState(false);
  const [businessPurpose, setBusinessPurpose] = useState("");
  const [useCases, setUseCases] = useState("");
  const [dataSensitivity, setDataSensitivity] = useState("internal");
  const [refreshFrequency, setRefreshFrequency] = useState("ad-hoc");
  const [expectedRowCount, setExpectedRowCount] = useState("<1K");
  const [ownerTeam, setOwnerTeam] = useState("");

  // Sink options
  const [showSink, setShowSink] = useState(false);
  const [sinkTopic, setSinkTopic] = useState("");
  const [sinkTrigger, setSinkTrigger] = useState("change_event");
  const [sinkKeyColumn, setSinkKeyColumn] = useState("");

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
        const result = await compileQuery(roleId, query);
        setCompiled(result);
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
      const metadata = {
        business_purpose: businessPurpose.trim() || undefined,
        use_cases: useCases.trim() || undefined,
        data_sensitivity: dataSensitivity,
        refresh_frequency: refreshFrequency,
        expected_row_count: expectedRowCount,
        owner_team: ownerTeam.trim() || undefined,
      };
      const result = await submitQuery(roleId, query, undefined, sink, metadata);
      setSubmitMsg(
        result.message +
          (sink ? ` (sink → ${sink.topic}, trigger: ${sink.trigger})` : ""),
      );
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [roleId, query, showSink, sinkTopic, sinkTrigger, sinkKeyColumn,
      businessPurpose, useCases, dataSensitivity, refreshFrequency, expectedRowCount, ownerTeam]);

  return (
    <div className="provisa-tools">
      <div className="provisa-tools-actions">
        <button
          onClick={() => setShowSubmitForm(!showSubmitForm)}
          disabled={!query.trim()}
          className={showSubmitForm ? "" : "submit-btn"}
        >
          {showSubmitForm ? "Cancel" : "Submit for Approval"}
        </button>
        {showSubmitForm && (
          <button
            onClick={handleSubmit}
            disabled={loading || !query.trim() || !businessPurpose.trim()}
            className="submit-btn"
          >
            {loading ? "Submitting..." : "Submit"}
          </button>
        )}
      </div>

      {showSubmitForm && (
        <div className="provisa-tools-metadata">
          <label>
            Business Purpose <span className="required">*</span>
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
        </div>
      )}

      <label className="provisa-tools-sink-toggle">
        <input
          type="checkbox"
          checked={showSink}
          onChange={(e) => setShowSink(e.target.checked)}
        />
        Include Kafka sink
      </label>

      {showSink && (
        <div className="provisa-tools-sink">
          <label>
            Topic
            <input
              value={sinkTopic}
              onChange={(e) => setSinkTopic(e.target.value)}
              placeholder="e.g., order-report-updates"
            />
          </label>
          <label>
            Trigger
            <select
              value={sinkTrigger}
              onChange={(e) => setSinkTrigger(e.target.value)}
            >
              <option value="change_event">On data change</option>
              <option value="schedule">On schedule</option>
              <option value="manual">Manual</option>
            </select>
          </label>
          <label>
            Key Column <span style={{ fontWeight: "normal" }}>(optional)</span>
            <input
              value={sinkKeyColumn}
              onChange={(e) => setSinkKeyColumn(e.target.value)}
              placeholder="e.g., region"
            />
          </label>
        </div>
      )}

      {error && <div className="provisa-tools-error">{error}</div>}
      {submitMsg && <div className="provisa-tools-success">{submitMsg}</div>}

      {compiled && (
        <SqlPanel compiled={compiled} />
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
