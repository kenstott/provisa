/**
 * GraphiQL plugin: Provisa Tools
 *
 * Adds View SQL and Submit for Approval functionality inside GraphiQL.
 * Uses the GraphiQL editor state hooks to access the current query.
 */

import { useState, useCallback } from "react";
import { useOperationsEditorState } from "@graphiql/react";
import { compileQuery, submitQuery } from "../api/admin";
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

function ProvisaToolsContent({ roleId }: { roleId: string }) {
  const [query] = useOperationsEditorState();
  const [compiled, setCompiled] = useState<CompileResult | null>(null);
  const [submitMsg, setSubmitMsg] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const handleViewSql = useCallback(async () => {
    if (!query.trim()) return;
    setError("");
    setCompiled(null);
    setLoading(true);
    try {
      const result = await compileQuery(roleId, query);
      setCompiled(result);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [roleId, query]);

  const handleSubmit = useCallback(async () => {
    if (!query.trim()) return;
    setError("");
    setSubmitMsg("");
    setLoading(true);
    try {
      const result = await submitQuery(roleId, query);
      setSubmitMsg(result.message);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [roleId, query]);

  return (
    <div className="provisa-tools">
      <div className="provisa-tools-actions">
        <button onClick={handleViewSql} disabled={loading || !query.trim()}>
          View SQL
        </button>
        <button
          onClick={handleSubmit}
          disabled={loading || !query.trim()}
          className="submit-btn"
        >
          Submit for Approval
        </button>
      </div>

      {error && <div className="provisa-tools-error">{error}</div>}
      {submitMsg && <div className="provisa-tools-success">{submitMsg}</div>}

      {compiled && (
        <div className="provisa-tools-sql">
          <div className="provisa-tools-meta">
            <div>
              <strong>Route:</strong> {compiled.route}
            </div>
            <div className="provisa-tools-reason">{compiled.route_reason}</div>
            <div>
              <strong>Sources:</strong> {compiled.sources.join(", ")}
            </div>
          </div>
          <div className="provisa-tools-label">
            {compiled.direct_sql
              ? "Direct SQL"
              : compiled.trino_sql
                ? "Trino SQL"
                : "Compiled SQL"}
          </div>
          <pre className="provisa-tools-code">
            {compiled.direct_sql || compiled.trino_sql || compiled.sql}
          </pre>
          {compiled.params.length > 0 && (
            <div className="provisa-tools-params">
              <strong>Params:</strong> {JSON.stringify(compiled.params)}
            </div>
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
