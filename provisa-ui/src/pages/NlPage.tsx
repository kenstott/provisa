// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useRef, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext";
import { submitNlQuery, streamNlResult, type NlBranchEvent } from "../api/admin";
import "./NlPage.css";

const EXPLORER_ROUTES: Record<string, { path: string; stateKey: string }> = {
  sql: { path: "/sql", stateKey: "sql" },
  graphql: { path: "/query", stateKey: "query" },
  cypher: { path: "/graph", stateKey: "query" },
};

const GUIDE_KEY = "provisa.nl.guide.collapsed";

const EXAMPLES = [
  "Show all customers whose orders total more than 10,000, grouped by region",
  "List the top 5 products by revenue in the last 30 days",
  "Find all users who have never placed an order",
];

function GuidanceBanner() {
  const [collapsed, setCollapsed] = useState(
    () => localStorage.getItem(GUIDE_KEY) === "1",
  );

  function toggle() {
    const next = !collapsed;
    setCollapsed(next);
    localStorage.setItem(GUIDE_KEY, next ? "1" : "0");
  }

  return (
    <div className="nl-guide">
      <button className="nl-guide-toggle" onClick={toggle} aria-expanded={!collapsed}>
        <span className="nl-guide-title">How to write a good question</span>
        <span className="nl-guide-chevron">{collapsed ? "▸" : "▾"}</span>
      </button>
      {!collapsed && (
        <div className="nl-guide-body">
          <p className="nl-guide-desc">
            This tool generates queries directly from your schema — it does not reason over
            free-form text or general knowledge. Phrase your question as a composition of the
            tables, fields, and relationships that exist in your data.
          </p>
          <ul className="nl-guide-rules">
            <li>Use the names of your entities, not synonyms (<em>Orders</em>, not <em>purchases</em>)</li>
            <li>Specify filters, groupings, and aggregations the way you would in a query</li>
            <li>If a field or relationship is not in your schema, it cannot be queried</li>
          </ul>
          <div className="nl-guide-examples-label">Examples</div>
          <ul className="nl-guide-examples">
            {EXAMPLES.map((ex) => (
              <li key={ex} className="nl-guide-example">{ex}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

type BranchState = {
  query: string | null;
  result: unknown | null;
  error: string | null;
  loading: boolean;
};

const EMPTY_BRANCH: BranchState = { query: null, result: null, error: null, loading: false };

const TARGETS = ["sql", "graphql", "cypher"] as const;
type Target = (typeof TARGETS)[number];

const LABELS: Record<Target, string> = { sql: "SQL", graphql: "GraphQL", cypher: "Cypher" };

export function NlPage() {
  const { role } = useAuth();
  const navigate = useNavigate();
  const NL_QUESTION_KEY = "nl-question";
  const NL_BRANCHES_KEY = "nl-branches";
  const [question, setQuestion] = useState(() => localStorage.getItem(NL_QUESTION_KEY) ?? "");
  const [submitting, setSubmitting] = useState(false);
  const [globalError, setGlobalError] = useState<string | null>(null);
  const [branches, setBranches] = useState<Record<Target, BranchState>>(() => {
    try {
      const saved = localStorage.getItem(NL_BRANCHES_KEY);
      return saved ? JSON.parse(saved) : { sql: EMPTY_BRANCH, graphql: EMPTY_BRANCH, cypher: EMPTY_BRANCH };
    } catch {
      return { sql: EMPTY_BRANCH, graphql: EMPTY_BRANCH, cypher: EMPTY_BRANCH };
    }
  });
  const [hasResults, setHasResults] = useState(
    () => localStorage.getItem(NL_BRANCHES_KEY) !== null,
  );
  const cancelRef = useRef<(() => void) | null>(null);

  const saveBranches = useCallback((next: Record<Target, BranchState>) => {
    localStorage.setItem(NL_BRANCHES_KEY, JSON.stringify(next));
    setBranches(next);
  }, []);

  const handleSubmit = useCallback(async () => {
    const q = question.trim();
    if (!q || submitting) return;

    cancelRef.current?.();
    cancelRef.current = null;

    const roleId = role ? role.id : "default";
    setGlobalError(null);
    setHasResults(true);
    setSubmitting(true);
    saveBranches({
      sql: { ...EMPTY_BRANCH, loading: true },
      graphql: { ...EMPTY_BRANCH, loading: true },
      cypher: { ...EMPTY_BRANCH, loading: true },
    });

    let jobId: string;
    try {
      const res = await submitNlQuery(q, roleId);
      jobId = res.job_id;
    } catch (e) {
      setGlobalError(e instanceof Error ? e.message : String(e));
      setSubmitting(false);
      saveBranches({ sql: EMPTY_BRANCH, graphql: EMPTY_BRANCH, cypher: EMPTY_BRANCH });
      return;
    }

    const stop = streamNlResult(
      jobId,
      (event: NlBranchEvent) => {
        const t = event.target as Target;
        setBranches((prev) => {
          const next = { ...prev, [t]: { query: event.query, result: event.result, error: event.error, loading: false } };
          localStorage.setItem(NL_BRANCHES_KEY, JSON.stringify(next));
          return next;
        });
      },
      (_state) => {
        setSubmitting(false);
        setBranches((prev) => {
          const next = { ...prev };
          for (const t of TARGETS) {
            if (next[t].loading) next[t] = { ...EMPTY_BRANCH };
          }
          localStorage.setItem(NL_BRANCHES_KEY, JSON.stringify(next));
          return next;
        });
      },
      (msg) => {
        setGlobalError(msg);
        setSubmitting(false);
        setBranches((prev) => {
          const next = { ...prev };
          for (const t of TARGETS) {
            if (next[t].loading) next[t] = { ...EMPTY_BRANCH, error: msg };
          }
          localStorage.setItem(NL_BRANCHES_KEY, JSON.stringify(next));
          return next;
        });
      },
    );
    cancelRef.current = stop;
  }, [question, submitting, role]);

  const openInExplorer = useCallback((target: Target, query: string) => {
    const route = EXPLORER_ROUTES[target];
    navigate(route.path, { state: { [route.stateKey]: query, autoRun: true } });
  }, [navigate]);

  return (
    <div className="nl-page">
      <GuidanceBanner />
      <div className="nl-input-bar">
        <textarea
          className="nl-textarea"
          placeholder="Ask a question in plain English…"
          value={question}
          rows={2}
          onChange={(e) => { setQuestion(e.target.value); localStorage.setItem(NL_QUESTION_KEY, e.target.value); }}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              void handleSubmit();
            }
          }}
        />
        <button
          className="nl-submit-btn"
          disabled={submitting || !question.trim()}
          onClick={() => void handleSubmit()}
        >
          {submitting ? "Generating…" : "Generate"}
        </button>
      </div>

      {globalError && <div className="nl-global-error">{globalError}</div>}

      {hasResults && (
        <div className="nl-panels">
          {TARGETS.map((t) => (
            <BranchPanel key={t} label={LABELS[t]} target={t} branch={branches[t]} onOpen={openInExplorer} />
          ))}
        </div>
      )}
    </div>
  );
}

function BranchPanel({
  label,
  target,
  branch,
  onOpen,
}: {
  label: string;
  target: Target;
  branch: BranchState;
  onOpen: (target: Target, query: string) => void;
}) {
  const notApplicable = branch.error === "NOT_APPLICABLE";
  return (
    <div className="nl-branch-panel">
      <div className="nl-branch-header">
        <span className="nl-branch-label">{label}</span>
        {!branch.loading && branch.query && (
          <button
            className="nl-open-btn"
            title={`Open in ${label} explorer`}
            onClick={() => onOpen(target, branch.query!)}
          >
            Open in {label}
          </button>
        )}
      </div>
      <div className="nl-branch-body">
        {branch.loading && (
          <div className="nl-branch-loading" style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
            <span className="btn-spinner" style={{ flexShrink: 0 }} />
            Generating…
          </div>
        )}
        {!branch.loading && notApplicable && (
          <div className="nl-branch-na">Not applicable for this query type</div>
        )}
        {!branch.loading && !notApplicable && branch.error && (
          <div className="nl-branch-error">{branch.error}</div>
        )}
        {!branch.loading && branch.query && (
          <pre className="nl-branch-query">{branch.query}</pre>
        )}
        {!branch.loading && !branch.query && !branch.error && (
          <div className="nl-branch-empty">No query generated</div>
        )}
        {!branch.loading && branch.result != null && (
          <ResultTable result={branch.result} />
        )}
      </div>
    </div>
  );
}

function ResultTable({ result }: { result: unknown }) {
  if (
    typeof result !== "object" ||
    result === null ||
    !Array.isArray((result as { rows?: unknown }).rows)
  ) {
    return <pre className="nl-branch-result-raw">{JSON.stringify(result, null, 2)}</pre>;
  }

  const { columns, rows } = result as { columns: string[]; rows: Record<string, unknown>[] };
  if (!rows.length) return <div className="nl-branch-empty">No rows returned</div>;

  return (
    <div className="nl-result-table-wrap">
      <table className="nl-result-table">
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 100).map((row, i) => (
            <tr key={i}>
              {columns.map((c, j) => (
                <td key={j}>{row[c] == null ? "" : String(row[c])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > 100 && (
        <div className="nl-result-truncated">Showing 100 of {rows.length} rows</div>
      )}
    </div>
  );
}
