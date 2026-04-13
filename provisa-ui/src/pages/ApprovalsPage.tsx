// Copyright (c) 2026 Kenneth Stott
// Canary: 7ccb073a-ee3e-4940-8bd7-4812ca8c39af
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useCallback } from "react";
import { FilterInput } from "../components/admin/FilterInput";
import CodeMirror from "@uiw/react-codemirror";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import { graphql } from "cm6-graphql";
import { cypherLanguage } from "@neo4j-cypher/codemirror";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView } from "@codemirror/view";
import { ConfirmDialog } from "../components/ConfirmDialog";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8001";

interface GovernedQuery {
  id: number;
  queryText: string;
  compiledSql: string;
  status: string;
  stableId: string | null;
  developerId: string | null;
  approvedBy: string | null;
  sinkTopic: string | null;
  sinkTrigger: string | null;
  sinkKeyColumn: string | null;
  businessPurpose: string | null;
  useCases: string | null;
  dataSensitivity: string | null;
  refreshFrequency: string | null;
  expectedRowCount: string | null;
  ownerTeam: string | null;
  expiryDate: string | null;
  visibleTo: string[];
  scheduleCron: string | null;
  scheduleOutputType: string | null;
  scheduleOutputFormat: string | null;
  scheduleDestination: string | null;
  compiledCypher: string | null;
}

const GQL_FIELDS = `
  id queryText compiledSql status stableId developerId approvedBy
  sinkTopic sinkTrigger sinkKeyColumn businessPurpose useCases
  dataSensitivity refreshFrequency expectedRowCount ownerTeam expiryDate
  visibleTo scheduleCron scheduleOutputType scheduleOutputFormat scheduleDestination
  compiledCypher
`;

const STATUS_FILTERS = ["pending", "approved", "rejected", "revoked"] as const;
type StatusFilter = typeof STATUS_FILTERS[number];

async function gql<T>(query: string): Promise<T> {
  const resp = await fetch(`${API_BASE}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  const json = await resp.json();
  return json.data;
}

async function fetchAll(): Promise<GovernedQuery[]> {
  const data = await gql<{ governedQueries: GovernedQuery[] }>(
    `{ governedQueries { ${GQL_FIELDS} } }`
  );
  return data?.governedQueries ?? [];
}

async function fetchRoleIds(): Promise<string[]> {
  const data = await gql<{ roles: { id: string }[] }>(`{ roles { id } }`);
  return (data?.roles ?? []).map((r) => r.id);
}

function queryDisplayName(q: GovernedQuery): string {
  const m = q.queryText.match(/(?:query|mutation)\s+(\w+)/);
  return m ? m[1] : `Query #${q.id}`;
}

function StatusBadge({ status }: { status: string }) {
  return <span className={`status-badge status-${status}`}>{status}</span>;
}

interface ExpandedActionsProps {
  query: GovernedQuery;
  roleIds: string[];
  onDone: () => void;
}

function ExpandedActions({ query, roleIds, onDone }: ExpandedActionsProps) {
  const [visibleTo, setVisibleTo] = useState<string[]>(query.visibleTo ?? []);
  const [rejectReason, setRejectReason] = useState("");
  const [showReject, setShowReject] = useState(false);
  const [busy, setBusy] = useState(false);

  const toggleRole = (rid: string) =>
    setVisibleTo((prev) =>
      prev.includes(rid) ? prev.filter((r) => r !== rid) : [...prev, rid]
    );

  const toggleAll = () =>
    setVisibleTo((prev) =>
      prev.includes("*") ? prev.filter((r) => r !== "*") : [...prev, "*"]
    );

  const handleApprove = async () => {
    setBusy(true);
    const vArg = JSON.stringify(visibleTo);
    await gql(
      `mutation { approveQuery(queryId: ${query.id}, visibleTo: ${vArg}) { success } }`
    );
    setBusy(false);
    onDone();
  };

  const handleReject = async () => {
    if (!rejectReason.trim()) return;
    setBusy(true);
    const escaped = rejectReason.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
    await gql(
      `mutation { rejectQuery(queryId: ${query.id}, reason: "${escaped}") { success } }`
    );
    setBusy(false);
    onDone();
  };

  const handleRevoke = async () => {
    setBusy(true);
    await gql(`mutation { revokeQuery(queryId: ${query.id}) { success } }`);
    setBusy(false);
    onDone();
  };

  if (query.status === "approved") {
    return (
      <div className="approval-actions">
        <ConfirmDialog
          title={`Revoke approval for "${queryDisplayName(query)}"?`}
          consequence="The stable ID will be cleared and any scheduled job removed."
          onConfirm={handleRevoke}
        >
          {(open) => (
            <button className="reject" disabled={busy} onClick={open}>
              Revoke
            </button>
          )}
        </ConfirmDialog>
      </div>
    );
  }

  if (query.status === "pending") {
    return (
      <div className="approval-detail-actions">
        <div className="visible-to-select">
          <label>Visible to (roles):</label>
          <div className="role-checkboxes">
            <label>
              <input
                type="checkbox"
                checked={visibleTo.includes("*")}
                onChange={toggleAll}
              />
              All roles (*)
            </label>
            {roleIds.map((rid) => (
              <label key={rid}>
                <input
                  type="checkbox"
                  checked={visibleTo.includes(rid)}
                  onChange={() => toggleRole(rid)}
                />
                {rid}
              </label>
            ))}
          </div>
        </div>
        <div className="approval-actions">
          <ConfirmDialog
            title={`Approve "${queryDisplayName(query)}"?`}
            consequence="This query will become available for production use."
            onConfirm={handleApprove}
          >
            {(open) => (
              <button className="approve" disabled={busy} onClick={open}>
                Approve
              </button>
            )}
          </ConfirmDialog>
          <button
            className="reject"
            disabled={busy}
            onClick={() => setShowReject((v) => !v)}
          >
            Reject
          </button>
        </div>
        {showReject && (
          <div className="reject-form">
            <label htmlFor="reject-reason">
              Rejection reason (required):
              <textarea
                id="reject-reason"
                value={rejectReason}
                onChange={(e) => setRejectReason(e.target.value)}
                placeholder="Explain what needs to change..."
                rows={3}
              />
            </label>
            <div className="reject-actions">
              <button onClick={() => { setShowReject(false); setRejectReason(""); }}>
                Cancel
              </button>
              <button
                className="destructive"
                disabled={!rejectReason.trim() || busy}
                onClick={handleReject}
              >
                Submit Rejection
              </button>
            </div>
          </div>
        )}
      </div>
    );
  }

  return null;
}

interface QueryRowProps {
  query: GovernedQuery;
  expanded: boolean;
  roleIds: string[];
  onToggle: () => void;
  onDone: () => void;
}

function QueryRow({ query, expanded, roleIds, onToggle, onDone }: QueryRowProps) {
  const name = queryDisplayName(query);

  return (
    <div className={`approval-row${expanded ? " approval-row--expanded" : ""}`}>
      <div
        className="approval-row-summary"
        onClick={onToggle}
        role="button"
        tabIndex={0}
        onKeyDown={(e) => e.key === "Enter" && onToggle()}
      >
        <div className="approval-row-primary">
          <span className="approval-row-name">{name}</span>
          <StatusBadge status={query.status} />
          <span className="approval-row-submitter">{query.developerId ?? "unknown"}</span>
          <span className="approval-row-chevron">{expanded ? "▲" : "▼"}</span>
        </div>
        {(query.ownerTeam || query.businessPurpose || query.useCases) && (
          <div className="approval-row-secondary">
            {query.ownerTeam && <span className="approval-row-team">{query.ownerTeam}</span>}
            {query.businessPurpose && (
              <span className="approval-row-meta-item">
                <span className="approval-row-meta-label">Purpose:</span>
                {query.businessPurpose}
              </span>
            )}
            {query.useCases && (
              <span className="approval-row-meta-item">
                <span className="approval-row-meta-label">Use cases:</span>
                {query.useCases}
              </span>
            )}
          </div>
        )}
      </div>

      {expanded && (
        <div className="approval-row-detail">
          <div className="approval-query-split">
            <section className="detail-section">
              <h4>Query</h4>
              <CodeMirror
                value={query.queryText}
                extensions={[graphql(), EditorView.lineWrapping]}
                theme={oneDark}
                editable={false}
                basicSetup={{ lineNumbers: false, foldGutter: true }}
                className="approval-query-editor"
              />
            </section>

            {query.compiledSql && (() => {
              let combined: string;
              try {
                const parsed = JSON.parse(query.compiledSql);
                combined = Array.isArray(parsed) ? parsed.join(";\n\n") : query.compiledSql;
              } catch {
                combined = query.compiledSql;
              }
              return (
                <section className="detail-section">
                  <h4>Semantic SQL</h4>
                  <CodeMirror
                    value={combined}
                    extensions={[sql({ dialect: PostgreSQL }), EditorView.lineWrapping]}
                    theme={oneDark}
                    editable={false}
                    basicSetup={{ lineNumbers: false, foldGutter: true }}
                    className="approval-query-editor"
                  />
                </section>
              );
            })()}

            {query.compiledCypher && (
              <section className="detail-section">
                <h4>Semantic Cypher</h4>
                <CodeMirror
                  value={query.compiledCypher}
                  extensions={[cypherLanguage(), EditorView.lineWrapping]}
                  theme={oneDark}
                  editable={false}
                  basicSetup={{ lineNumbers: false, foldGutter: true }}
                  className="approval-query-editor"
                />
              </section>
            )}
          </div>

          <section className="detail-section detail-meta">
            {query.stableId && <DetailRow label="Stable ID" value={query.stableId} />}
            {query.approvedBy && <DetailRow label="Approved by" value={query.approvedBy} />}
            {query.ownerTeam && <DetailRow label="Owner team" value={query.ownerTeam} />}
            {query.businessPurpose && <DetailRow label="Business purpose" value={query.businessPurpose} />}
            {query.useCases && <DetailRow label="Use cases" value={query.useCases} />}
            {query.dataSensitivity && <DetailRow label="Data sensitivity" value={query.dataSensitivity} />}
            {query.refreshFrequency && <DetailRow label="Refresh frequency" value={query.refreshFrequency} />}
            {query.expectedRowCount && <DetailRow label="Expected row count" value={query.expectedRowCount} />}
            {query.expiryDate && <DetailRow label="Expiry date" value={query.expiryDate} />}
            {query.visibleTo?.length > 0 && (
              <DetailRow label="Visible to" value={query.visibleTo.join(", ")} />
            )}
          </section>

          {query.scheduleCron && (
            <section className="detail-section">
              <h4>Scheduled Delivery</h4>
              <div className="detail-meta">
                <DetailRow label="Cron" value={query.scheduleCron} />
                {query.scheduleOutputType && <DetailRow label="Output type" value={query.scheduleOutputType} />}
                {query.scheduleOutputFormat && <DetailRow label="Format" value={query.scheduleOutputFormat} />}
                {query.scheduleDestination && <DetailRow label="Destination" value={query.scheduleDestination} />}
              </div>
            </section>
          )}

          {(query.sinkTopic || query.sinkTrigger) && (
            <section className="detail-section">
              <h4>Sink</h4>
              <div className="detail-meta">
                {query.sinkTopic && <DetailRow label="Topic" value={query.sinkTopic} />}
                {query.sinkTrigger && <DetailRow label="Trigger" value={query.sinkTrigger} />}
                {query.sinkKeyColumn && <DetailRow label="Key column" value={query.sinkKeyColumn} />}
              </div>
            </section>
          )}

          <ExpandedActions query={query} roleIds={roleIds} onDone={onDone} />
        </div>
      )}
    </div>
  );
}

function DetailRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="detail-row">
      <span className="detail-label">{label}</span>
      <span className="detail-value">{value}</span>
    </div>
  );
}

/** Approval queue — searchable, filterable list of all governed queries (REQ-063). */
export function ApprovalsPage() {
  const [queries, setQueries] = useState<GovernedQuery[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [activeStatuses, setActiveStatuses] = useState<Set<StatusFilter>>(
    new Set(STATUS_FILTERS)
  );
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [roleIds, setRoleIds] = useState<string[]>([]);

  const load = useCallback(() => {
    setLoading(true);
    fetchAll().then(setQueries).finally(() => setLoading(false));
  }, []);

  useEffect(load, [load]);
  useEffect(() => { fetchRoleIds().then(setRoleIds); }, []);

  const toggleStatus = (s: StatusFilter) => {
    setActiveStatuses((prev) => {
      const next = new Set(prev);
      next.has(s) ? next.delete(s) : next.add(s);
      return next;
    });
  };

  const filtered = queries.filter((q) => {
    if (!activeStatuses.has(q.status as StatusFilter)) return false;
    if (!search.trim()) return true;
    const term = search.toLowerCase();
    return (
      queryDisplayName(q).toLowerCase().includes(term) ||
      q.status.toLowerCase().includes(term) ||
      (q.developerId ?? "").toLowerCase().includes(term) ||
      (q.ownerTeam ?? "").toLowerCase().includes(term) ||
      (q.businessPurpose ?? "").toLowerCase().includes(term) ||
      q.queryText.toLowerCase().includes(term)
    );
  });

  const toggle = (id: number) =>
    setExpandedId((prev) => (prev === id ? null : id));

  if (loading) return <div className="page">Loading governed queries...</div>;

  return (
    <div className="page">
      <h2>Approval Queue</h2>
      <div className="approvals-toolbar">
        <FilterInput value={search} onChange={setSearch} placeholder="Search by name, status, submitter..." />
        <div className="approvals-status-filters">
          {STATUS_FILTERS.map((s) => (
            <button
              key={s}
              className={`status-toggle status-toggle--${s}${activeStatuses.has(s) ? " status-toggle--active" : ""}`}
              onClick={() => toggleStatus(s)}
            >
              <input
                type="checkbox"
                checked={activeStatuses.has(s)}
                onChange={() => toggleStatus(s)}
                onClick={(e) => e.stopPropagation()}
                tabIndex={-1}
              />
              {s}
            </button>
          ))}
        </div>
        <span className="approvals-count">
          {filtered.length} of {queries.length}
        </span>
      </div>
      {filtered.length === 0 ? (
        <p className="approvals-empty">
          {queries.length === 0
            ? "No governed queries."
            : "No results match your search."}
        </p>
      ) : (
        <div className="approval-list">
          {filtered.map((q) => (
            <QueryRow
              key={q.id}
              query={q}
              expanded={expandedId === q.id}
              roleIds={roleIds}
              onToggle={() => toggle(q.id)}
              onDone={() => { load(); setExpandedId(null); }}
            />
          ))}
        </div>
      )}
    </div>
  );
}
