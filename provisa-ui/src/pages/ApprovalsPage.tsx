// Copyright (c) 2026 Kenneth Stott
// Canary: 7ccb073a-ee3e-4940-8bd7-4812ca8c39af
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect } from "react";
import { ConfirmDialog } from "../components/ConfirmDialog";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8001";

interface PendingQuery {
  id: number;
  queryText: string;
  developerId: string | null;
  status: string;
}

async function fetchPending(): Promise<PendingQuery[]> {
  const resp = await fetch(`${API_BASE}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: `{ persistedQueries { id queryText developerId status } }`,
    }),
  });
  const json = await resp.json();
  const all: PendingQuery[] = json.data?.persistedQueries ?? [];
  return all.filter((q) => q.status === "pending");
}

async function fetchRoleIds(): Promise<string[]> {
  const resp = await fetch(`${API_BASE}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: `{ roles { id } }` }),
  });
  const json = await resp.json();
  return (json.data?.roles ?? []).map((r: { id: string }) => r.id);
}

async function approveQuery(id: number, visibleTo: string[]): Promise<void> {
  const visibleToArg = JSON.stringify(visibleTo);
  await fetch(`${API_BASE}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: `mutation { approveQuery(queryId: ${id}, visibleTo: ${visibleToArg}) { success } }`,
    }),
  });
}

async function rejectQuery(id: number, reason: string): Promise<void> {
  await fetch(`${API_BASE}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: `mutation { rejectQuery(id: ${id}, reason: "${reason.replace(/"/g, '\\"')}") { success } }`,
    }),
  });
}

/** Approval queue — steward-optimized (REQ-063). */
export function ApprovalsPage() {
  const [queries, setQueries] = useState<PendingQuery[]>([]);
  const [loading, setLoading] = useState(true);
  const [rejectId, setRejectId] = useState<number | null>(null);
  const [reason, setReason] = useState("");
  const [roleIds, setRoleIds] = useState<string[]>([]);
  const [visibleTo, setVisibleTo] = useState<Record<number, string[]>>({});

  const load = () => {
    setLoading(true);
    fetchPending().then(setQueries).finally(() => setLoading(false));
  };

  useEffect(load, []);
  useEffect(() => { fetchRoleIds().then(setRoleIds); }, []);

  const toggleRole = (queryId: number, roleId: string) => {
    setVisibleTo((prev) => {
      const current = prev[queryId] ?? [];
      const updated = current.includes(roleId)
        ? current.filter((r) => r !== roleId)
        : [...current, roleId];
      return { ...prev, [queryId]: updated };
    });
  };

  const toggleAll = (queryId: number) => {
    setVisibleTo((prev) => {
      const current = prev[queryId] ?? [];
      const updated = current.includes("*") ? current.filter((r) => r !== "*") : [...current, "*"];
      return { ...prev, [queryId]: updated };
    });
  };

  const handleReject = async () => {
    if (rejectId === null || !reason.trim()) return;
    await rejectQuery(rejectId, reason.trim());
    setRejectId(null);
    setReason("");
    load();
  };

  if (loading) return <div className="page">Loading approval queue...</div>;

  return (
    <div className="page">
      <h2>Approval Queue</h2>
      {queries.length === 0 ? (
        <p>No queries pending approval.</p>
      ) : (
        <div className="approval-list">
          {queries.map((q) => {
            const nameMatch = q.queryText.match(/(?:query|mutation)\s+(\w+)/);
            const displayName = nameMatch ? nameMatch[1] : `Query #${q.id}`;
            return (
            <div key={q.id} className="approval-card">
              <div className="approval-header">
                <h3>{displayName}</h3>
                <span className="submitted-by">by {q.developerId || "unknown"}</span>
              </div>
              <pre className="approval-query">{q.queryText}</pre>
              <div className="visible-to-select">
                <label>Subscribe access (visible to):</label>
                <div className="role-checkboxes">
                  <label key="*">
                    <input
                      type="checkbox"
                      checked={(visibleTo[q.id] ?? []).includes("*")}
                      onChange={() => toggleAll(q.id)}
                    />
                    All roles (*)
                  </label>
                  {roleIds.map((rid) => (
                    <label key={rid}>
                      <input
                        type="checkbox"
                        checked={(visibleTo[q.id] ?? []).includes(rid)}
                        onChange={() => toggleRole(q.id, rid)}
                      />
                      {rid}
                    </label>
                  ))}
                </div>
              </div>
              <div className="approval-actions">
                <ConfirmDialog
                  title={`Approve query "${q.queryText.slice(0, 40)}..."?`}
                  consequence="This query will become available for production use."
                  onConfirm={async () => { await approveQuery(q.id, visibleTo[q.id] ?? []); load(); }}
                >
                  {(open) => <button className="approve" onClick={open}>Approve</button>}
                </ConfirmDialog>
                <button className="reject" onClick={() => setRejectId(q.id)}>
                  Reject
                </button>
              </div>
              {rejectId === q.id && (
                <div className="reject-form">
                  <label>
                    Rejection reason (required — must be specific and actionable):
                    <textarea
                      value={reason}
                      onChange={(e) => setReason(e.target.value)}
                      placeholder="Explain what needs to change for this query to be approved..."
                      rows={3}
                    />
                  </label>
                  <div className="reject-actions">
                    <button onClick={() => { setRejectId(null); setReason(""); }}>Cancel</button>
                    <button
                      className="destructive"
                      onClick={handleReject}
                      disabled={!reason.trim()}
                    >
                      Submit Rejection
                    </button>
                  </div>
                </div>
              )}
            </div>
          );
          })}
        </div>
      )}
    </div>
  );
}
