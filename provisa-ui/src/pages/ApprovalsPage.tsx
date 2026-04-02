import { useState, useEffect } from "react";
import { ConfirmDialog } from "../components/ConfirmDialog";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8001";

interface PendingQuery {
  id: number;
  name: string;
  query_text: string;
  submitted_by: string;
  status: string;
}

async function fetchPending(): Promise<PendingQuery[]> {
  const resp = await fetch(`${API_BASE}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: `{ persistedQueries(status: "submitted") { id name queryText submittedBy status } }`,
    }),
  });
  const json = await resp.json();
  return json.data?.persistedQueries ?? [];
}

async function approveQuery(id: number): Promise<void> {
  await fetch(`${API_BASE}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: `mutation { approveQuery(id: ${id}) { success } }`,
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

  const load = () => {
    setLoading(true);
    fetchPending().then(setQueries).finally(() => setLoading(false));
  };

  useEffect(load, []);

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
          {queries.map((q) => (
            <div key={q.id} className="approval-card">
              <div className="approval-header">
                <h3>{q.name}</h3>
                <span className="submitted-by">by {q.submitted_by}</span>
              </div>
              <pre className="approval-query">{q.query_text}</pre>
              <div className="approval-actions">
                <ConfirmDialog
                  title={`Approve query "${q.name}"?`}
                  consequence="This query will become available for production use."
                  onConfirm={async () => { await approveQuery(q.id); load(); }}
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
          ))}
        </div>
      )}
    </div>
  );
}
