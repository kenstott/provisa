// Copyright (c) 2026 Kenneth Stott
// Canary: c543c04b-a6b6-4082-beef-b38df177f30a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { FilterInput } from "../components/admin/FilterInput";

const API_BASE = import.meta.env.VITE_API_BASE || "";

const PAGE_SIZE = 50;

const TAB_LABELS: Record<string, string> = {
  pending: "Pending",
  resolved: "Resolved",
};

interface CreationRequest {
  id: number;
  request_type: string;
  capability: string;
  payload: Record<string, unknown>;
  requested_by: string | null;
  status: string;
  rejection_reason: string | null;
  resolved_by: string | null;
  created_at: string;
  resolved_at: string | null;
  approvals: { approver: string; approved_at: string }[];
  required_approvals: number;
}

async function apiFetch(path: string, opts?: RequestInit) {
  const resp = await fetch(`${API_BASE}${path}`, opts);
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(body.detail || resp.statusText);
  }
  return resp.json();
}

async function fetchRequests(status?: string): Promise<CreationRequest[]> {
  const qs = status ? `?status=${encodeURIComponent(status)}` : "";
  return apiFetch(`/admin/creation-requests${qs}`);
}

async function fetchRejectionReasons(): Promise<Record<string, string[]>> {
  return apiFetch("/admin/creation-requests/rejection-reasons");
}

async function apiApprove(id: number): Promise<CreationRequest> {
  return apiFetch(`/admin/creation-requests/${id}/approve`, { method: "POST" });
}

async function apiExecute(id: number): Promise<{ status: string }> {
  return apiFetch(`/admin/creation-requests/${id}/execute`, { method: "POST" });
}

async function apiReject(id: number, reason: string): Promise<{ status: string }> {
  return apiFetch(`/admin/creation-requests/${id}/reject`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ reason }),
  });
}

const REASON_LABELS: Record<string, string> = {
  duplicate: "Duplicate",
  incorrect_join_columns: "Incorrect join columns",
  wrong_cardinality: "Wrong cardinality",
  source_not_registered: "Source not registered",
  insufficient_detail: "Insufficient detail",
  query_invalid: "Query invalid",
  governance_violation: "Governance violation",
  out_of_scope: "Out of scope",
  endpoint_unreachable: "Endpoint unreachable",
  schema_mismatch: "Schema mismatch",
};

export function RequestsPage() {
  const [tab, setTab] = useState<"pending" | "resolved">("pending");
  const [rows, setRows] = useState<CreationRequest[]>([]);
  const [reasons, setReasons] = useState<Record<string, string[]>>({});
  const [error, setError] = useState<string | null>(null);
  const [rejectingId, setRejectingId] = useState<number | null>(null);
  const [rejectReason, setRejectReason] = useState("");
  const [busy, setBusy] = useState(false);
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(0);

  const load = (status: "pending" | "resolved") => {
    fetchRequests(status === "pending" ? "pending" : undefined)
      .then((data) =>
        status === "resolved" ? setRows(data.filter((r) => r.status !== "pending")) : setRows(data),
      )
      .catch((e) => setError(String(e)));
  };

  useEffect(() => {
    load(tab);
    fetchRejectionReasons().then(setReasons).catch(() => {});
  }, [tab]);

  const doApprove = async (id: number) => {
    setBusy(true);
    setError(null);
    try {
      await apiApprove(id);
      load(tab);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };

  const doExecute = async (id: number) => {
    setBusy(true);
    setError(null);
    try {
      await apiExecute(id);
      load(tab);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };

  const doReject = async () => {
    if (rejectingId === null || !rejectReason) return;
    setBusy(true);
    setError(null);
    try {
      await apiReject(rejectingId, rejectReason);
      setRejectingId(null);
      setRejectReason("");
      load(tab);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };

  const displayRows =
    tab === "pending"
      ? rows.filter((r) => r.status === "pending")
      : rows.filter((r) => r.status !== "pending");

  const q = search.toLowerCase();
  const filtered = displayRows.filter(
    (r) =>
      String(r.id).includes(q) ||
      r.request_type.toLowerCase().includes(q) ||
      (r.requested_by ?? "").toLowerCase().includes(q) ||
      r.status.toLowerCase().includes(q),
  );
  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safePage = Math.min(page, totalPages - 1);
  const paged = filtered.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE);

  return (
    <div className="page">
      <div className="page-header">
        <h2 style={{ margin: 0 }}>Requests</h2>
        <FilterInput
          value={search}
          onChange={(v) => { setSearch(v); setPage(0); }}
          placeholder="Filter by ID, type, requester, status…"
        />
        <div className="page-actions">
          <div style={{ display: "flex", gap: "0.25rem" }}>
            {(["pending", "resolved"] as const).map((t) => (
              <button
                key={t}
                onClick={() => { setTab(t); setPage(0); }}
                style={{
                  fontWeight: tab === t ? 600 : 400,
                  background: tab === t ? "var(--accent)" : "transparent",
                  color: tab === t ? "#fff" : "var(--text)",
                  border: "1px solid var(--border)",
                }}
              >
                {TAB_LABELS[t]}
              </button>
            ))}
          </div>
        </div>
      </div>

      {error && (
        <div style={{ color: "var(--error)", marginBottom: "1rem" }}>{error}</div>
      )}

      {rejectingId !== null && (
        <div className="form-card" style={{ marginBottom: "1rem" }}>
          <strong>Reject request #{rejectingId}</strong>
          <label>
            Reason
            <select value={rejectReason} onChange={(e) => setRejectReason(e.target.value)}>
              <option value="">— select reason —</option>
              {(reasons[rows.find((r) => r.id === rejectingId)?.request_type ?? ""] ?? []).map((r) => (
                <option key={r} value={r}>{REASON_LABELS[r] ?? r}</option>
              ))}
            </select>
          </label>
          <div style={{ display: "flex", gap: "0.5rem" }}>
            <button onClick={doReject} disabled={!rejectReason || busy}>Confirm</button>
            <button onClick={() => { setRejectingId(null); setRejectReason(""); }}>✕</button>
          </div>
        </div>
      )}

      {filtered.length === 0 ? (
        <p style={{ color: "var(--text-muted)" }}>
          No {tab} requests{search ? " matching filter" : ""}.
        </p>
      ) : (
        <>
          <table className="data-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Type</th>
                <th>Requester</th>
                <th>Submitted</th>
                <th>Payload</th>
                <th>Approvals</th>
                <th>Status</th>
                <th>Reason</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {paged.map((row) => (
                <tr key={row.id}>
                  <td>{row.id}</td>
                  <td>{row.request_type}</td>
                  <td>{row.requested_by ?? "—"}</td>
                  <td style={{ whiteSpace: "nowrap" }}>
                    {new Date(row.created_at).toLocaleString()}
                  </td>
                  <td style={{ maxWidth: "20rem" }}>
                    <details>
                      <summary style={{ cursor: "pointer" }}>view</summary>
                      <pre style={{ fontSize: "0.75rem", whiteSpace: "pre-wrap" }}>
                        {JSON.stringify(row.payload, null, 2)}
                      </pre>
                    </details>
                  </td>
                  <td>{row.approvals.length} / {row.required_approvals}</td>
                  <td>
                    <span className={`status-badge status-${row.status === "pending" ? "pending" : row.status === "approved" ? "active" : "disabled"}`}>
                      {row.status}
                    </span>
                  </td>
                  <td>{row.rejection_reason ?? "—"}</td>
                  <td style={{ whiteSpace: "nowrap" }}>
                    {row.status === "pending" && (
                      <div style={{ display: "flex", gap: "0.4rem" }}>
                        <button
                          onClick={() => doApprove(row.id)}
                          disabled={busy}
                          style={{ fontSize: "0.75rem", padding: "0.2rem 0.5rem" }}
                        >
                          Approve ({row.approvals.length + 1}/{row.required_approvals})
                        </button>
                        {row.required_approvals === 1 && (
                          <button
                            onClick={() => doExecute(row.id)}
                            disabled={busy}
                            style={{ fontSize: "0.75rem", padding: "0.2rem 0.5rem" }}
                          >
                            Execute
                          </button>
                        )}
                        <button
                          onClick={() => { setRejectingId(row.id); setRejectReason(""); }}
                          disabled={busy}
                          style={{ fontSize: "0.75rem", padding: "0.2rem 0.5rem" }}
                        >
                          Reject
                        </button>
                      </div>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {totalPages > 1 && (
            <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", justifyContent: "flex-end", padding: "0.5rem 0" }}>
              <button onClick={() => setPage(0)} disabled={safePage === 0}>«</button>
              <button onClick={() => setPage((p) => p - 1)} disabled={safePage === 0}>‹</button>
              <span>Page {safePage + 1} / {totalPages}</span>
              <button onClick={() => setPage((p) => p + 1)} disabled={safePage >= totalPages - 1}>›</button>
              <button onClick={() => setPage(totalPages - 1)} disabled={safePage >= totalPages - 1}>»</button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
