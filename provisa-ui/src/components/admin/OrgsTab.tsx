// Copyright (c) 2026 Kenneth Stott
// Canary: f09d73b4-de5f-4d5a-a125-c26597327e3c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect } from "react";
import { Trash2 } from "lucide-react";
import {
  fetchOrgs,
  createOrg,
  deleteOrg,
  fetchOrgMembers,
  addOrgMember,
  removeOrgMember,
  fetchInvites,
  createInvite,
  revokeInvite,
} from "../../api/admin";
import type { Org, OrgMember, OrgInvite } from "../../api/admin";
import { FilterInput } from "./FilterInput";

const PAGE_SIZE = 50;

export function OrgsTab() {
  const [orgs, setOrgs] = useState<Org[]>([]);
  const [newOrgId, setNewOrgId] = useState("");
  const [newOrgName, setNewOrgName] = useState("");
  const [orgMsg, setOrgMsg] = useState("");
  const [showCreateOrg, setShowCreateOrg] = useState(false);
  const [expandedOrgId, setExpandedOrgId] = useState<string | null>(null);
  const [orgMembers, setOrgMembers] = useState<Record<string, OrgMember[]>>({});
  const [addMemberUserId, setAddMemberUserId] = useState("");
  const [orgSearch, setOrgSearch] = useState("");
  const [orgPage, setOrgPage] = useState(0);
  const [orgInvites, setOrgInvites] = useState<OrgInvite[]>([]);
  const [inviteOrgId, setInviteOrgId] = useState("");
  const [showInviteForm, setShowInviteForm] = useState(false);
  const [inviteSearch, setInviteSearch] = useState("");
  const [inviteMsg, setInviteMsg] = useState("");
  const [copiedToken, setCopiedToken] = useState<string | null>(null);
  const [invitePage, setInvitePage] = useState(0);

  useEffect(() => {
    fetchOrgs()
      .then(setOrgs)
      .catch(() => setOrgs([]));
    fetchInvites()
      .then(setOrgInvites)
      .catch(() => setOrgInvites([]));
  }, []);

  const handleCreateOrg = async () => {
    if (!newOrgId.trim() || !newOrgName.trim()) return;
    await createOrg(newOrgId.trim(), newOrgName.trim());
    setOrgs(await fetchOrgs());
    setNewOrgId("");
    setNewOrgName("");
    setShowCreateOrg(false);
    setOrgMsg(`Created "${newOrgName.trim()}"`);
  };

  const handleDeleteOrg = async (id: string) => {
    await deleteOrg(id);
    setOrgs(await fetchOrgs());
    setOrgMsg(`Deleted "${id}"`);
  };

  const handleExpandOrg = async (id: string) => {
    if (expandedOrgId === id) {
      setExpandedOrgId(null);
      return;
    }
    setExpandedOrgId(id);
    if (!orgMembers[id]) {
      const members = await fetchOrgMembers(id);
      setOrgMembers((prev) => ({ ...prev, [id]: members }));
    }
  };

  const handleAddOrgMember = async (oid: string) => {
    if (!addMemberUserId.trim()) return;
    await addOrgMember(oid, addMemberUserId.trim());
    const members = await fetchOrgMembers(oid);
    setOrgMembers((prev) => ({ ...prev, [oid]: members }));
    setAddMemberUserId("");
  };

  const handleRemoveOrgMember = async (oid: string, userId: string) => {
    await removeOrgMember(oid, userId);
    setOrgMembers((prev) => ({
      ...prev,
      [oid]: (prev[oid] ?? []).filter((m) => m.user_id !== userId),
    }));
  };

  const handleCreateInvite = async () => {
    if (!inviteOrgId.trim()) return;
    const invite = await createInvite(inviteOrgId.trim());
    setOrgInvites(await fetchInvites());
    const url = `${window.location.origin}/register?invite=${invite.token}`;
    await navigator.clipboard.writeText(url);
    setInviteMsg(`Invite created and copied: ${url}`);
    setInviteOrgId("");
    setShowInviteForm(false);
  };

  const handleRevokeInvite = async (token: string) => {
    await revokeInvite(token);
    setOrgInvites((prev) => prev.filter((i) => i.token !== token));
  };

  const handleCopyInvite = async (token: string) => {
    const url = `${window.location.origin}/register?invite=${token}`;
    await navigator.clipboard.writeText(url);
    setCopiedToken(token);
    setTimeout(() => setCopiedToken(null), 2000);
  };

  const q = orgSearch.toLowerCase();
  const filteredOrgs = orgs.filter(
    (o) => o.id.toLowerCase().includes(q) || o.name.toLowerCase().includes(q),
  );
  const orgTotalPages = Math.max(1, Math.ceil(filteredOrgs.length / PAGE_SIZE));
  const orgSafePage = Math.min(orgPage, orgTotalPages - 1);
  const pagedOrgs = filteredOrgs.slice(orgSafePage * PAGE_SIZE, (orgSafePage + 1) * PAGE_SIZE);

  const iq = inviteSearch.toLowerCase();
  const filteredInvites = orgInvites.filter(
    (i) =>
      (i.org_name ?? "").toLowerCase().includes(iq) ||
      i.token.toLowerCase().includes(iq) ||
      (i.created_by ?? "").toLowerCase().includes(iq),
  );
  const invTotalPages = Math.max(1, Math.ceil(filteredInvites.length / PAGE_SIZE));
  const invSafePage = Math.min(invitePage, invTotalPages - 1);
  const pagedInvites = filteredInvites.slice(invSafePage * PAGE_SIZE, (invSafePage + 1) * PAGE_SIZE);

  return (
    <div>
      {orgMsg && <div className="success" style={{ marginBottom: "0.5rem" }}>{orgMsg}</div>}

      <div className="page-header">
        <h3 style={{ margin: 0 }}>Organizations</h3>
        <FilterInput
          value={orgSearch}
          onChange={(v) => { setOrgSearch(v); setOrgPage(0); }}
          placeholder="Filter by ID or name…"
        />
        <div className="page-actions">
          <button onClick={() => setShowCreateOrg((v) => !v)}>
            {showCreateOrg ? "✕" : "+ Org"}
          </button>
        </div>
      </div>

      {showCreateOrg && (
        <div className="form-card" style={{ marginBottom: "1rem" }}>
          <label>
            ID (slug)
            <input value={newOrgId} onChange={(e) => setNewOrgId(e.target.value)} placeholder="my-org" />
          </label>
          <label>
            Name
            <input value={newOrgName} onChange={(e) => setNewOrgName(e.target.value)} placeholder="My Org" />
          </label>
          <div style={{ display: "flex", gap: "0.5rem" }}>
            <button onClick={handleCreateOrg} disabled={!newOrgId.trim() || !newOrgName.trim()}>
              + Org
            </button>
          </div>
        </div>
      )}

      <table className="data-table">
        <thead>
          <tr>
            <th>ID</th>
            <th>Name</th>
            <th>Members</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {filteredOrgs.length === 0 && (
            <tr>
              <td colSpan={4} style={{ color: "var(--text-muted)", textAlign: "center" }}>
                No organizations
              </td>
            </tr>
          )}
          {pagedOrgs.map((org) => (
            <>
              <tr key={org.id}>
                <td>{org.id}</td>
                <td>{org.name}</td>
                <td>
                  <button
                    style={{ fontSize: "0.75rem", padding: "0.2rem 0.5rem" }}
                    onClick={() => handleExpandOrg(org.id)}
                  >
                    {expandedOrgId === org.id ? "Hide" : "Members"}
                  </button>
                </td>
                <td>
                  {org.id !== "root" && (
                    <button
                      className="btn-icon-danger"
                      title="Delete"
                      onClick={() => handleDeleteOrg(org.id)}
                    >
                      <Trash2 size={14} />
                    </button>
                  )}
                </td>
              </tr>
              {expandedOrgId === org.id && (
                <tr key={`${org.id}-members`}>
                  <td colSpan={4} style={{ paddingLeft: "2rem", background: "var(--bg-alt, var(--bg))" }}>
                    <div style={{ padding: "0.75rem 0" }}>
                      <strong style={{ fontSize: "0.85rem" }}>Members</strong>
                      <div style={{ marginTop: "0.5rem", display: "flex", flexWrap: "wrap", gap: "0.4rem" }}>
                        {(orgMembers[org.id] ?? []).length === 0 && (
                          <span style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>No members</span>
                        )}
                        {(orgMembers[org.id] ?? []).map((m) => (
                          <span
                            key={m.user_id}
                            style={{
                              display: "inline-flex",
                              alignItems: "center",
                              gap: "0.25rem",
                              background: "var(--border)",
                              borderRadius: "4px",
                              padding: "0.2rem 0.5rem",
                              fontSize: "0.8rem",
                            }}
                          >
                            {m.display_name ?? m.email ?? m.user_id}
                            <button
                              style={{ background: "none", border: "none", color: "var(--danger, #e55)", cursor: "pointer", padding: 0, lineHeight: 1 }}
                              onClick={() => handleRemoveOrgMember(org.id, m.user_id)}
                              title="Remove"
                            >
                              ×
                            </button>
                          </span>
                        ))}
                      </div>
                      <div style={{ marginTop: "0.5rem", display: "flex", gap: "0.5rem", alignItems: "center" }}>
                        <input
                          placeholder="user_id"
                          value={addMemberUserId}
                          onChange={(e) => setAddMemberUserId(e.target.value)}
                          style={{ background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.3rem", borderRadius: "4px", fontSize: "0.85rem" }}
                        />
                        <button
                          style={{ fontSize: "0.8rem", padding: "0.3rem 0.7rem" }}
                          onClick={() => handleAddOrgMember(org.id)}
                          disabled={!addMemberUserId.trim()}
                        >
                          Add
                        </button>
                      </div>
                    </div>
                  </td>
                </tr>
              )}
            </>
          ))}
        </tbody>
      </table>
      {orgTotalPages > 1 && (
        <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", justifyContent: "flex-end", padding: "0.5rem 0" }}>
          <button onClick={() => setOrgPage(0)} disabled={orgSafePage === 0}>«</button>
          <button onClick={() => setOrgPage((p) => p - 1)} disabled={orgSafePage === 0}>‹</button>
          <span>Page {orgSafePage + 1} / {orgTotalPages}</span>
          <button onClick={() => setOrgPage((p) => p + 1)} disabled={orgSafePage >= orgTotalPages - 1}>›</button>
          <button onClick={() => setOrgPage(orgTotalPages - 1)} disabled={orgSafePage >= orgTotalPages - 1}>»</button>
        </div>
      )}

      <div className="page-header" style={{ marginTop: "1.5rem" }}>
        <h3 style={{ margin: 0 }}>Invite Links</h3>
        <FilterInput
          value={inviteSearch}
          onChange={(v) => { setInviteSearch(v); setInvitePage(0); }}
          placeholder="Filter by org, token, or creator…"
        />
        <div className="page-actions">
          <button onClick={() => setShowInviteForm((v) => !v)}>
            {showInviteForm ? "✕" : "+ Invite Link"}
          </button>
        </div>
      </div>

      {showInviteForm && (
        <div className="form-card" style={{ marginBottom: "1rem" }}>
          <label>
            Organization
            <select value={inviteOrgId} onChange={(e) => setInviteOrgId(e.target.value)}>
              <option value="">Select org…</option>
              {orgs.map((o) => (
                <option key={o.id} value={o.id}>{o.name} ({o.id})</option>
              ))}
            </select>
          </label>
          {inviteMsg && <p style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>{inviteMsg}</p>}
          <div style={{ display: "flex", gap: "0.5rem" }}>
            <button onClick={handleCreateInvite} disabled={!inviteOrgId}>
              Generate Invite Link
            </button>
          </div>
        </div>
      )}

      <table className="data-table">
        <thead>
          <tr>
            <th>Org</th>
            <th>Token</th>
            <th>Created By</th>
            <th>Expires</th>
            <th>Status</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {filteredInvites.length === 0 && (
            <tr>
              <td colSpan={6} style={{ color: "var(--text-muted)", textAlign: "center" }}>No invites</td>
            </tr>
          )}
          {pagedInvites.map((inv) => (
            <tr key={inv.token}>
              <td>{inv.org_name}</td>
              <td><code>{inv.token.slice(0, 8)}…</code></td>
              <td>{inv.created_by}</td>
              <td>{new Date(inv.expires_at).toLocaleDateString()}</td>
              <td>{inv.used_at ? `Used ${new Date(inv.used_at).toLocaleDateString()}` : "Active"}</td>
              <td>
                <div style={{ display: "flex", gap: "0.25rem" }}>
                  {!inv.used_at && (
                    <button style={{ fontSize: "0.75rem", padding: "0.2rem 0.5rem" }} onClick={() => handleCopyInvite(inv.token)}>
                      {copiedToken === inv.token ? "Copied!" : "Copy"}
                    </button>
                  )}
                  {!inv.used_at && (
                    <button className="destructive" style={{ fontSize: "0.75rem", padding: "0.2rem 0.5rem" }} onClick={() => handleRevokeInvite(inv.token)}>
                      Revoke
                    </button>
                  )}
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {invTotalPages > 1 && (
        <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", justifyContent: "flex-end", padding: "0.5rem 0" }}>
          <button onClick={() => setInvitePage(0)} disabled={invSafePage === 0}>«</button>
          <button onClick={() => setInvitePage((p) => p - 1)} disabled={invSafePage === 0}>‹</button>
          <span>Page {invSafePage + 1} / {invTotalPages}</span>
          <button onClick={() => setInvitePage((p) => p + 1)} disabled={invSafePage >= invTotalPages - 1}>›</button>
          <button onClick={() => setInvitePage(invTotalPages - 1)} disabled={invSafePage >= invTotalPages - 1}>»</button>
        </div>
      )}
    </div>
  );
}
