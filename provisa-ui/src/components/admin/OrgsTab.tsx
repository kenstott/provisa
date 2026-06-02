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

const PAGE_SIZE = 50;

export function OrgsTab() {
  const [orgs, setOrgs] = useState<Org[]>([]);
  const [newOrgId, setNewOrgId] = useState("");
  const [newOrgName, setNewOrgName] = useState("");
  const [orgMsg, setOrgMsg] = useState("");
  const [expandedOrgId, setExpandedOrgId] = useState<string | null>(null);
  const [orgMembers, setOrgMembers] = useState<Record<string, OrgMember[]>>({});
  const [addMemberUserId, setAddMemberUserId] = useState("");
  const [orgPage, setOrgPage] = useState(0);
  const [orgInvites, setOrgInvites] = useState<OrgInvite[]>([]);
  const [inviteOrgId, setInviteOrgId] = useState("");
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

  return (
    <div>
      <h3>Organizations</h3>
      {orgMsg && <p className="form-msg">{orgMsg}</p>}
      {(() => {
        const totalPages = Math.max(1, Math.ceil(orgs.length / PAGE_SIZE));
        const paged = orgs.slice(orgPage * PAGE_SIZE, (orgPage + 1) * PAGE_SIZE);
        return (
          <div>
            <table className="admin-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Name</th>
                  <th>Members</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {paged.map((org) => (
                  <>
                    <tr key={org.id}>
                      <td>{org.id}</td>
                      <td>{org.name}</td>
                      <td>
                        <button className="btn-secondary" onClick={() => handleExpandOrg(org.id)}>
                          {expandedOrgId === org.id ? "Hide" : "Members"}
                        </button>
                      </td>
                      <td>
                        {org.id !== "root" && (
                          <button className="btn-danger" onClick={() => handleDeleteOrg(org.id)}>
                            <Trash2 size={14} />
                          </button>
                        )}
                      </td>
                    </tr>
                    {expandedOrgId === org.id && (
                      <tr key={`${org.id}-members`}>
                        <td colSpan={4}>
                          <div className="assignment-panel">
                            {(orgMembers[org.id] ?? []).map((m) => (
                              <span key={m.user_id} className="assignment-chip">
                                {m.display_name ?? m.email ?? m.user_id}
                                <button onClick={() => handleRemoveOrgMember(org.id, m.user_id)}>
                                  ×
                                </button>
                              </span>
                            ))}
                            <div className="assignment-add">
                              <input
                                placeholder="user_id"
                                value={addMemberUserId}
                                onChange={(e) => setAddMemberUserId(e.target.value)}
                              />
                              <button
                                className="btn-primary"
                                onClick={() => handleAddOrgMember(org.id)}
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
            {totalPages > 1 && (
              <div
                style={{
                  display: "flex",
                  gap: "0.5rem",
                  alignItems: "center",
                  justifyContent: "flex-end",
                  padding: "0.5rem 0",
                  marginBottom: "1rem",
                }}
              >
                <button onClick={() => setOrgPage(0)} disabled={orgPage === 0}>
                  «
                </button>
                <button onClick={() => setOrgPage((p) => p - 1)} disabled={orgPage === 0}>
                  ‹
                </button>
                <span>
                  Page {orgPage + 1} / {totalPages}
                </span>
                <button onClick={() => setOrgPage((p) => p + 1)} disabled={orgPage >= totalPages - 1}>
                  ›
                </button>
                <button
                  onClick={() => setOrgPage(totalPages - 1)}
                  disabled={orgPage >= totalPages - 1}
                >
                  »
                </button>
              </div>
            )}
          </div>
        );
      })()}
      <h4>Create Org</h4>
      <div className="form-row">
        <input placeholder="ID (slug)" value={newOrgId} onChange={(e) => setNewOrgId(e.target.value)} />
        <input placeholder="Name" value={newOrgName} onChange={(e) => setNewOrgName(e.target.value)} />
        <button className="btn-primary" onClick={handleCreateOrg}>
          Create
        </button>
      </div>
      <h3 style={{ marginTop: 24 }}>Invite Links</h3>
      <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
        <select value={inviteOrgId} onChange={(e) => setInviteOrgId(e.target.value)}>
          <option value="">Select org...</option>
          {orgs.map((o) => (
            <option key={o.id} value={o.id}>
              {o.name} ({o.id})
            </option>
          ))}
        </select>
        <button className="btn-primary" onClick={handleCreateInvite} disabled={!inviteOrgId}>
          Generate Invite Link
        </button>
      </div>
      {inviteMsg && (
        <div style={{ marginBottom: 8, color: "var(--muted-foreground)", fontSize: 13 }}>
          {inviteMsg}
        </div>
      )}
      {(() => {
        const totalPages = Math.max(1, Math.ceil(orgInvites.length / PAGE_SIZE));
        const paged = orgInvites.slice(invitePage * PAGE_SIZE, (invitePage + 1) * PAGE_SIZE);
        return (
          <div>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr>
                  {["Org", "Token", "Created By", "Expires", "Status", ""].map((h) => (
                    <th
                      key={h}
                      style={{
                        textAlign: "left",
                        padding: "4px 8px",
                        borderBottom: "1px solid var(--border)",
                      }}
                    >
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {paged.map((inv) => (
                  <tr key={inv.token}>
                    <td style={{ padding: "4px 8px" }}>{inv.org_name}</td>
                    <td style={{ padding: "4px 8px", fontFamily: "monospace", fontSize: 11 }}>
                      {inv.token.slice(0, 8)}…
                    </td>
                    <td style={{ padding: "4px 8px" }}>{inv.created_by}</td>
                    <td style={{ padding: "4px 8px" }}>
                      {new Date(inv.expires_at).toLocaleDateString()}
                    </td>
                    <td style={{ padding: "4px 8px" }}>
                      {inv.used_at ? `Used ${new Date(inv.used_at).toLocaleDateString()}` : "Active"}
                    </td>
                    <td style={{ padding: "4px 8px", display: "flex", gap: 4 }}>
                      {!inv.used_at && (
                        <button onClick={() => handleCopyInvite(inv.token)} style={{ fontSize: 12 }}>
                          {copiedToken === inv.token ? "Copied!" : "Copy"}
                        </button>
                      )}
                      {!inv.used_at && (
                        <button
                          onClick={() => handleRevokeInvite(inv.token)}
                          style={{ fontSize: 12, color: "var(--destructive)" }}
                        >
                          Revoke
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
                {orgInvites.length === 0 && (
                  <tr>
                    <td colSpan={6} style={{ padding: "8px", color: "var(--muted-foreground)" }}>
                      No invites
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
            {totalPages > 1 && (
              <div
                style={{
                  display: "flex",
                  gap: "0.5rem",
                  alignItems: "center",
                  justifyContent: "flex-end",
                  padding: "0.5rem 0",
                  marginBottom: "1rem",
                }}
              >
                <button onClick={() => setInvitePage(0)} disabled={invitePage === 0}>
                  «
                </button>
                <button onClick={() => setInvitePage((p) => p - 1)} disabled={invitePage === 0}>
                  ‹
                </button>
                <span>
                  Page {invitePage + 1} / {totalPages}
                </span>
                <button
                  onClick={() => setInvitePage((p) => p + 1)}
                  disabled={invitePage >= totalPages - 1}
                >
                  ›
                </button>
                <button
                  onClick={() => setInvitePage(totalPages - 1)}
                  disabled={invitePage >= totalPages - 1}
                >
                  »
                </button>
              </div>
            )}
          </div>
        );
      })()}
    </div>
  );
}
