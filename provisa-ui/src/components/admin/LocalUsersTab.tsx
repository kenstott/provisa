// Copyright (c) 2026 Kenneth Stott
// Canary: fa264cd2-9acb-4b3b-b896-f4c690da4a02
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
  fetchLocalUsers,
  createLocalUser,
  deleteLocalUser,
  fetchUserAssignments,
  addUserAssignment,
  removeUserAssignment,
} from "../../api/admin";
import type { LocalUser, UserAssignment } from "../../api/admin";

const PAGE_SIZE = 50;

interface LocalUsersTabProps {
  allRoles: string[];
  allDomains: string[];
}

export function LocalUsersTab({ allRoles, allDomains }: LocalUsersTabProps) {
  const [localUsers, setLocalUsers] = useState<LocalUser[]>([]);
  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [newEmail, setNewEmail] = useState("");
  const [newDisplayName, setNewDisplayName] = useState("");
  const [userMsg, setUserMsg] = useState("");
  const [expandedUserId, setExpandedUserId] = useState<string | null>(null);
  const [userAssignments, setUserAssignments] = useState<Record<string, UserAssignment[]>>({});
  const [assignRole, setAssignRole] = useState("");
  const [assignDomain, setAssignDomain] = useState("");
  const [userPage, setUserPage] = useState(0);

  useEffect(() => {
    fetchLocalUsers()
      .then(setLocalUsers)
      .catch(() => setLocalUsers([]));
  }, []);

  const handleAddUser = async () => {
    if (!newUsername.trim() || !newPassword.trim()) return;
    setUserMsg("");
    try {
      await createLocalUser({
        username: newUsername.trim(),
        password: newPassword,
        email: newEmail.trim() || undefined,
        display_name: newDisplayName.trim() || undefined,
      });
      const updated = await fetchLocalUsers();
      setLocalUsers(updated);
      setNewUsername("");
      setNewPassword("");
      setNewEmail("");
      setNewDisplayName("");
      setUserMsg(`Created "${newUsername.trim()}"`);
    } catch (e: unknown) {
      setUserMsg(e instanceof Error ? e.message : "Create failed");
    }
  };

  const handleDeleteUser = async (userId: string, username: string) => {
    await deleteLocalUser(userId);
    setLocalUsers((prev) => prev.filter((u) => u.id !== userId));
    if (expandedUserId === userId) setExpandedUserId(null);
    setUserMsg(`Deleted "${username}"`);
  };

  const handleExpandUser = async (userId: string) => {
    if (expandedUserId === userId) {
      setExpandedUserId(null);
      return;
    }
    setExpandedUserId(userId);
    setAssignRole(allRoles[0] ?? "");
    setAssignDomain("*");
    if (!userAssignments[userId]) {
      const rows = await fetchUserAssignments(userId);
      setUserAssignments((prev) => ({ ...prev, [userId]: rows }));
    }
  };

  const handleAddAssignment = async (userId: string) => {
    if (!assignRole) return;
    try {
      await addUserAssignment(userId, assignRole, assignDomain || "*");
      const rows = await fetchUserAssignments(userId);
      setUserAssignments((prev) => ({ ...prev, [userId]: rows }));
    } catch (e: unknown) {
      setUserMsg(e instanceof Error ? e.message : "Assignment failed");
    }
  };

  const handleRemoveAssignment = async (userId: string, assignmentId: number) => {
    await removeUserAssignment(userId, assignmentId);
    setUserAssignments((prev) => ({
      ...prev,
      [userId]: (prev[userId] ?? []).filter((a) => a.id !== assignmentId),
    }));
  };

  return (
    <>
      {userMsg && (
        <div className="success" style={{ marginBottom: "0.5rem" }}>
          {userMsg}
        </div>
      )}
      {(() => {
        const totalPages = Math.max(1, Math.ceil(localUsers.length / PAGE_SIZE));
        const paged = localUsers.slice(userPage * PAGE_SIZE, (userPage + 1) * PAGE_SIZE);
        return (
          <div>
            <table className="data-table" style={{ marginBottom: "1rem" }}>
              <thead>
                <tr>
                  <th>Username</th>
                  <th>Email</th>
                  <th>Display Name</th>
                  <th>Active</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {localUsers.length === 0 && (
                  <tr>
                    <td colSpan={5} style={{ color: "var(--text-muted)", textAlign: "center" }}>
                      No local users
                    </td>
                  </tr>
                )}
                {paged.map((u) => (
                  <>
                    <tr key={u.id}>
                      <td style={{ fontFamily: "monospace" }}>
                        <button
                          style={{
                            background: "none",
                            border: "none",
                            color: "var(--accent)",
                            cursor: "pointer",
                            padding: 0,
                            fontFamily: "monospace",
                          }}
                          onClick={() => handleExpandUser(u.id)}
                        >
                          {expandedUserId === u.id ? "▾" : "▸"} {u.username}
                        </button>
                      </td>
                      <td>{u.email || "—"}</td>
                      <td>{u.display_name || "—"}</td>
                      <td>{u.is_active ? "Yes" : "No"}</td>
                      <td>
                        <button
                          className="btn-icon-danger"
                          title="Delete"
                          onClick={() => handleDeleteUser(u.id, u.username)}
                        >
                          <Trash2 size={14} />
                        </button>
                      </td>
                    </tr>
                    {expandedUserId === u.id && (
                      <tr key={`${u.id}-assign`}>
                        <td
                          colSpan={5}
                          style={{
                            paddingLeft: "2rem",
                            background: "var(--bg-alt, var(--bg))",
                          }}
                        >
                          <div style={{ padding: "0.75rem 0" }}>
                            <strong style={{ fontSize: "0.85rem" }}>Role:Domain Assignments</strong>
                            <div
                              style={{
                                marginTop: "0.5rem",
                                display: "flex",
                                flexWrap: "wrap",
                                gap: "0.4rem",
                              }}
                            >
                              {(userAssignments[u.id] ?? []).length === 0 && (
                                <span style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                                  No assignments
                                </span>
                              )}
                              {(userAssignments[u.id] ?? []).map((a) => (
                                <span
                                  key={a.id}
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
                                  {a.role_id}:{a.domain_id}
                                  <button
                                    style={{
                                      background: "none",
                                      border: "none",
                                      color: "var(--danger, #e55)",
                                      cursor: "pointer",
                                      padding: 0,
                                      lineHeight: 1,
                                    }}
                                    onClick={() => handleRemoveAssignment(u.id, a.id)}
                                    title="Remove"
                                  >
                                    ×
                                  </button>
                                </span>
                              ))}
                            </div>
                            <div
                              style={{
                                marginTop: "0.5rem",
                                display: "flex",
                                gap: "0.5rem",
                                alignItems: "center",
                              }}
                            >
                              <select
                                value={assignRole}
                                onChange={(e) => setAssignRole(e.target.value)}
                                style={{
                                  background: "var(--bg)",
                                  color: "var(--text)",
                                  border: "1px solid var(--border)",
                                  padding: "0.3rem",
                                  borderRadius: "4px",
                                  fontSize: "0.85rem",
                                }}
                              >
                                {allRoles.map((r) => (
                                  <option key={r} value={r}>
                                    {r}
                                  </option>
                                ))}
                              </select>
                              <select
                                value={assignDomain}
                                onChange={(e) => setAssignDomain(e.target.value)}
                                style={{
                                  background: "var(--bg)",
                                  color: "var(--text)",
                                  border: "1px solid var(--border)",
                                  padding: "0.3rem",
                                  borderRadius: "4px",
                                  fontSize: "0.85rem",
                                }}
                              >
                                <option value="*">* (all domains)</option>
                                {allDomains.map((d) => (
                                  <option key={d} value={d}>
                                    {d}
                                  </option>
                                ))}
                              </select>
                              <button
                                className="btn-primary"
                                style={{ fontSize: "0.8rem", padding: "0.3rem 0.7rem" }}
                                onClick={() => handleAddAssignment(u.id)}
                                disabled={!assignRole}
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
                <button onClick={() => setUserPage(0)} disabled={userPage === 0}>
                  «
                </button>
                <button onClick={() => setUserPage((p) => p - 1)} disabled={userPage === 0}>
                  ‹
                </button>
                <span>
                  Page {userPage + 1} / {totalPages}
                </span>
                <button
                  onClick={() => setUserPage((p) => p + 1)}
                  disabled={userPage >= totalPages - 1}
                >
                  ›
                </button>
                <button
                  onClick={() => setUserPage(totalPages - 1)}
                  disabled={userPage >= totalPages - 1}
                >
                  »
                </button>
              </div>
            )}
          </div>
        );
      })()}
      <h4 style={{ marginBottom: "0.5rem" }}>Create User</h4>
      <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem", maxWidth: "480px" }}>
        <input
          value={newUsername}
          onChange={(e) => setNewUsername(e.target.value)}
          placeholder="Username *"
          style={{
            background: "var(--bg)",
            color: "var(--text)",
            border: "1px solid var(--border)",
            padding: "0.5rem",
            borderRadius: "4px",
          }}
        />
        <input
          type="password"
          value={newPassword}
          onChange={(e) => setNewPassword(e.target.value)}
          placeholder="Password *"
          style={{
            background: "var(--bg)",
            color: "var(--text)",
            border: "1px solid var(--border)",
            padding: "0.5rem",
            borderRadius: "4px",
          }}
        />
        <input
          value={newEmail}
          onChange={(e) => setNewEmail(e.target.value)}
          placeholder="Email (optional)"
          style={{
            background: "var(--bg)",
            color: "var(--text)",
            border: "1px solid var(--border)",
            padding: "0.5rem",
            borderRadius: "4px",
          }}
        />
        <input
          value={newDisplayName}
          onChange={(e) => setNewDisplayName(e.target.value)}
          placeholder="Display name (optional)"
          style={{
            background: "var(--bg)",
            color: "var(--text)",
            border: "1px solid var(--border)",
            padding: "0.5rem",
            borderRadius: "4px",
          }}
        />
        <button
          className="btn-primary"
          onClick={handleAddUser}
          disabled={!newUsername.trim() || !newPassword.trim()}
          style={{ alignSelf: "flex-start" }}
        >
          + User
        </button>
      </div>
    </>
  );
}
