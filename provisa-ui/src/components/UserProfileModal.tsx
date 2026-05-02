// Copyright (c) 2026 Kenneth Stott
// Canary: e7f2a1b3-c4d5-6789-abcd-ef0123456789
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { X } from "lucide-react";
import { useAuth } from "../context/AuthContext";

interface Props {
  onClose: () => void;
}

export function UserProfileModal({ onClose }: Props) {
  const { displayName, email, userId, devMode, availableRoles, assignments, capabilities, orgMemberships, activeOrgId } = useAuth();

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal modal--wide" onClick={(e) => e.stopPropagation()} style={{ maxWidth: 560 }}>
        <div className="modal-header">
          <h3>Profile</h3>
          <button className="modal-close" onClick={onClose}><X size={16} /></button>
        </div>
        <div className="modal-body" style={{ display: "flex", flexDirection: "column", gap: "1.25rem", padding: "1.25rem" }}>

          <section>
            <h4 style={{ margin: "0 0 0.5rem", fontSize: "0.75rem", textTransform: "uppercase", color: "var(--text-muted)", letterSpacing: "0.05em" }}>Identity</h4>
            <dl style={{ display: "grid", gridTemplateColumns: "max-content 1fr", gap: "0.25rem 1rem", margin: 0 }}>
              {displayName && <><dt style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>Name</dt><dd style={{ margin: 0, fontSize: "0.85rem" }}>{displayName}</dd></>}
              {email && <><dt style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>Email</dt><dd style={{ margin: 0, fontSize: "0.85rem" }}>{email}</dd></>}
              {userId && <><dt style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>User ID</dt><dd style={{ margin: 0, fontSize: "0.85rem", fontFamily: "monospace" }}>{userId}</dd></>}
              {activeOrgId && <><dt style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>Org</dt><dd style={{ margin: 0, fontSize: "0.85rem" }}>{orgMemberships.find((m) => m.org_id === activeOrgId)?.org_name ?? activeOrgId}</dd></>}
              {devMode && <><dt style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>Mode</dt><dd style={{ margin: 0 }}><span style={{ fontSize: "0.7rem", fontWeight: 600, padding: "1px 6px", borderRadius: 3, background: "var(--text-muted)", color: "var(--bg)" }}>DEV</span></dd></>}
            </dl>
          </section>

          <section>
            <h4 style={{ margin: "0 0 0.5rem", fontSize: "0.75rem", textTransform: "uppercase", color: "var(--text-muted)", letterSpacing: "0.05em" }}>Roles & Domain Access</h4>
            {availableRoles.length === 0 ? (
              <span style={{ fontSize: "0.85rem", color: "var(--text-muted)" }}>No roles assigned</span>
            ) : (
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.82rem" }}>
                <thead>
                  <tr style={{ borderBottom: "1px solid var(--border)" }}>
                    <th style={{ textAlign: "left", padding: "0.25rem 0.5rem 0.25rem 0", color: "var(--text-muted)", fontWeight: 500 }}>Role</th>
                    <th style={{ textAlign: "left", padding: "0.25rem 0.5rem", color: "var(--text-muted)", fontWeight: 500 }}>Domains</th>
                  </tr>
                </thead>
                <tbody>
                  {availableRoles.map((role) => {
                    const domains = assignments
                      .filter((a) => a.role_id === role.id)
                      .map((a) => a.domain_id);
                    return (
                      <tr key={role.id} style={{ borderBottom: "1px solid var(--border)" }}>
                        <td style={{ padding: "0.35rem 0.5rem 0.35rem 0", fontFamily: "monospace" }}>{role.id}</td>
                        <td style={{ padding: "0.35rem 0.5rem" }}>
                          {domains.length === 0 ? <span style={{ color: "var(--text-muted)" }}>—</span>
                            : domains.includes("*") ? <span style={{ color: "var(--approve)" }}>all</span>
                            : domains.join(", ")}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </section>

          <section>
            <h4 style={{ margin: "0 0 0.5rem", fontSize: "0.75rem", textTransform: "uppercase", color: "var(--text-muted)", letterSpacing: "0.05em" }}>Capabilities</h4>
            {capabilities.length === 0 ? (
              <span style={{ fontSize: "0.85rem", color: "var(--text-muted)" }}>None</span>
            ) : (
              <div style={{ display: "flex", flexWrap: "wrap", gap: "0.35rem" }}>
                {capabilities.map((cap) => (
                  <span key={cap} style={{ fontSize: "0.72rem", padding: "2px 8px", borderRadius: 12, background: "var(--surface)", border: "1px solid var(--border)", fontFamily: "monospace" }}>
                    {cap}
                  </span>
                ))}
              </div>
            )}
          </section>

        </div>
      </div>
    </div>
  );
}
