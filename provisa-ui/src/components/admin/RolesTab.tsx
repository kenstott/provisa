// Copyright (c) 2026 Kenneth Stott
// Canary: 5f6d7095-5246-445f-bb37-5106cc619ea2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect } from "react";
import { Trash2 } from "lucide-react";
import { fetchOrgRoles, deleteOrgRole } from "../../api/admin";
import type { Role } from "../../types/auth";

const PAGE_SIZE = 50;

interface RolesTabProps {
  orgId: string;
}

export function RolesTab({ orgId }: RolesTabProps) {
  const [orgRoles, setOrgRoles] = useState<Role[]>([]);
  const [roleMsg, setRoleMsg] = useState("");
  const [rolePage, setRolePage] = useState(0);

  useEffect(() => {
    fetchOrgRoles(orgId)
      .then(setOrgRoles)
      .catch(() => setOrgRoles([]));
  }, [orgId]);

  const handleDeleteOrgRole = async (roleId: string) => {
    await deleteOrgRole(orgId, roleId);
    setOrgRoles((prev) => prev.filter((r) => r.id !== roleId));
    setRoleMsg(`Deleted "${roleId}"`);
  };

  return (
    <div>
      <h3>Roles — {orgId}</h3>
      {roleMsg && <p className="form-msg">{roleMsg}</p>}
      {(() => {
        const totalPages = Math.max(1, Math.ceil(orgRoles.length / PAGE_SIZE));
        const paged = orgRoles.slice(rolePage * PAGE_SIZE, (rolePage + 1) * PAGE_SIZE);
        return (
          <div>
            <table className="admin-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Capabilities</th>
                  <th>Domain Access</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {paged.map((role) => (
                  <tr key={role.id}>
                    <td>{role.id}</td>
                    <td>{role.capabilities.join(", ")}</td>
                    <td>{role.domain_access.join(", ")}</td>
                    <td>
                      <button className="btn-danger" onClick={() => handleDeleteOrgRole(role.id)}>
                        <Trash2 size={14} />
                      </button>
                    </td>
                  </tr>
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
                <button onClick={() => setRolePage(0)} disabled={rolePage === 0}>
                  «
                </button>
                <button onClick={() => setRolePage((p) => p - 1)} disabled={rolePage === 0}>
                  ‹
                </button>
                <span>
                  Page {rolePage + 1} / {totalPages}
                </span>
                <button
                  onClick={() => setRolePage((p) => p + 1)}
                  disabled={rolePage >= totalPages - 1}
                >
                  ›
                </button>
                <button
                  onClick={() => setRolePage(totalPages - 1)}
                  disabled={rolePage >= totalPages - 1}
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
