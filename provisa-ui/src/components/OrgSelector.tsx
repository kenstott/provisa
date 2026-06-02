// Copyright (c) 2026 Kenneth Stott
// Canary: d83ae114-81d4-4d5c-b79a-0412df654833
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useAuth } from "../context/AuthContext";

export function OrgSelector() {
  const { orgMemberships, activeOrgId, selectOrg } = useAuth();

  if (activeOrgId || orgMemberships.length <= 1) return null;

  return (
    <div className="modal-overlay">
      <div className="modal">
        <h2>Select Organization</h2>
        <p>You belong to multiple organizations. Choose one to continue.</p>
        <ul className="org-list">
          {orgMemberships.map((m) => (
            <li key={m.org_id}>
              <button className="btn-primary" onClick={() => selectOrg(m.org_id)}>
                {m.org_name}
              </button>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
