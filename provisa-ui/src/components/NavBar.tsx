// Copyright (c) 2026 Kenneth Stott
// Canary: bd13514a-c705-475b-bf21-997c34eaaab5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Link } from "react-router-dom";
import { CapabilityGate } from "./CapabilityGate";
import { RoleSelector } from "./RoleSelector";

export function NavBar() {
  return (
    <nav className="navbar">
      <div className="navbar-brand">
        <Link to="/">Provisa</Link>
      </div>
      <div className="navbar-links">
        <CapabilityGate capability="source_registration">
          <Link to="/sources">Sources</Link>
        </CapabilityGate>
        <CapabilityGate capability="table_registration">
          <Link to="/tables">Tables</Link>
        </CapabilityGate>
        <CapabilityGate capability="table_registration">
          <Link to="/views">Views</Link>
        </CapabilityGate>
        <CapabilityGate capability="admin">
          <Link to="/commands">Commands</Link>
        </CapabilityGate>
        <CapabilityGate capability="relationship_registration">
          <Link to="/relationships">Relationships</Link>
        </CapabilityGate>
        <CapabilityGate capability="security_config">
          <Link to="/security">Security</Link>
        </CapabilityGate>
        <CapabilityGate capability="query_development">
          <Link to="/schema">Schema Explorer</Link>
        </CapabilityGate>
        <CapabilityGate capability="query_development">
          <Link to="/query">Query</Link>
        </CapabilityGate>
        <CapabilityGate capability="query_approval">
          <Link to="/approvals">Approvals</Link>
        </CapabilityGate>
        <CapabilityGate capability="admin">
          <Link to="/admin">Admin</Link>
        </CapabilityGate>
      </div>
      <div className="navbar-role">
        <RoleSelector />
      </div>
    </nav>
  );
}
