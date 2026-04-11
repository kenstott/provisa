// Copyright (c) 2026 Kenneth Stott
// Canary: bd13514a-c705-475b-bf21-997c34eaaab5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { NavLink } from "react-router-dom";
import { CapabilityGate } from "./CapabilityGate";
import { RoleSelector } from "./RoleSelector";

export function NavBar() {
  return (
    <nav className="navbar">
      <div className="navbar-brand">
        <NavLink to="/">Provisa</NavLink>
      </div>
      <div className="navbar-links">
        <CapabilityGate capability="source_registration">
          <NavLink to="/sources">Sources</NavLink>
        </CapabilityGate>
        <CapabilityGate capability="table_registration">
          <NavLink to="/tables">Tables</NavLink>
        </CapabilityGate>
        <CapabilityGate capability="table_registration">
          <NavLink to="/views">Views</NavLink>
        </CapabilityGate>
        <CapabilityGate capability="admin">
          <NavLink to="/commands">Commands</NavLink>
        </CapabilityGate>
        <CapabilityGate capability="relationship_registration">
          <NavLink to="/relationships">Relationships</NavLink>
        </CapabilityGate>
        <CapabilityGate capability="security_config">
          <NavLink to="/security">Security</NavLink>
        </CapabilityGate>
        <CapabilityGate capability="query_development">
          <NavLink to="/schema">Schema</NavLink>
        </CapabilityGate>
        <CapabilityGate capability="query_development">
          <NavLink to="/graph">Graph</NavLink>
        </CapabilityGate>
        <CapabilityGate capability="query_development">
          <NavLink to="/query">Query</NavLink>
        </CapabilityGate>
        <CapabilityGate capability="query_approval">
          <NavLink to="/approvals">Approvals</NavLink>
        </CapabilityGate>
        <CapabilityGate capability="admin">
          <NavLink to="/admin">Admin</NavLink>
        </CapabilityGate>
      </div>
      <div className="navbar-role">
        <RoleSelector />
      </div>
    </nav>
  );
}
