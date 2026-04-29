// Copyright (c) 2026 Kenneth Stott
// Canary: bd13514a-c705-475b-bf21-997c34eaaab5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { NavLink, useLocation } from "react-router-dom";
import { ChevronDown } from "lucide-react";
import { CapabilityGate } from "./CapabilityGate";
import { RoleSelector } from "./RoleSelector";
import { useDomainFilter } from "../context/DomainFilterContext";

const SYSTEM_DOMAINS = new Set(["meta", "ops"]);

interface DropdownItem {
  to: string;
  label: string;
  capability: string;
  comingSoon?: boolean;
}

function NavDropdown({ label, items }: { label: string; items: DropdownItem[] }) {
  const location = useLocation();
  const isActive = items.some((i) => !i.comingSoon && location.pathname === i.to);

  return (
    <div className={`nav-dropdown${isActive ? " nav-dropdown-active" : ""}`}>
      <span className="nav-dropdown-trigger">
        {label} <ChevronDown size={11} />
      </span>
      <div className="nav-dropdown-menu">
        {items.map((item) =>
          item.comingSoon ? (
            <span key={item.to} className="coming-soon">{item.label} — coming soon</span>
          ) : (
            <CapabilityGate key={item.to} capability={item.capability}>
              <NavLink to={item.to}>{item.label}</NavLink>
            </CapabilityGate>
          )
        )}
      </div>
    </div>
  );
}

export function NavBar() {
  const location = useLocation();
  const { domains, selectedDomain, setSelectedDomain } = useDomainFilter();
  const onTablesPage =
    location.pathname === "/tables" ||
    location.pathname === "/relationships" ||
    location.pathname === "/security" ||
    location.pathname === "/schema" ||
    location.pathname === "/query";

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
        <CapabilityGate capability="relationship_registration">
          <NavLink to="/relationships">Relationships</NavLink>
        </CapabilityGate>
        <NavDropdown
          label="Explore"
          items={[
            { to: "/query", label: "GraphQL", capability: "query_development" },
            { to: "/schema", label: "Schema", capability: "query_development" },
            { to: "/graph", label: "Cypher", capability: "query_development" },
            { to: "/sql", label: "SQL", capability: "query_development", comingSoon: true },
          ]}
        />
        <NavDropdown
          label="Model"
          items={[
            { to: "/views", label: "Views", capability: "table_registration" },
            { to: "/commands", label: "Commands", capability: "admin" },
          ]}
        />
        <NavDropdown
          label="Security"
          items={[
            { to: "/security", label: "Policies", capability: "security_config" },
            { to: "/approvals", label: "Approvals", capability: "query_approval" },
          ]}
        />
        <CapabilityGate capability="admin">
          <NavLink to="/admin">Admin</NavLink>
        </CapabilityGate>
      </div>
      <div className="navbar-role">
        {onTablesPage && domains.length > 0 && (
          <select
            className="navbar-domain-select"
            value={selectedDomain}
            onChange={(e) => setSelectedDomain(e.target.value)}
          >
            <option value="all">All Domains</option>
            {domains.filter((d) => !SYSTEM_DOMAINS.has(d)).map((d) => (
              <option key={d} value={d}>{d}</option>
            ))}
          </select>
        )}
        <RoleSelector />
      </div>
    </nav>
  );
}
