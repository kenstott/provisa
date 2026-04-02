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
        <CapabilityGate capability="relationship_registration">
          <Link to="/relationships">Relationships</Link>
        </CapabilityGate>
        <CapabilityGate capability="table_registration">
          <Link to="/views">Views</Link>
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
