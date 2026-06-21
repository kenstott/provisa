// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder

import { useState, useEffect, useRef } from "react";
import { useAuth } from "../context/AuthContext";
import { fetchOrgs } from "../api/admin";
import type { Org } from "../api/admin";

export function OrgSwitcher() {
  const { capabilities, orgMemberships, activeOrgId, selectOrg } = useAuth();
  const [open, setOpen] = useState(false);
  const [allOrgs, setAllOrgs] = useState<Org[]>([]);
  const ref = useRef<HTMLDivElement>(null);

  const isSuperAdmin = capabilities.includes("superadmin") || capabilities.includes("admin");

  useEffect(() => {
    if (!isSuperAdmin) return;
    fetchOrgs().then(setAllOrgs).catch(() => {});
  }, [isSuperAdmin]);

  useEffect(() => {
    if (!open) return;
    const handleClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [open]);

  const orgs: Array<{ id: string; name: string }> = isSuperAdmin
    ? allOrgs.map((o) => ({ id: o.id, name: o.name }))
    : orgMemberships.map((m) => ({ id: m.org_id, name: m.org_name }));

  const activeOrg = orgs.find((o) => o.id === activeOrgId);
  const orgName = activeOrg?.name ?? activeOrgId ?? "";

  if (!isSuperAdmin && orgMemberships.length <= 1) {
    if (orgMemberships.length === 0) return null;
    return <span className="role-selector">Org: {orgName}</span>;
  }

  function handleSelect(orgId: string) {
    selectOrg(orgId);
    setOpen(false);
  }

  return (
    <div className="role-selector" ref={ref}>
      <button
        type="button"
        className="role-selector-trigger"
        onClick={() => setOpen(!open)}
        aria-expanded={open}
      >
        <span className="role-selector-label">Org: {orgName}</span>
        <span className="role-selector-arrow">{open ? "▴" : "▾"}</span>
      </button>
      {open && (
        <div className="role-selector-dropdown">
          {orgs.map((o) => (
            <div
              key={o.id}
              className={`role-selector-option${o.id === activeOrgId ? " role-selector-option--selected" : ""}`}
              onClick={() => handleSelect(o.id)}
              role="option"
              aria-selected={o.id === activeOrgId}
            >
              {o.name}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
