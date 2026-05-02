// Copyright (c) 2026 Kenneth Stott
// Canary: b7e1733a-e373-4838-b104-3007b8107524
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useRef } from "react";
import { useAuth } from "../context/AuthContext";
import type { Role } from "../types/auth";

export function RoleSelector() {
  const { selectedRole, availableRoles, selectRole, devMode } = useAuth();
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

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

  if (availableRoles.length === 0) return <span>No roles configured</span>;

  const label = selectedRole === "all" ? "All" : (selectedRole as Role).id;

  function handleSelect(value: Role | "all") {
    selectRole(value);
    setOpen(false);
  }

  return (
    <div className={`role-selector${devMode ? " role-selector--dev" : ""}`} ref={ref}>
      <button
        type="button"
        className="role-selector-trigger"
        onClick={() => setOpen(!open)}
        aria-expanded={open}
      >
        <span className="role-selector-label">Role: {label}</span>
        {devMode && <span className="role-selector-dev-badge">DEV</span>}
        <span className="role-selector-arrow">{open ? "▴" : "▾"}</span>
      </button>
      {open && (
        <div className="role-selector-dropdown">
          <div
            className={`role-selector-option${selectedRole === "all" ? " role-selector-option--selected" : ""}`}
            onClick={() => handleSelect("all")}
            role="option"
            aria-selected={selectedRole === "all"}
          >
            All
          </div>
          {availableRoles.map((r) => (
            <div
              key={r.id}
              className={`role-selector-option${selectedRole !== "all" && (selectedRole as Role).id === r.id ? " role-selector-option--selected" : ""}`}
              onClick={() => handleSelect(r)}
              role="option"
              aria-selected={selectedRole !== "all" && (selectedRole as Role).id === r.id}
            >
              {r.id}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
