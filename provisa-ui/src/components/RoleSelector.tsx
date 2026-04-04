import { useState, useEffect, useRef } from "react";
import { useAuth } from "../context/AuthContext";

/** Multi-role selector — appears in the header as a chip-based dropdown. */
export function RoleSelector() {
  const { selectedRoles, availableRoles, toggleRole } = useAuth();
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

  const selectedIds = new Set(selectedRoles.map((r) => r.id));
  const label = selectedRoles.map((r) => r.id).join(", ");

  return (
    <div className="role-selector" ref={ref}>
      <button
        type="button"
        className="role-selector-trigger"
        onClick={() => setOpen(!open)}
        aria-expanded={open}
      >
        <span className="role-selector-label">Role: {label}</span>
        <span className="role-selector-arrow">{open ? "\u25B4" : "\u25BE"}</span>
      </button>
      {open && (
        <div className="role-selector-dropdown">
          {availableRoles.map((r) => (
            <label key={r.id} className="role-selector-option">
              <input
                type="checkbox"
                checked={selectedIds.has(r.id)}
                onChange={() => toggleRole(r)}
              />
              {r.id}
            </label>
          ))}
        </div>
      )}
    </div>
  );
}
