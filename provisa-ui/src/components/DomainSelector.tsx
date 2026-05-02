// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useRef } from "react";
import { useAuth } from "../context/AuthContext";

export function DomainSelector() {
  const { selectedDomain, availableDomains, selectDomain } = useAuth();
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

  const label = selectedDomain ?? "All Domains";

  function handleSelect(domain: string | null) {
    selectDomain(domain);
    setOpen(false);
  }

  return (
    <div className="domain-selector" ref={ref}>
      <button
        type="button"
        className="domain-selector-trigger"
        onClick={() => setOpen(!open)}
        aria-expanded={open}
      >
        <span className="domain-selector-label">Domain: {label}</span>
        <span className="domain-selector-arrow">{open ? "▴" : "▾"}</span>
      </button>
      {open && (
        <div className="domain-selector-dropdown">
          <div
            className={`domain-selector-option${selectedDomain === null ? " domain-selector-option--selected" : ""}`}
            onClick={() => handleSelect(null)}
            role="option"
            aria-selected={selectedDomain === null}
          >
            All Domains
          </div>
          {availableDomains.map((d) => (
            <div
              key={d}
              className={`domain-selector-option${selectedDomain === d ? " domain-selector-option--selected" : ""}`}
              onClick={() => handleSelect(d)}
              role="option"
              aria-selected={selectedDomain === d}
            >
              {d}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
