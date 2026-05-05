// Copyright (c) 2026 Kenneth Stott
// Canary: ea9667ae-371a-414f-be7a-2601c4eb9dfd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useLayoutEffect, useRef } from "react";
import { createPortal } from "react-dom";

export function MultiSelect({ options, value, onChange, className }: {
  options: { id: string; label: string }[];
  value: string[];
  onChange: (selected: string[]) => void;
  className?: string;
}) {
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number; width: number } | null>(null);
  const triggerRef = useRef<HTMLDivElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  const updatePos = () => {
    if (triggerRef.current) {
      const rect = triggerRef.current.getBoundingClientRect();
      setPos({ top: rect.bottom + 2, left: rect.left, width: rect.width });
    }
  };

  useLayoutEffect(() => {
    if (open) updatePos();
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as Node;
      if (
        triggerRef.current && !triggerRef.current.contains(target) &&
        dropdownRef.current && !dropdownRef.current.contains(target)
      ) setOpen(false);
    };
    document.addEventListener("mousedown", handleClickOutside);
    window.addEventListener("scroll", updatePos, true);
    window.addEventListener("resize", updatePos);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
      window.removeEventListener("scroll", updatePos, true);
      window.removeEventListener("resize", updatePos);
    };
  }, [open]);

  const display = value.length > 0 ? value.join(", ") : "all";

  return (
    <div className={`multiselect${className ? ` ${className}` : ""}`} ref={triggerRef}>
      <div className="multiselect-trigger" onClick={() => setOpen(!open)}>
        <span className="multiselect-text">{display}</span>
        <span className="multiselect-arrow">{open ? "▴" : "▾"}</span>
      </div>
      {open && pos && createPortal(
        <div
          className="multiselect-dropdown"
          ref={dropdownRef}
          style={{ top: pos.top, left: pos.left, width: pos.width }}
        >
          {options.map((opt) => (
            <label key={opt.id} className="multiselect-option">
              <input
                type="checkbox"
                checked={value.includes(opt.id)}
                onChange={(e) => {
                  const next = e.target.checked
                    ? [...value, opt.id]
                    : value.filter((v) => v !== opt.id);
                  onChange(next);
                }}
              />
              {opt.label}
            </label>
          ))}
        </div>,
        document.body
      )}
    </div>
  );
}
