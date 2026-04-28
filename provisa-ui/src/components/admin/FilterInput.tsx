// Copyright (c) 2026 Kenneth Stott
// Canary: 154765ce-1e79-4785-bf31-0b39e7dbf0d1
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Copy, X } from "lucide-react";

export function FilterInput({ value, onChange, placeholder }: {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
}) {
  return (
    <div className="search-wrap">
      <input
        type="search"
        className="approvals-search"
        placeholder={placeholder}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
      <div className="filter-hover-btns">
        <button type="button" title="Copy" onClick={() => navigator.clipboard.writeText(value)}><Copy size={11} /></button>
        <button type="button" title="Clear" onClick={() => onChange("")}><X size={11} /></button>
      </div>
    </div>
  );
}
