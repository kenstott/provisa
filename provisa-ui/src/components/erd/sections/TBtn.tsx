// Copyright (c) 2026 Kenneth Stott
// Canary: a3d9e2f1-7b4c-4a8e-9d5f-2c1b6e3a7f8d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";

// ── small toolbar-button helper ───────────────────────────────────────────────
export function TBtn({
  onClick, title, active, children,
}: {
  onClick: () => void;
  title?: string;
  active?: boolean;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      title={title}
      style={{
        padding: "2px 8px",
        fontSize: 11,
        background: active ? "#334155" : "transparent",
        color: active ? "#e2e8f0" : "#64748b",
        border: "1px solid #334155",
        borderRadius: 4,
        cursor: "pointer",
        display: "flex",
        alignItems: "center",
        gap: 4,
      }}
    >
      {children}
    </button>
  );
}
