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
import { Button } from "@mantine/core";

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
    <Button
      onClick={onClick}
      title={title}
      aria-pressed={active}
      variant="default"
      size="compact-xs"
      data-testid="erd-toolbar-btn"
      styles={{
        root: {
          padding: "2px 8px",
          fontSize: 11,
          height: "auto",
          minHeight: 0,
          background: active ? "var(--surface-alt)" : "transparent",
          color: active ? "var(--text)" : "var(--text-muted)",
          border: "1px solid var(--border)",
          borderRadius: 4,
        },
        label: {
          display: "flex",
          alignItems: "center",
          gap: 4,
        },
      }}
    >
      {children}
    </Button>
  );
}
