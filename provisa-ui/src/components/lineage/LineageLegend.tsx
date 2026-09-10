// Copyright (c) 2026 Kenneth Stott
// Canary: 2a9c4e17-6d83-4b5f-a0e2-7c1d8f3b6e94
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The colour key for a LineageDag — one definition, shown wherever the DAG is (REQ-1667).

import type { ReactNode } from "react";
import { Group } from "@mantine/core";
import { ROLE_STYLE } from "./lineage-layout";
import type { Role } from "./lineage-layout";

// Each entry is drawn the way the DAG draws a field of that role — same colour, weight and style
// from ROLE_STYLE on a white row — so the legend IS a sample of the graph.
const LEGEND_ROLES: { label: string; role: Role }[] = [
  { label: "source", role: "source" },
  { label: "intermediate", role: "intermediate" },
  { label: "result", role: "output" },
];

export function LineageLegend({ children }: { children?: ReactNode }) {
  return (
    <Group gap="md" data-testid="lineage-legend">
      {LEGEND_ROLES.map((l) => (
        <div
          key={l.label}
          style={{
            background: ROLE_STYLE[l.role].fill,
            border: "1px solid #adb5bd",
            borderRadius: 6,
            padding: "2px 12px",
            fontFamily: "monospace",
            fontSize: 10,
            lineHeight: "16px",
            color: ROLE_STYLE[l.role].color,
            fontStyle: ROLE_STYLE[l.role].fontStyle,
            fontWeight: ROLE_STYLE[l.role].fontWeight,
          }}
        >
          {l.label}
        </div>
      ))}
      {children}
    </Group>
  );
}
