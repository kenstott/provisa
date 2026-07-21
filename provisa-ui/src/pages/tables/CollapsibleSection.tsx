// Copyright (c) 2026 Kenneth Stott
// Canary: 6f2b9d14-7c38-4a56-9e01-3d5c8f4b7a29
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A per-MV config panel that collapses to reduce clutter. Children stay mounted (Mantine Collapse),
// so form state and testability are unaffected — the header only toggles visibility.

import { useState, type ReactNode } from "react";
import { ActionIcon, Badge, Collapse, Group, Text } from "@mantine/core";

export function CollapsibleSection({
  title,
  testId,
  badge,
  defaultOpen = false,
  children,
}: {
  title: string;
  testId: string;
  badge?: ReactNode;
  defaultOpen?: boolean;
  children: ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div style={{ gridColumn: "1 / -1" }}>
      <Group
        gap="xs"
        wrap="nowrap"
        style={{ cursor: "pointer" }}
        onClick={() => setOpen((o) => !o)}
        data-testid={`${testId}-toggle`}
        role="button"
        aria-expanded={open}
      >
        <ActionIcon variant="subtle" size="sm" aria-hidden>
          {open ? "−" : "+"}
        </ActionIcon>
        <Text fw={600} size="sm">
          {title}
        </Text>
        {badge && (
          <Badge size="xs" variant="light" color="grape">
            {badge}
          </Badge>
        )}
      </Group>
      <Collapse in={open}>
        <div
          style={{
            display: "grid",
            gap: "var(--mantine-spacing-sm)",
            paddingTop: "var(--mantine-spacing-xs)",
          }}
        >
          {children}
        </div>
      </Collapse>
    </div>
  );
}
