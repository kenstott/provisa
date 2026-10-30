// Copyright (c) 2026 Kenneth Stott
// Canary: 3b7e0d52-91c4-4a6f-b8e2-5d0f7a1c9e64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The dense card panels of a data product's detail: the product's own, and each of its contracts'
// when its row in the contracts list is open (REQ-2158).

import React from "react";
import { ActionIcon, Group, Text } from "@mantine/core";
import { CircleHelp } from "lucide-react";
import { Tip } from "../../components/Tip";
import { CollapsibleItem, Collapsibles } from "../../components/collapsible/Collapsible";

// A card in the masonry: kept whole within its column.
const CARD_IN_COLUMN: React.CSSProperties = {
  breakInside: "avoid",
  minWidth: 0,
  marginBottom: "var(--mantine-spacing-sm)",
};
// A card's content stays inside the card: long words wrap, and what cannot wrap (a table's
// columns) scrolls within it.
const CARD_BODY: React.CSSProperties = {
  minWidth: 0,
  overflowX: "auto",
  overflowWrap: "anywhere",
};

/** One card of the panel: the application's collapsible (one look, its open or closed state
 * remembered per section), headed by its label and, behind (?), what it holds. ``wide`` spans the
 * panel's width, outside the columns. */
export function DetailCard({
  section,
  label,
  hint,
  testId,
  tourId,
  wide = false,
  children,
}: {
  section: string;
  label: string;
  hint?: string;
  testId: string;
  tourId?: string;
  wide?: boolean;
  children: React.ReactNode;
}) {
  const heading = (
    <Group gap="xs" wrap="nowrap" component="span">
      <span>{label}</span>
      {hint && (
        <Tip text={hint}>
          <ActionIcon
            component="span"
            variant="subtle"
            color="gray"
            size="xs"
            role="button"
            tabIndex={0}
            aria-label={hint}
            data-testid={`${testId}-hint`}
            // The icon sits in the heading; using it must not also open or close the card.
            onClick={(e) => e.stopPropagation()}
            onKeyDown={(e) => e.stopPropagation()}
          >
            <CircleHelp size={14} aria-hidden />
          </ActionIcon>
        </Tip>
      )}
    </Group>
  );
  return (
    <div
      style={wide ? undefined : CARD_IN_COLUMN}
      data-tour={tourId}
      data-testid={testId}
    >
      <Collapsibles id={`data-product.${section}`}>
        <CollapsibleItem value={section} title={heading} controlTestId={`${testId}-toggle`}>
          <div style={CARD_BODY}>{children}</div>
        </CollapsibleItem>
      </Collapsibles>
    </div>
  );
}

export function Empty({ text }: { text: string }) {
  return (
    <Text size="sm" c="var(--text-muted)">
      {text}
    </Text>
  );
}

export function Muted({ children }: { children: React.ReactNode }) {
  return (
    <Text component="span" c="var(--text-muted)" ff="text" size="xs">
      {" "}
      {children}
    </Text>
  );
}
