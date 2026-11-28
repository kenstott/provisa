// Copyright (c) 2026 Kenneth Stott
// Canary: cc3638d9-97e7-4b08-bc0f-5e069f0dd96c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// One command of a page header's action row (PageActions): the control, what it does, and its
// documentation (maintainer 2026-11-28). In the row, hovering the control opens a card with the
// description and a "Read the documentation" link; the card is a HoverCard, so the pointer can move
// onto it and follow the link. Moved into the (...) menu, the control shows the same text and link
// beneath it. toolbarGuard fails a PageActions child that is not a PageCommand with a description
// and a docs target.

import type { ReactNode } from "react";
import { HoverCard, Stack, Text } from "@mantine/core";
import { DocsLink } from "./DocsLink";
import type { DocsTarget } from "./docsTarget";
import { PAGE_HELP_CLOSE_DELAY_MS } from "./PageHelp";
import { TIP_MAX_WIDTH, TIP_OPEN_DELAY_MS } from "./Tip";

export interface PageCommandProps {
  /** What the command does: one plain sentence, translated. */
  description: string;
  /** The documentation of what the command does. */
  docs: DocsTarget;
  /** The command's control (a button, a split button, a menu). */
  children: ReactNode;
  /** Set by PageActions for a command it has moved into its (...) menu. */
  inMenu?: boolean;
}

export function PageCommand({ description, docs, children, inMenu = false }: PageCommandProps) {
  if (inMenu) {
    return (
      <Stack gap={4} maw={TIP_MAX_WIDTH} className="page-command" data-testid="page-command">
        <div>{children}</div>
        <Text size="xs" c="dimmed" data-testid="page-command-description">
          {description}
        </Text>
        <DocsLink target={docs} testId="page-command-docs" />
      </Stack>
    );
  }
  return (
    <HoverCard
      openDelay={TIP_OPEN_DELAY_MS}
      closeDelay={PAGE_HELP_CLOSE_DELAY_MS}
      position="bottom-end"
      width={TIP_MAX_WIDTH}
      shadow="md"
      withArrow
    >
      <HoverCard.Target>
        <span className="page-command" data-testid="page-command">
          {children}
        </span>
      </HoverCard.Target>
      <HoverCard.Dropdown data-testid="page-command-card">
        <Stack gap="xs">
          <Text size="xs" data-testid="page-command-description">
            {description}
          </Text>
          <DocsLink target={docs} testId="page-command-docs" />
        </Stack>
      </HoverCard.Dropdown>
    </HoverCard>
  );
}
