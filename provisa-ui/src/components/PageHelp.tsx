// Copyright (c) 2026 Kenneth Stott
// Canary: 3c9e7a52-1f64-4b8d-a2e0-7d5b9c8f4e13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { ActionIcon, Popover, Stack, Text } from "@mantine/core";
import { CircleHelp } from "lucide-react";

/**
 * A page's own (?) help: what the page is, who uses what it holds, and the one or two rules a
 * reader cannot see on the screen. It sits last among the page's head actions. Unlike HelpBubble,
 * which explains one control on hover, it opens on a click and stays open while it is read; Escape
 * or a click outside closes it.
 */
export function PageHelp({
  title,
  paragraphs,
  ariaLabel,
  testId,
}: {
  title: string;
  paragraphs: string[];
  ariaLabel: string;
  testId: string;
}) {
  const [opened, setOpened] = useState(false);
  return (
    <Popover
      opened={opened}
      onChange={setOpened}
      position="bottom-end"
      width={360}
      withArrow
      shadow="md"
      closeOnEscape
      closeOnClickOutside
      returnFocus
    >
      <Popover.Target>
        <ActionIcon
          variant="subtle"
          color="gray"
          aria-label={ariaLabel}
          data-testid={testId}
          onClick={() => setOpened((o) => !o)}
        >
          <CircleHelp size={16} />
        </ActionIcon>
      </Popover.Target>
      <Popover.Dropdown aria-label={title} data-testid={`${testId}-card`}>
        <Stack gap="xs">
          <Text size="sm" fw={600}>
            {title}
          </Text>
          {paragraphs.map((p) => (
            <Text key={p} size="xs">
              {p}
            </Text>
          ))}
        </Stack>
      </Popover.Dropdown>
    </Popover>
  );
}
