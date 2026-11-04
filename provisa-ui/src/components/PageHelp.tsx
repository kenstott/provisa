// Copyright (c) 2026 Kenneth Stott
// Canary: 3c9e7a52-1f64-4b8d-a2e0-7d5b9c8f4e13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useEffect, useRef, useState } from "react";
import { ActionIcon, Popover, Stack, Text } from "@mantine/core";
import { CircleHelp } from "lucide-react";

/**
 * A page's own (?) help: what the page is, who uses what it holds, and the one or two rules a
 * reader cannot see on the screen. It sits last among the page's head actions. Like HelpBubble it
 * opens on hover and on keyboard focus, but it stays open while the pointer is over the icon or the
 * card, so the text can be read and selected; leaving both closes it after a short delay. A click
 * toggles it too (the way a touch screen opens it); Escape or a click outside closes it.
 */

/** Long enough to cross the gap between the icon and the card, short enough not to be waited for. */
export const PAGE_HELP_CLOSE_DELAY_MS = 150;

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
  const timer = useRef<number | null>(null);
  const cancel = useCallback(() => {
    if (timer.current !== null) window.clearTimeout(timer.current);
    timer.current = null;
  }, []);
  useEffect(() => cancel, [cancel]);
  const open = () => {
    cancel();
    setOpened(true);
  };
  const closeSoon = () => {
    cancel();
    timer.current = window.setTimeout(() => setOpened(false), PAGE_HELP_CLOSE_DELAY_MS);
  };
  const set = (next: boolean) => {
    cancel();
    setOpened(next);
  };
  return (
    <Popover
      opened={opened}
      onChange={set}
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
          onClick={() => set(!opened)}
          onMouseEnter={open}
          onMouseLeave={closeSoon}
          onFocus={(e) => {
            // A click focuses too; only a focus the keyboard made opens it, so the click that
            // follows a press still toggles.
            if (e.currentTarget.matches(":focus-visible")) open();
          }}
          onBlur={closeSoon}
        >
          <CircleHelp size={16} />
        </ActionIcon>
      </Popover.Target>
      <Popover.Dropdown
        aria-label={title}
        data-testid={`${testId}-card`}
        onMouseEnter={open}
        onMouseLeave={closeSoon}
      >
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
