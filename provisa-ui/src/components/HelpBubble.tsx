// Copyright (c) 2026 Kenneth Stott
// Canary: fb81a4a2-6ded-49c4-bf70-fef30f50f05c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { ComponentProps } from "react";
import { Stack, Text } from "@mantine/core";
import type { DocsTarget } from "./docsTarget";
import { PageHelp } from "./PageHelp";
import { TipSurface } from "./Tip";

/**
 * The explanation attached to a page control. It answers what the control is FOR — what
 * the object does once it exists, or what the tool behind an icon actually opens — because
 * the label already says what it does and the purpose is the part nobody can read off the
 * screen.
 *
 * Beside an add button it renders its own "?" target: a (?) help, which ends with a "Read the
 * documentation" link to `docs` (maintainer 2026-11-28), so it is drawn as the page help is
 * (PageHelp), a card the pointer can move onto to follow the link. An icon-only button is already
 * the opaque thing being explained, so it passes itself as `target` and hovering the icon opens
 * the card: a second "?" per icon would double the row of glyphs it is meant to decode.
 */
type HelpBubbleProps = {
  title: string;
  paragraphs: string[];
  ariaLabel: string;
  testId: string;
} & (
  | { target: ComponentProps<typeof TipSurface>["children"]; docs?: never }
  | { target?: undefined; docs: DocsTarget }
);

export function HelpBubble({
  title,
  paragraphs,
  ariaLabel,
  testId,
  target,
  docs,
}: HelpBubbleProps) {
  if (target === undefined) {
    return (
      <PageHelp
        title={title}
        paragraphs={paragraphs}
        ariaLabel={ariaLabel}
        testId={testId}
        docs={docs}
      />
    );
  }
  return (
    // The same surface as every one-line hint (components/Tip.tsx), as a titled card.
    <TipSurface
      card
      position="bottom-end"
      testId={`${testId}-card`}
      content={
        <Stack gap="xs">
          <Text size="sm" fw={600}>
            {title}
          </Text>
          {paragraphs.map((p) => (
            <Text key={p} size="xs" c="dimmed">
              {p}
            </Text>
          ))}
        </Stack>
      }
    >
      {target}
    </TipSurface>
  );
}
