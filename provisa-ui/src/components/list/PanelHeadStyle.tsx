// Copyright (c) 2026 Kenneth Stott
// Canary: d0356b7d-814e-4d41-aa06-e2631ac5de74
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// THE one style of every action in a detail panel's head, defined once: a text action and an
// icon action alike have no background and no border at rest, the same height and type size.
// Hovering tints it and keyboard focus rings it -- those are states, not a background. An
// action keeps a colour it is given (a delete stays red, as text or icon only); what it may not
// do is choose its own variant or size, which a guard refuses (panelHeadGuard.test.ts). The
// shared Save and Cancel take this style in a head and draw the filled Save and bordered Cancel
// at a form's foot and in a dialog (`head` of SaveButton and CancelButton).
//
// It is Mantine's `subtle`: transparent at rest with a visible hover, where `transparent` has
// none. Applied as the default props of every Button and ActionIcon rendered inside it, so a
// page's head needs no styling of its own.

import type { ReactNode } from "react";
import {
  ActionIcon,
  Button,
  MantineThemeProvider,
  createTheme,
  defaultVariantColorsResolver,
  type VariantColorsResolver,
} from "@mantine/core";

/** The height of a head action, text or icon, in pixels. */
export const HEAD_ACTION_PX = 26;

// An action given a colour (a delete's red) shows it as its text or icon: the theme's readable
// shade of that colour, where `subtle` alone gives a tint too pale to read as the colour.
const headColors: VariantColorsResolver = (input) => {
  const resolved = defaultVariantColorsResolver(input);
  const named = typeof input.color === "string" && input.color in input.theme.colors;
  if (input.variant !== "subtle" || !named) return resolved;
  return { ...resolved, color: `var(--mantine-color-${input.color}-text)` };
};

const HEAD_ACTIONS = createTheme({
  variantColorResolver: headColors,
  components: {
    Button: Button.extend({ defaultProps: { variant: "subtle", size: "compact-sm" } }),
    ActionIcon: ActionIcon.extend({ defaultProps: { variant: "subtle", size: HEAD_ACTION_PX } }),
  },
});

export function PanelHeadStyle({ children }: { children: ReactNode }) {
  return <MantineThemeProvider theme={HEAD_ACTIONS}>{children}</MantineThemeProvider>;
}
