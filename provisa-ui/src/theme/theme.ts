// Copyright (c) 2026 Kenneth Stott
// Canary: 18871956-e207-4395-9014-09d521ece58a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { createTheme, type MantineColorsTuple } from "@mantine/core";

// Brand indigo scale. Index 5 (#6366f1) is the historical --primary token;
// index 4 (#818cf8) is --primary-hover. Filled components use `primaryShade`
// index 6 (#4f46e5) so white label text clears WCAG 2.1 AA (6.28:1) — the
// brand-500 fill (#6366f1) only reaches 4.47:1, which fails AA for body text
// (REQ-1013).
const brand: MantineColorsTuple = [
  "#eef2ff",
  "#e0e7ff",
  "#c7d2fe",
  "#a5b4fc",
  "#818cf8",
  "#6366f1",
  "#4f46e5",
  "#4338ca",
  "#3730a3",
  "#312e81",
];

export const theme = createTheme({
  primaryColor: "brand",
  // Index 6 in both schemes keeps filled-control label contrast >= AA.
  primaryShade: { light: 6, dark: 6 },
  colors: { brand },
  fontFamily:
    '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
  headings: { fontWeight: "600" },
  defaultRadius: "md",
  cursorType: "pointer",
  // Respect the OS reduced-motion setting for all Mantine transitions (REQ-1013).
  respectReducedMotion: true,
});
