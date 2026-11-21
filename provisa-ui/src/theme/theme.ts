// Copyright (c) 2026 Kenneth Stott
// Canary: 18871956-e207-4395-9014-09d521ece58a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { createTheme, InputWrapper, type MantineColorsTuple } from "@mantine/core";
import { BRAND, SCALES } from "./palette";

// Every scale comes from the palette, so the graph tokens (which cannot import Mantine) and the
// components draw the same colours.
const colors: Record<string, MantineColorsTuple> = { ...SCALES, brand: BRAND };

export const theme = createTheme({
  primaryColor: "brand",
  // Index 6 in both schemes keeps filled-control label contrast >= AA.
  primaryShade: { light: 6, dark: 6 },
  colors,
  fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
  headings: { fontWeight: "600" },
  defaultRadius: "md",
  cursorType: "pointer",
  // Respect the OS reduced-motion setting for all Mantine transitions (REQ-1013).
  respectReducedMotion: true,
  components: {
    // Nothing sits between a field's label and its control: an explanation is below the control,
    // before the error. Set once here so no form can place it otherwise.
    InputWrapper: InputWrapper.extend({
      defaultProps: { inputWrapperOrder: ["label", "input", "description", "error"] },
    }),
  },
});
