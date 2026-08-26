// Copyright (c) 2026 Kenneth Stott
// Canary: ab0f697b-d831-4ffc-8b32-4426767d71d2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useComputedColorScheme } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { CheckoutAppearance } from "./billing";

/**
 * The appearance every checkout is opened with: the scheme and language this app is being read in.
 *
 * `useComputedColorScheme` rather than `useMantineColorScheme`, because the second answers "auto"
 * for anyone who never touched the toggle and the checkout has to be told light or dark. The
 * resolved value is also what `theme/tokens.css` is painting the page behind the overlay with, so
 * the two agree by construction.
 */
export function useCheckoutAppearance(): CheckoutAppearance {
  const scheme = useComputedColorScheme();
  const { i18n } = useTranslation();
  // The tag as the browser wrote it, region and all. The server owns the mapping onto the store's
  // own locale list, which is a shorter list than the product's languages.
  return { scheme, locale: i18n.language };
}
