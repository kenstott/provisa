// Copyright (c) 2026 Kenneth Stott
// Canary: b5f2d8a1-3c6e-4a97-8e10-d4c7a9f2b063
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Expands one of a data product page's summary rows in place, where it is not expanded already: a
// row's state is remembered, so a test asks for the state it needs.

import { fireEvent, screen } from "@testing-library/react";
import type { ProductSection } from "../productSections";

export function sectionToggle(section: ProductSection, productId: string): HTMLElement {
  return screen.getByTestId(`data-product-row-${section}-${productId}-toggle`);
}

export function openSection(section: ProductSection, productId: string) {
  const toggle = sectionToggle(section, productId);
  if (toggle.getAttribute("aria-expanded") !== "true") fireEvent.click(toggle);
}
