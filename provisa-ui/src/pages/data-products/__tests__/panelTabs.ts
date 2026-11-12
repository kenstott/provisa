// Copyright (c) 2026 Kenneth Stott
// Canary: b5f2d8a1-3c6e-4a97-8e10-d4c7a9f2b063
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Opens one of a data product panel's tabs: only the tab in view is mounted.

import { fireEvent, screen } from "@testing-library/react";
import type { ProductTab } from "../tabs/productTabs";

export function openTab(tab: ProductTab, productId: string) {
  fireEvent.click(screen.getByTestId(`data-product-tab-${tab}-${productId}`));
}
