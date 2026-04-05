// Copyright (c) 2026 Kenneth Stott
// Canary: e160ce9d-5c95-44f8-8f27-ea54fab49af0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import "@testing-library/jest-dom/vitest";

// @react-aria/interactions patches HTMLElement.prototype.focus at module-init time.
// In jsdom vm contexts the property can be accessor-only; re-declare as a plain
// writable value so the assignment doesn't throw in strict-mode ES module scope.
if (typeof HTMLElement !== 'undefined') {
  const desc = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'focus')
  if (desc) {
    const fn = 'value' in desc ? desc.value : desc.get?.call(HTMLElement.prototype)
    Object.defineProperty(HTMLElement.prototype, 'focus', {
      configurable: true,
      writable: true,
      value: typeof fn === 'function' ? fn : () => {},
    })
  }
}
