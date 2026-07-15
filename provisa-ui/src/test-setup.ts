// Copyright (c) 2026 Kenneth Stott
// Canary: e160ce9d-5c95-44f8-8f27-ea54fab49af0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import '@testing-library/jest-dom/vitest';
import { afterEach, vi } from 'vitest';
import { cleanup } from '@testing-library/react';

// @testing-library/react auto-cleanup doesn't reliably fire between tests in
// vitest's vmThreads pool.  Register it explicitly so DOM state never leaks.
afterEach(cleanup);

// Mantine components (ScrollArea, Select, transitions) depend on browser APIs
// jsdom does not implement. Provide the standard polyfills once, globally, so
// every component test can render Mantine without per-test boilerplate.
if (typeof window !== 'undefined' && !window.matchMedia) {
  window.matchMedia = (query: string) =>
    ({
      matches: false,
      media: query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    }) as MediaQueryList;
}
class ResizeObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
}
globalThis.ResizeObserver = globalThis.ResizeObserver ?? (ResizeObserverStub as never);
if (typeof Element !== 'undefined' && !Element.prototype.scrollIntoView) {
  Element.prototype.scrollIntoView = vi.fn();
}

// @react-aria/interactions patches HTMLElement.prototype.focus at module-init time.
// In jsdom vm contexts the property can be accessor-only; re-declare as a plain
// writable value so the assignment doesn't throw in strict-mode ES module scope.
if (typeof HTMLElement !== 'undefined') {
  const desc = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'focus');
  if (desc) {
    const fn = 'value' in desc ? desc.value : desc.get?.call(HTMLElement.prototype);
    Object.defineProperty(HTMLElement.prototype, 'focus', {
      configurable: true,
      writable: true,
      value: typeof fn === 'function' ? fn : () => {},
    });
  }
}
