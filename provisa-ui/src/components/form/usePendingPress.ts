// Copyright (c) 2026 Kenneth Stott
// Canary: fdd8aea4-ac8e-4a6a-a011-4f70beb4df81
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";

/**
 * The busy state of a press whose work is a promise: Save's, and a detail panel's Delete
 * (`PanelDelete`). `press` runs `work`; from then until the promise settles `busy` is true, and
 * the control shows a spinner in place of its icon and is disabled. A press whose work is not a
 * promise is never busy. `settled` hears how the promise came out, after `busy` returns to false.
 */
export function usePendingPress<T>(
  work: (() => unknown) | undefined,
  settled?: (outcome: { ok: true; value: T } | { ok: false; error: unknown }) => void,
) {
  const [busy, setBusy] = useState(false);
  const press = () => {
    const result = work?.();
    if (result instanceof Promise) {
      setBusy(true);
      // Settled either way: a refused press returns the control to rest, and the form shows why.
      void result.then(
        (value: T) => {
          setBusy(false);
          settled?.({ ok: true, value });
        },
        (error: unknown) => {
          setBusy(false);
          settled?.({ ok: false, error });
        },
      );
    }
  };
  return { busy, press };
}
