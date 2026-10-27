// Copyright (c) 2026 Kenneth Stott
// Canary: 4d3b6856-be1a-42d3-82d2-5ee725be0812
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * Resolve when an element matching `selector` is present in the DOM. Rejects
 * after `timeoutMs` so a genuinely missing anchor surfaces as a real error
 * rather than hanging the tour. No silent fallback — a failed wait aborts.
 *
 * The window is generous because each step may navigate to a lazily-loaded
 * route whose chunk is compiled/fetched on first visit; a short cap would abort
 * the whole tour on a cold chunk (observed killing it at the NL step in dev).
 * {@link ANCHOR_WAIT_MS} is sized for a machine under heavy load, where a page's
 * own render and its data round-trip both stretch; the wait is now visible while
 * it runs and recoverable when it expires, so a long one costs the visitor
 * nothing but a spinner.
 */
export const ANCHOR_WAIT_MS = 45000;

export function waitForElement(
  selector: string,
  timeoutMs = ANCHOR_WAIT_MS,
  signal?: AbortSignal,
): Promise<HTMLElement> {
  const existing = document.querySelector<HTMLElement>(selector);
  if (existing) return Promise.resolve(existing);
  // An abort raised before this wait began never fires its event again: without this the observer
  // would run its whole window against a document the provider has already left.
  if (signal?.aborted) return Promise.reject(new Error(`Tour: wait for ${selector} abandoned`));
  return new Promise((resolve, reject) => {
    const stop = () => {
      observer.disconnect();
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    };
    const observer = new MutationObserver(() => {
      const el = document.querySelector<HTMLElement>(selector);
      if (el) {
        stop();
        resolve(el);
      }
    });
    const timer = setTimeout(() => {
      stop();
      reject(new Error(`Tour: element not found within ${timeoutMs}ms: ${selector}`));
    }, timeoutMs);
    // Without this the observer outlives the step that started it — up to ANCHOR_WAIT_MS of
    // observing a document the provider has already left.
    const onAbort = () => {
      stop();
      reject(new Error(`Tour: wait for ${selector} abandoned`));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    // Attributes too: a page marks a hand-off answered on an element already there (REQ-2010).
    observer.observe(document.body, { childList: true, subtree: true, attributes: true });
  });
}

/**
 * REQ-2052: watch for what a page shows when a step's object does not exist (a table never
 * profiled). Once it appears, `said` is told its text and `abort` stops the step's waits, so the
 * step is stuck at once, naming why, instead of waiting out an anchor nothing will draw.
 */
export function watchAbsent(
  selector: string,
  timeoutMs: number | undefined,
  abort: AbortController,
  said: (reason: string) => void,
): void {
  waitForElement(selector, timeoutMs, abort.signal).then(
    (shown) => {
      said(shown.textContent ?? "");
      abort.abort();
    },
    // Its wait ends with the step (left, or past its window): the object was there.
    () => {},
  );
}
