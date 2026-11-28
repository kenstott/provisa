// Copyright (c) 2026 Kenneth Stott
// Canary: 21ad74ad-a990-474c-9692-e596c81cd611
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The open state of a card that opens on hover and stays open while the pointer is over its target
// or the card itself, so the card's text can be read and its link followed (PageHelp, a page's (?);
// PageCommand, a page command's description). Leaving both closes it after a short delay. A pinned
// card stays open after the pointer leaves, until Escape, a click outside, or `set(false)`.

import { useCallback, useEffect, useRef, useState } from "react";

/** Long enough to cross the gap between the target and the card, short enough not to be waited for. */
export const STAY_OPEN_CLOSE_DELAY_MS = 150;

export function useStayOpen() {
  const [opened, setOpened] = useState(false);
  const pinned = useRef(false);
  const timer = useRef<number | null>(null);
  const cancel = useCallback(() => {
    if (timer.current !== null) window.clearTimeout(timer.current);
    timer.current = null;
  }, []);
  useEffect(() => cancel, [cancel]);
  /** Opens it, at once or after `delay` ms. */
  const open = (delay = 0) => {
    cancel();
    if (delay === 0) setOpened(true);
    else timer.current = window.setTimeout(() => setOpened(true), delay);
  };
  /** Closes it after the delay, unless it is pinned or the pointer comes back first. */
  const closeSoon = () => {
    cancel();
    if (pinned.current) return;
    timer.current = window.setTimeout(() => setOpened(false), STAY_OPEN_CLOSE_DELAY_MS);
  };
  /** Opens it pinned, or closes and unpins it (Escape and click-outside arrive here with false). */
  const set = (next: boolean) => {
    cancel();
    pinned.current = next;
    setOpened(next);
  };
  return { opened, pinned, open, closeSoon, set };
}
