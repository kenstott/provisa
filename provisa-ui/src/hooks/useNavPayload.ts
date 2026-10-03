// Copyright (c) 2026 Kenneth Stott
// Canary: e0d9a55c-5091-49e6-8307-e614e4d7ebc9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useRef } from "react";
import { useLocation, useNavigate } from "react-router-dom";

/**
 * Handle the value a page is handed as router state (a query to open, a method to select).
 *
 * The handler runs for every navigation that carries a payload, keyed on the history entry
 * (`location.key`), so a payload sent to the page the user is already on is handled without a
 * remount — Polly's `navigate` tool does exactly that. Once handled, the payload is removed from
 * the history entry, so a refresh does not hand it over a second time.
 *
 * A page reads its payload only through this hook: anything derived from `location.state` in
 * render would vanish when the entry is cleared, so the handler copies what the page keeps into
 * its own state.
 */
export function useNavPayload<T>(handler: (payload: T) => void): void {
  const location = useLocation();
  const navigate = useNavigate();
  const handled = useRef<string | null>(null);
  const latest = useRef(handler);

  useEffect(() => {
    latest.current = handler;
  });

  useEffect(() => {
    if (location.state == null || handled.current === location.key) return;
    handled.current = location.key;
    latest.current(location.state as T);
    navigate(`${location.pathname}${location.search}${location.hash}`, {
      replace: true,
      state: null,
    });
  }, [location.key, location.state, location.pathname, location.search, location.hash, navigate]);
}
