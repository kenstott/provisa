// Copyright (c) 2026 Kenneth Stott
// Canary: 8da6e434-6cf4-4b3f-9092-18cb2e837c3d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: the kinds of entitlement list a role grant carries, and the names a list is picked
// from, as the server defines them (GET /admin/entitlements/kinds, from the definition its write
// checks). Nothing here lists a kind of its own.

import { useEffect, useState } from "react";
import { fetchKinds, type EntitlementKinds, type KindInfo } from "../../api/entitlements";

/** Everything of the kind, including objects added later. */
export const WILDCARD = "*";

/** The server's kinds and names tree, read once for the component that mounts this; null until
 *  they arrive. A failure is passed to `onError`. */
export function useEntitlementKinds(onError: (e: unknown) => void): EntitlementKinds | null {
  const [kinds, setKinds] = useState<EntitlementKinds | null>(null);
  useEffect(() => {
    let live = true;
    fetchKinds()
      .then((k) => live && setKinds(k))
      .catch((e) => live && onError(e));
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- onError may be re-created each render, so depending on it refetches in a loop; the read is once per mount
  }, []);
  return kinds;
}

/** The kind named `kind` among the server's, or undefined when the server has no such kind. */
export function kindInfo(kinds: EntitlementKinds | null, kind: string): KindInfo | undefined {
  return kinds?.kinds.find((k) => k.kind === kind);
}
