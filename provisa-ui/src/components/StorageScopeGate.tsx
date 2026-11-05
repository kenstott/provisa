// Copyright (c) 2026 Kenneth Stott
// Canary: 5e168162-bad0-4f91-a49a-2f34f1848bc3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Fragment, useEffect, useState, type ReactNode } from "react";
import { useAuth } from "../context/AuthContext";
import { adoptCacheScope } from "../apolloClient";
import { bindStorageScope, storageKey, type StorageScope } from "../lib/browserStorage";
import { requestDataAddresses } from "../lib/dataAddress";
import { PageLoading } from "./PageLoading";

function same(a: StorageScope | null, b: StorageScope | null): boolean {
  return a?.controlPlaneId === b?.controlPlaneId && a?.orgId === b?.orgId;
}

/**
 * REQ-2008: the pages below save their state per control plane and org (lib/browserStorage). They
 * mount once both are known, and are mounted again when either changes, so each reads the state
 * of the scope it is in. The pages of the scope being left unmount first, while it is still bound,
 * so what they save on the way out is saved in that scope; only then is the next one bound.
 */
export function StorageScopeGate({ children }: { children: ReactNode }) {
  const { controlPlaneId, activeOrgId, loading } = useAuth();
  const [shown, setShown] = useState<StorageScope | null>(null);
  // An identity refresh in flight is not a change of scope: the pages stay as they are meanwhile.
  const wanted: StorageScope | null =
    !loading && controlPlaneId !== null ? { controlPlaneId, orgId: activeOrgId } : null;
  const wantedKey = wanted === null ? null : `${wanted.controlPlaneId}:${wanted.orgId ?? ""}`;

  useEffect(() => {
    if (wanted === null || same(wanted, shown)) return;
    /* eslint-disable react-hooks/set-state-in-effect -- the scope is bound outside React, between the old pages' unmount and the new ones' mount */
    if (shown !== null) {
      setShown(null); // unmount the scope being left first
      return;
    }
    bindStorageScope(wanted);
    void adoptCacheScope(storageKey(""));
    // REQ-1916: where this scope's data calls go is asked before its pages mount and make any.
    requestDataAddresses();
    setShown(wanted);
    /* eslint-enable react-hooks/set-state-in-effect */
    // eslint-disable-next-line react-hooks/exhaustive-deps -- wantedKey is `wanted` by value
  }, [wantedKey, shown]);

  if (shown === null) return <PageLoading />;
  return <Fragment key={`${shown.controlPlaneId}:${shown.orgId ?? ""}`}>{children}</Fragment>;
}
