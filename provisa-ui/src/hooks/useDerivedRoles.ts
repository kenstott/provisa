// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2078, REQ-2079: the org's derived roles, read with their values seen and their uses.

import { useCallback, useEffect, useState } from "react";
import { fetchDerivedRoles, type DerivedRoles } from "../api/admin";

/** Between an attribute and its value in a value role's id (provisa/auth/derived_roles.py). */
export const SEPARATOR = "__";

/** A derived role picked in the list: the parent (value null) or one of its value roles. */
export interface DerivedPick {
  attribute: string;
  value: string | null;
}

const said = (e: unknown) => (e instanceof Error ? e.message : String(e));

/** The org's derived roles, and a reload for after a change. */
export function useDerivedRoles() {
  const [derived, setDerived] = useState<DerivedRoles | null>(null);
  const [problem, setProblem] = useState<string | null>(null);
  const reload = useCallback(async () => {
    try {
      setDerived(await fetchDerivedRoles());
      setProblem(null);
    } catch (e) {
      setProblem(said(e));
    }
  }, []);
  useEffect(() => {
    let live = true;
    fetchDerivedRoles().then(
      (d) => live && setDerived(d),
      (e: unknown) => live && setProblem(said(e)),
    );
    return () => {
      live = false;
    };
  }, []);
  return { derived, problem, reload };
}
