// Copyright (c) 2026 Kenneth Stott
// Canary: 8c2f4a61-3d7e-4b95-a0c8-5e1b9f7d2a46
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback } from "react";
import { useLazyQuery } from "@apollo/client/react";
import { OwnerField } from "./admin.graphql";

/** REQ-2219 (amended 2026-11-23): what an object's Owner or Steward field names. */
export interface OwnerFieldValue {
  people: string[];
  roles: string[];
}

/** Reads what an object's Owner (or a domain's Steward) field names, for the form that edits it. */
export function useOwnerField() {
  const [run] = useLazyQuery<{ ownerField: OwnerFieldValue }>(OwnerField, {
    fetchPolicy: "network-only",
  });
  return useCallback(
    async (
      kind: "data_product" | "domain" | "relationship",
      objectId: string,
    ): Promise<OwnerFieldValue> => {
      const { data, error } = await run({ variables: { kind, objectId } });
      if (error) throw error;
      if (!data) throw new Error(`ownerField returned no data for ${kind} ${objectId}`);
      return data.ownerField;
    },
    [run],
  );
}
