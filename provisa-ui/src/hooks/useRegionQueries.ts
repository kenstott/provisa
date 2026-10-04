// Copyright (c) 2026 Kenneth Stott
// Canary: c10b066a-bb97-4383-afda-0d2d096cba87
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: the org's regions, and the setters of a table's and a source's region.

import { useState } from "react";
import { useQuery, useMutation } from "@apollo/client/react";
import type { MutationResult } from "../types/admin";
import { RegionChoices, SetTableRegion, SetSourceRegion, SetTableDraft } from "./admin.graphql";

/** REQ-1921: the org's regions and the connected one; no regions = the admin shows none. */
export interface RegionChoicesData {
  regions: string[];
  connected: string | null;
  /** REQ-1922: the choices query failed. A failure is NOT the same as a platform with no regions —
   * the selector surfaces it as an error rather than silently hiding, so a 503 never reads as
   * "this deployment has no regions". */
  error?: boolean;
}

export function useRegionChoices(): RegionChoicesData {
  const { data, error } = useQuery<{ regionChoices: RegionChoicesData }>(RegionChoices);
  if (error) return { ...NO_REGIONS, error: true };
  // Until the answer arrives no region field is shown: the same as a platform with none.
  return data?.regionChoices ?? NO_REGIONS;
}

const NO_REGIONS: RegionChoicesData = { regions: [], connected: null };

export function useSetTableRegion() {
  const [setTableRegion] = useMutation<{ setTableRegion: MutationResult }>(SetTableRegion);
  return async (tableId: number, region: string | null) => {
    const result = await setTableRegion({ variables: { tableId, region } });
    return (result.data?.setTableRegion ?? { success: false, message: "" }) as MutationResult;
  };
}

export function useSetSourceRegion() {
  const [setSourceRegion] = useMutation<{ setSourceRegion: MutationResult }>(SetSourceRegion);
  return async (sourceId: string, region: string | null) => {
    const result = await setSourceRegion({ variables: { sourceId, region } });
    return (result.data?.setSourceRegion ?? { success: false, message: "" }) as MutationResult;
  };
}

/** REQ-1921: the region a form shows — the operator's own choice, made for the current ``key``
 * (the source picked, the dialog opened), else ``start``: where a new table or view starts. */
export function useRegionChoice(
  start: string | null,
  key: unknown,
): [string | null, (region: string | null) => void] {
  const [chosen, setChosen] = useState<{ key: unknown; region: string | null } | null>(null);
  const region = chosen !== null && chosen.key === key ? chosen.region : start;
  return [region, (picked) => setChosen({ key, region: picked })];
}

/** REQ-1921: put a table or view out of service (draft: true) or release it (false). */
export function useSetTableDraft() {
  const [setTableDraft] = useMutation<{ setTableDraft: MutationResult }>(SetTableDraft);
  return async (tableId: number, draft: boolean) => {
    const result = await setTableDraft({ variables: { tableId, draft } });
    return (result.data?.setTableDraft ?? { success: false, message: "" }) as MutationResult;
  };
}
