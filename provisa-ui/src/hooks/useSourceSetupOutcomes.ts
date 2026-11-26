// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2262: the source setup stepper's record -- what became of each step, one record per source
// on the server, shared by every user and environment. A write replaces the record and reads the
// sources again, so every panel showing the source sees it.

import { useCallback } from "react";
import { useMutation } from "@apollo/client/react";
import type { MutationResult, SourceSetupOutcomes } from "../types/admin";
import { SetSourceSetupOutcomes } from "./admin.graphql";
import { refusalError } from "../i18n/serverMessage";

/** The steps' outcomes as a write gives them: the store sets `decidedAt`. */
export type SetupOutcomesWrite = Omit<SourceSetupOutcomes, "decidedAt">;

export function useSetSourceSetupOutcomes(): (
  sourceId: string,
  outcomes: SetupOutcomesWrite,
) => Promise<void> {
  const [mutate] = useMutation<{ setSourceSetupOutcomes: MutationResult }>(
    SetSourceSetupOutcomes,
    { refetchQueries: ["SourcesQuery"], awaitRefetchQueries: true },
  );
  return useCallback(
    async (sourceId, outcomes) => {
      const { data } = await mutate({ variables: { sourceId, ...outcomes } });
      if (!data) throw new Error("setSourceSetupOutcomes answered nothing");
      if (!data.setSourceSetupOutcomes.success) throw refusalError(data.setSourceSetupOutcomes);
    },
    [mutate],
  );
}
