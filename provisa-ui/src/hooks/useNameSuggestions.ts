// Copyright (c) 2026 Kenneth Stott
// Canary: 4e8a1c27-9b35-4d6f-a0e2-71c5d93b8f40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2361, REQ-194 (NAME SUGGESTIONS COME FROM THE AUTHORITY): every name the admin suggests
// before an object is saved is the server's naming authority's, under the org's convention. The
// UI asks for it and builds none itself.

import { useCallback } from "react";
import { useLazyQuery } from "@apollo/client/react";
import { SuggestColumnAliases, SuggestReverseRelAlias } from "./admin.graphql";

export interface ReverseAliasSuggestion {
  graphqlAlias: string;
  cypherAlias: string;
}

/** The aliases suggested for the reverse of a relationship with these aliases. */
export function useSuggestReverseRelAlias(): (
  graphqlAlias: string | null,
  cypherAlias: string | null,
) => Promise<ReverseAliasSuggestion> {
  const [run] = useLazyQuery<{ suggestReverseRelAlias: ReverseAliasSuggestion }>(
    SuggestReverseRelAlias,
    { fetchPolicy: "no-cache" },
  );
  return useCallback(
    async (graphqlAlias, cypherAlias) => {
      const { data, error } = await run({ variables: { graphqlAlias, cypherAlias } });
      if (error) throw error;
      if (!data) throw new Error("The server returned no reverse alias suggestion.");
      return data.suggestReverseRelAlias;
    },
    [run],
  );
}

/** The alias suggested for each newly registered column, in order; null where the column is
 *  already named so. */
export function useSuggestColumnAliases(): (columns: string[]) => Promise<(string | null)[]> {
  const [run] = useLazyQuery<{ suggestColumnAliases: (string | null)[] }>(SuggestColumnAliases, {
    fetchPolicy: "no-cache",
  });
  return useCallback(
    async (columns) => {
      if (columns.length === 0) return [];
      const { data, error } = await run({ variables: { columns } });
      if (error) throw error;
      if (!data) throw new Error("The server returned no column alias suggestions.");
      return data.suggestColumnAliases;
    },
    [run],
  );
}
