// Copyright (c) 2026 Kenneth Stott
// Canary: 42879282-86cb-4b99-8519-513fadf931ce
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useQuery } from "@apollo/client/react";
import { GovdataSubjectsQuery as GOVDATA_SUBJECTS_QUERY } from "./admin.graphql";
import type { GovdataSubjectCatalog } from "../pages/sources/govdataSubjects";

/** REQ-540: the subjects an AskAmerica source can be given, as the server states them.
    `catalog` is null until the server has answered; a form does not build a schema list
    without it. */
export function useGovdataSubjects(): {
  catalog: GovdataSubjectCatalog | null;
  error: Error | undefined;
} {
  const { data, error } = useQuery<{ govdataSubjects: GovdataSubjectCatalog }>(
    GOVDATA_SUBJECTS_QUERY,
  );
  return { catalog: data?.govdataSubjects ?? null, error };
}
