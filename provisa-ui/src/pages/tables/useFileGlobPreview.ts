// Copyright (c) 2026 Kenneth Stott
// Canary: 0b6e4d93-2a7c-4f15-8e3d-c9a1f6b7e204
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-788: what one table over many files of a files source would read, asked of the server
// before anything is registered: the glob (as typed, or derived from the files picked), the files
// it matches, the columns they share and the files whose columns differ.

import { useQuery } from "@apollo/client/react";
import { FileGlobPreview as FILE_GLOB_PREVIEW } from "../../hooks/admin.graphql";

export interface FilesDiffer {
  file: string;
  missing: string[];
  extra: string[];
  error: string | null;
}

export interface FileGlobPreview {
  glob: string;
  files: { file: string; table: string }[];
  sharedColumns: string[];
  differing: FilesDiffer[];
  /** Files a derived glob reads besides those picked: none when the glob is exact. */
  extraFiles: string[];
  /** Picked tables no file of the source was found for. */
  unmatchedTables: string[];
  /** A listed table whose columns the combined table takes. */
  referenceTable: string | null;
}

/** The preview of a typed `glob` (its files named by the `tables` listed), or of the glob
 * derived from the `tables` picked; nothing is asked while `sourceId` is null. */
export function useFileGlobPreview(
  sourceId: string | null,
  glob: string | null,
  tables: string[],
): { preview: FileGlobPreview | null; checking: boolean; error: string | null } {
  const { data, loading, error } = useQuery<{ fileGlobPreview: FileGlobPreview }>(
    FILE_GLOB_PREVIEW,
    {
      variables: { sourceId, glob, tables },
      skip: !sourceId,
      fetchPolicy: "no-cache",
    },
  );
  return {
    preview: data?.fileGlobPreview ?? null,
    checking: loading,
    error: error?.message ?? null,
  };
}
