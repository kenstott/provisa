// Copyright (c) 2026 Kenneth Stott
// Canary: 67c72cb9-1bf6-4eb6-884e-25e9a8a323d7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-788: combining files into one table is its own action of Register Table. On a files source,
// two or more files picked, or a glob typed, offer "Combine into one table". Chosen, the server
// is asked what the glob (as typed, or derived from the files picked) reads: the files, the columns
// they share, the files that differ, and the files a derived glob adds to those picked. The new
// table's name is its own; nothing existing stands for it.

import { useEffect, useState } from "react";
import type { TFunction } from "i18next";
import { useFileGlobPreview, type FileGlobPreview } from "./useFileGlobPreview";

export type FilesMode = "perFile" | "combine";

/** What one table over many files is registered as. */
export interface Combined {
  name: string;
  glob: string;
  sourceFileColumn: string;
  /** A listed table whose columns the combined table takes. */
  reference: string;
}

export function useCombineFiles({
  sourceId,
  filesSource,
  offered,
  picked,
}: {
  sourceId: string;
  /** Whether the source serves files (a files, csv or parquet source). */
  filesSource: boolean;
  /** The names of the tables the source lists. */
  offered: string[];
  /** The tables picked in the Tables field. */
  picked: string[];
}) {
  const [typedGlob, setTypedGlob] = useState("");
  const [mode, setMode] = useState<FilesMode>("perFile");
  const [name, setName] = useState("");
  const [sourceFileColumn, setSourceFileColumn] = useState("");
  const [includeExtra, setIncludeExtra] = useState(false);
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- form cascade reset: what was typed for one source is not another's
    setTypedGlob("");
    setMode("perFile");
    setName("");
    setSourceFileColumn("");
  }, [sourceId]);

  const glob = filesSource ? typedGlob.trim() : "";
  const offeredChoice = filesSource && (picked.length >= 2 || glob !== "");
  const combining = offeredChoice && mode === "combine";
  // A typed glob is asked at once (a glob matching nothing says so before anything is chosen);
  // the files picked are asked only once they are to be combined.
  const asked = glob !== "" ? offered : combining ? picked : [];
  const { preview, checking, error } = useFileGlobPreview(
    glob !== "" || combining ? sourceId : null,
    glob || null,
    asked,
  );
  // Another glob, or other files, are accepted afresh.
  const answeredGlob = preview?.glob ?? "";
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- the extra files accepted were those of the glob answered before
    setIncludeExtra(false);
  }, [answeredGlob]);

  return {
    typedGlob,
    setTypedGlob,
    mode,
    setMode,
    offeredChoice,
    combining,
    preview,
    checking,
    error,
    name,
    setName,
    sourceFileColumn,
    setSourceFileColumn,
    includeExtra,
    setIncludeExtra,
    /** Nothing a glob matches, or files whose columns differ, cannot be one table. */
    registrable: preview !== null && !checking && registrableOf(preview),
    /** Why the combined table cannot be registered yet, or the registration to send. */
    combined: (t: TFunction): { problem: string } | { combined: Combined } =>
      combinedOf(t, preview, { name, sourceFileColumn, includeExtra }),
  };
}

/** A preview whose files can be one table: some matched, all of them sharing their columns. */
export function registrableOf(preview: FileGlobPreview): boolean {
  return preview.files.length > 0 && preview.differing.length === 0 && !!preview.referenceTable;
}

/** What the form registers of a preview; the problem the person is told when it cannot be yet. */
export function combinedOf(
  t: TFunction,
  preview: FileGlobPreview | null,
  given: { name: string; sourceFileColumn: string; includeExtra: boolean },
): { problem: string } | { combined: Combined } {
  if (!given.name.trim()) return { problem: t("combineFiles.nameRequired") };
  if (preview === null || !registrableOf(preview))
    return { problem: t("combineFiles.notRegistrable") };
  if (preview.extraFiles.length > 0 && !given.includeExtra)
    return { problem: t("combineFiles.extraUnconfirmed") };
  return {
    combined: {
      name: given.name.trim(),
      glob: preview.glob,
      sourceFileColumn: given.sourceFileColumn.trim(),
      reference: preview.referenceTable as string,
    },
  };
}
