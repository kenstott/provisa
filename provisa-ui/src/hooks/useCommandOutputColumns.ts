// Copyright (c) 2026 Kenneth Stott
// Canary: 2d9a6f18-7c34-4e51-b8a2-0f5e3c7d9b46
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-25): every command's declared output columns, the objects the Tags and
// Glossary pages put a tag or a term on.

import { useEffect, useState } from "react";
import { fetchActions } from "../api/actions";

export interface CommandColumnRef {
  command: string;
  column: string;
}

/** Every declared output column of every command, in command then column order. */
export function useCommandOutputColumns(): {
  columns: CommandColumnRef[] | null;
  error: string | null;
} {
  const [columns, setColumns] = useState<CommandColumnRef[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let live = true;
    fetchActions().then(
      ({ functions }) => {
        if (!live) return;
        const rows = functions.flatMap((f) =>
          (f.outputColumns ?? [])
            .filter((c) => c.name.trim() !== "")
            .map((c) => ({ command: f.name, column: c.name })),
        );
        rows.sort((a, b) => a.command.localeCompare(b.command) || a.column.localeCompare(b.column));
        setColumns(rows);
      },
      (e: Error) => live && setError(e.message),
    );
    return () => {
      live = false;
    };
  }, []);
  return { columns, error };
}
