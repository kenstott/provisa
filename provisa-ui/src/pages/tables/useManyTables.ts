// Copyright (c) 2026 Kenneth Stott
// Canary: 5c8e1b47-0a29-4f63-9d1e-2b7f4c6a8e90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234: several tables of a schema picked at once and registered in one action, each with the
// defaults its registration alone would take. Discovery decides, table by table, whether a table
// needs input from the person: one that does is listed, unticked, saying why, and is registered
// on its own. A table registered already is listed, and cannot be picked again.

import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  fetchTableUniqueConstraints,
  type ColumnMetadata,
  type TableMetadata,
} from "../../api/admin";
import { useReadColumns } from "../../hooks/useAvailableColumnsMetadataLazy";
import { useRegisterTables, type TableRegistrationOutcome } from "../../hooks/useTableQueries";
import { draftFromRegistration } from "./draftTable";
import { registrationInput } from "./useCreateTable";
import {
  defaultWatermark,
  discoveredColumns,
  inputNeeded,
  registrationOf,
  type InputNeeded,
} from "./registrationDefaults";
import { registeredColumns } from "./registrationColumns";

/** What discovery said of one listed table. */
export type Reading =
  | { state: "reading" }
  | { state: "read"; columns: ColumnMetadata[]; needs: InputNeeded | null }
  | { state: "failed"; error: string };

export interface ManyTablesChoice {
  sourceId: string;
  sourceType: string;
  domainId: string;
  schemaName: string;
  /** Only where regions exist: `{ region }`. */
  regionInput: Record<string, unknown>;
  roleIds: string[];
  /** The tables the schema lists. */
  offered: TableMetadata[];
  isRegistered: (table: { name: string }) => boolean;
  suggestTableAlias: (tableName: string, domainId: string, sourceId: string) => Promise<string>;
}

/** The picker's state: what each table needs, which are ticked, and the one action. */
export function useManyTables(choice: ManyTablesChoice) {
  const { t } = useTranslation();
  const readColumns = useReadColumns();
  const { registerTables } = useRegisterTables();
  const { sourceId, schemaName, offered, isRegistered } = choice;
  const [readings, setReadings] = useState<Record<string, Reading>>({});
  // A table discovery says needs no input is ticked once it is read; the person may untick it.
  const [unticked, setUnticked] = useState<Set<string>>(new Set());
  const [registering, setRegistering] = useState(false);
  const pickable = offered.filter((tbl) => !isRegistered(tbl));
  // The tables are read again only when which tables are listed changes, not at each render.
  const pickableKey = pickable.map((tbl) => tbl.name).join("\n");
  const listed = useRef(pickable);
  useEffect(() => {
    listed.current = pickable;
  });

  useEffect(() => {
    let live = true;
    const queue = listed.current.filter((tbl) => pickableKey.split("\n").includes(tbl.name));
    setReadings(Object.fromEntries(queue.map((tbl) => [tbl.name, { state: "reading" }])));
    setUnticked(new Set());
    const readNext = async (): Promise<void> => {
      const tbl = queue.shift();
      if (!tbl || !live) return;
      let reading: Reading;
      try {
        const columns = await readColumns(sourceId, schemaName, tbl.name);
        reading = { state: "read", columns, needs: inputNeeded(tbl, columns) };
      } catch (e) {
        reading = { state: "failed", error: e instanceof Error ? e.message : String(e) };
      }
      if (!live) return;
      setReadings((prev) => ({ ...prev, [tbl.name]: reading }));
      await readNext();
    };
    // One table after another: a schema of hundreds of tables is not read in one burst.
    void readNext();
    return () => {
      live = false;
    };
  }, [readColumns, sourceId, schemaName, pickableKey]);

  const ready = (name: string) => {
    const r = readings[name];
    return r?.state === "read" && r.needs === null;
  };
  const ticked = pickable.map((tbl) => tbl.name).filter((n) => ready(n) && !unticked.has(n));
  const setTicked = (names: string[]) =>
    setUnticked(new Set(pickable.map((tbl) => tbl.name).filter((n) => !names.includes(n))));

  /** Each ticked table registered with its defaults, in one action; what each answered. */
  const register = async (): Promise<TableRegistrationOutcome[]> => {
    setRegistering(true);
    try {
      const inputs = await Promise.all(
        ticked.map(async (name) => {
          const tbl = offered.find((o) => o.name === name) as TableMetadata;
          const read = readings[name] as Extract<Reading, { state: "read" }>;
          const columns = discoveredColumns(read.columns, choice.roleIds, choice.sourceType);
          const [alias, uniqueConstraints] = await Promise.all([
            choice.suggestTableAlias(name, choice.domainId, sourceId),
            fetchTableUniqueConstraints(sourceId, schemaName, name),
          ]);
          const registration = registrationOf({
            sourceId,
            domainId: choice.domainId,
            regionInput: choice.regionInput,
            schemaName,
            tableName: name,
            alias,
            description: tbl.comment ?? "",
            watermarkColumn: defaultWatermark(columns, choice.sourceType),
            columns: registeredColumns(columns, null),
            uniqueConstraints,
            pagingKind: tbl.pagingKind ?? null,
            pagination: tbl.pagination ?? null,
            pagingCeilingRows: tbl.pagingCeilingRows ?? null,
            extra: { discover: false },
          });
          // As the editor's Save sends a table registered alone (useCreateTable).
          const draft = draftFromRegistration(registration);
          return registrationInput(draft, registration, draft.cacheTtl ?? null);
        }),
      );
      return await registerTables(inputs);
    } finally {
      setRegistering(false);
    }
  };

  /** The one action: each ticked table registered. Some registered: `onRegistered` hears what
   * each answered. None registered: each table is named with why, as the form's problem. */
  const registerAll = async (
    domainId: string,
    onRegistered: (outcomes: TableRegistrationOutcome[]) => void,
    setError: (e: string | null) => void,
  ) => {
    setError(null);
    if (!domainId) {
      setError(t("registerTableForm.errorRequiredFields"));
      return;
    }
    try {
      const outcomes = await register();
      if (outcomes.some((o) => o.result.success)) {
        onRegistered(outcomes);
        return;
      }
      setError(
        outcomes
          .map((o) =>
            t("registerManyTables.refused", { table: o.tableName, message: o.result.message }),
          )
          .join("\n"),
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  return { readings, ticked, setTicked, registerAll, registering };
}

/** What a registration of several tables came to: the ids registered, and the tables refused. */
export interface Registered {
  tableIds: number[];
  refused: TableRegistrationOutcome[];
}

/** The ids registered and the outcomes refused, from what each table's registration answered. */
export function registeredOf(outcomes: TableRegistrationOutcome[]): Registered {
  return {
    tableIds: outcomes
      .map((o) => o.result.params?.id)
      .filter((id): id is number => typeof id === "number"),
    refused: outcomes.filter((o) => !o.result.success),
  };
}
