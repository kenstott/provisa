// Copyright (c) 2026 Kenneth Stott
// Canary: 5c8e1b47-0a29-4f63-9d1e-2b7f4c6a8e90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234: one table or several of a schema picked in the standard multi-select and registered in
// one action, each with the defaults its registration alone would take. Discovery decides, table
// by table, whether a table needs input from the person: one that does is offered, cannot be
// picked, says why, and is registered on its own. A table registered already is not offered.

import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  fetchTableUniqueConstraints,
  type ColumnMetadata,
  type TableMetadata,
} from "../../api/admin";
import { useReadColumns } from "../../hooks/useAvailableColumnsMetadataLazy";
import { useRegisterTables, type TableRegistrationOutcome } from "../../hooks/useTableQueries";
import { draftFromRegistration, type Registration } from "./draftTable";
import { registrationInput } from "./useCreateTable";
import {
  defaultWatermark,
  discoveredColumns,
  inputNeeded,
  registrationOf,
  type InputNeeded,
} from "./registrationDefaults";
import { registeredColumns } from "./registrationColumns";
import type { Combined } from "./useCombineFiles";

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
  /** REQ-1907: each table of the source needs its own Cache TTL to be registered (it lands, and
   * the source states none for its ttl signal): none is registered with the others. */
  needsCacheTtl?: boolean;
  /** Every table that needs no input starts picked (a source just saved); else none does. */
  pickReady?: boolean;
}

/** What keeps a listed table from being registered with the others; null when nothing does. */
export function blockedBy(
  reading: Reading | undefined,
  needsCacheTtl: boolean,
): InputNeeded | null {
  if (reading?.state !== "read") return null;
  return reading.needs ?? (needsCacheTtl ? { kind: "cacheTtl" } : null);
}

/** The picker's state: what each table needs, which are ticked, and the one action. */
export function useManyTables(choice: ManyTablesChoice) {
  const { t } = useTranslation();
  const readColumns = useReadColumns();
  const { registerTables } = useRegisterTables();
  const { sourceId, schemaName, offered, isRegistered } = choice;
  const [readings, setReadings] = useState<Record<string, Reading>>({});
  // What the person picked; null until they pick, while `pickReady` decides what starts picked.
  const [picked, setPicked] = useState<string[] | null>(null);
  const [registering, setRegistering] = useState(false);
  // The table being opened to be registered on its own, while its defaults are gathered.
  const [opening, setOpening] = useState<string | null>(null);
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
    setPicked(null);
    const readNext = async (): Promise<void> => {
      const tbl = queue.shift();
      if (!tbl || !live) return;
      let reading: Reading;
      try {
        const columns = await readColumns(sourceId, schemaName, tbl.name);
        reading = { state: "read", columns, needs: inputNeeded(tbl, columns, choice.sourceType) };
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
  }, [readColumns, sourceId, schemaName, pickableKey, choice.sourceType]);

  const needsCacheTtl = choice.needsCacheTtl === true;
  const ready = (name: string) =>
    readings[name]?.state === "read" && blockedBy(readings[name], needsCacheTtl) === null;
  const ticked = pickable
    .map((tbl) => tbl.name)
    .filter((n) => ready(n) && (picked === null ? choice.pickReady === true : picked.includes(n)));
  const setTicked: (names: string[]) => void = setPicked;

  /** One read table as a registration with the defaults it takes when registered alone; or,
   * `combined`, one table of its own name over the files of a glob, with this table's columns. */
  const registrationOfTable = async (name: string, combined?: Combined): Promise<Registration> => {
    const tbl: TableMetadata | undefined = offered.find((o) => o.name === name);
    const reading = readings[name];
    // A combined table's columns are a listed file's; one registered already was not read here.
    const read: Extract<Reading, { state: "read" }> =
      reading?.state === "read"
        ? reading
        : {
            state: "read",
            columns: await readColumns(sourceId, schemaName, name),
            needs: null,
          };
    const columns = discoveredColumns(read.columns, choice.roleIds, choice.sourceType);
    const tableName = combined?.name ?? name;
    const alias = await choice.suggestTableAlias(tableName, choice.domainId, sourceId);
    const uniqueConstraints = await fetchTableUniqueConstraints(sourceId, schemaName, name);
    return registrationOf({
      sourceId,
      domainId: choice.domainId,
      regionInput: choice.regionInput,
      schemaName,
      tableName,
      alias,
      description: combined ? "" : (tbl?.comment ?? ""),
      watermarkColumn: defaultWatermark(columns, choice.sourceType),
      // A collection's vectors go with it to the panel, where each names its model; one left
      // without a model is not registered (registrationInput).
      columns: registeredColumns(
        read.needs?.kind === "vectors" ? columns.map((c) => ({ ...c, selected: true })) : columns,
        null,
      ),
      uniqueConstraints,
      pagingKind: tbl?.pagingKind ?? null,
      pagination: tbl?.pagination ?? null,
      pagingCeilingRows: tbl?.pagingCeilingRows ?? null,
      extra: {
        discover: false,
        // REQ-788: the files the one table reads, and the column naming each row's file.
        ...(combined
          ? {
              fileGlob: combined.glob,
              sourceFileColumn: combined.sourceFileColumn || undefined,
            }
          : {}),
      },
    });
  };

  /** Each ticked table registered with its defaults, in one action, or the files of `combined`
   * as one table; what each answered. */
  const register = async (combined?: Combined): Promise<TableRegistrationOutcome[]> => {
    setRegistering(true);
    try {
      // One table after another: the alias each is suggested is one lazy query, and a second
      // call while one is in flight aborts it.
      const inputs: Record<string, unknown>[] = [];
      for (const name of combined ? [combined.reference] : ticked) {
        const registration = await registrationOfTable(name, combined);
        // As the editor's Save sends a table registered alone (useCreateTable).
        const draft = draftFromRegistration(registration);
        inputs.push(registrationInput(draft, registration, draft.cacheTtl ?? null));
      }
      return await registerTables(inputs);
    } finally {
      setRegistering(false);
    }
  };

  /** The one action: each ticked table registered, or (`combined`) the files combined into one
   * table. Some registered: `onRegistered` hears what each answered. None registered: each table
   * is named with why, as the form's problem. */
  const registerAll = async (
    domainId: string,
    onRegistered: (outcomes: TableRegistrationOutcome[]) => void,
    setError: (e: string | null) => void,
    combined?: Combined,
  ) => {
    setError(null);
    if (!domainId) {
      setError(t("registerManyTables.domainRequired"));
      return;
    }
    try {
      const outcomes = await register(combined);
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

  /**
   * REQ-2234: a table that is not registered with the others -- it needs input, or a Cache TTL
   * of its own -- is registered on its own: `open` is handed it as it will be registered, with
   * the defaults discovery gives (an untyped column left untyped), and opens its detail panel in
   * edit (TablesPage's startCreating), where what is missing is given. Its Save registers it, as
   * a draft as every table registered through the admin is, with all of it.
   */
  const registerAsDraft = async (
    tableName: string,
    open: (registration: Registration) => void,
    setError: (e: string | null) => void,
  ): Promise<void> => {
    setError(null);
    if (!choice.domainId) {
      setError(t("registerManyTables.domainRequired"));
      return;
    }
    if (readings[tableName]?.state !== "read") return;
    setOpening(tableName);
    try {
      const registration = await registrationOfTable(tableName);
      // An OpenAPI table with no response schema: its columns are declared in the panel.
      const declared = blockedBy(readings[tableName], needsCacheTtl)?.kind === "declared";
      open(declared ? { ...registration, declaresColumns: true } : registration);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setOpening(null);
    }
  };

  return {
    readings,
    ticked,
    setTicked,
    registerAll,
    registering,
    needsCacheTtl,
    registerAsDraft,
    opening,
  };
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
