// Copyright (c) 2026 Kenneth Stott
// Canary: 2d7b9e41-6c35-4f18-8a0e-5b3c1f9d7e62
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234: Register Table's one Tables field is the standard multi-select. A table registered
// already is not offered. A table that needs input is offered, cannot be picked, says why, and
// carries the action that registers it on its own. REQ-1986: the tables that need a parameter to
// be read (lookups) are listed apart, each marked; REQ-1954: a source's own headings are kept.

import type { TFunction } from "i18next";
import type { MultiSelectOption, OptionAction } from "../../components/MultiSelect";
import { LOOKUP_GLYPH } from "./lookups";
import { tableGroups } from "./tableGroups";
import { blockedBy, type Reading } from "./useManyTables";

interface Listed {
  name: string;
  comment: string | null;
  group?: string | null;
}

/** Why a listed table cannot be picked with the others; null when it can. */
export function whyNot(
  t: TFunction,
  reading: Reading | undefined,
  needsCacheTtl: boolean,
): string | null {
  if (!reading || reading.state === "reading") return t("registerManyTables.reading");
  if (reading.state === "failed")
    return t("registerManyTables.readFailed", { error: reading.error });
  const needs = blockedBy(reading, needsCacheTtl);
  if (needs === null) return null;
  if (needs.kind === "declared") return t("registerManyTables.needsDeclared");
  if (needs.kind === "noColumns") return t("registerManyTables.needsNoColumns");
  if (needs.kind === "cacheTtl") return t("registerManyTables.needsCacheTtl");
  return t("registerManyTables.needsUntyped", { columns: needs.columns.join(", ") });
}

export function tableChoices<T extends Listed>({
  t,
  tables,
  isRegistered,
  requiredOf,
  readings,
  needsCacheTtl,
  onRegisterOne,
  opening,
}: {
  t: TFunction;
  tables: T[];
  isRegistered: (table: T) => boolean;
  /** The parameters a read of the table must give; empty for one that lists rows with none. */
  requiredOf: (table: T) => string[];
  readings: Record<string, Reading>;
  needsCacheTtl: boolean;
  /** Registers on its own a table that needs input, opening it in edit (registerAsDraft). */
  onRegisterOne: (tableName: string) => void;
  /** The table being opened to be registered on its own. */
  opening: string | null;
}): MultiSelectOption[] {
  const offered = tables.filter((tbl) => !isRegistered(tbl));
  const lookups = offered.filter((tbl) => requiredOf(tbl).length > 0);
  const listing = offered.filter((tbl) => requiredOf(tbl).length === 0);
  const choice = (tbl: T, group: string | null, heading: string | undefined) => {
    const shown =
      group != null && tbl.comment
        ? t("registerTableForm.tableWithLabel", { name: tbl.name, label: tbl.comment })
        : tbl.name;
    const reason = whyNot(t, readings[tbl.name], needsCacheTtl);
    const action: OptionAction | undefined =
      blockedBy(readings[tbl.name], needsCacheTtl) === null
        ? undefined
        : {
            label: t("registerManyTables.registerOne"),
            ariaLabel: t("registerManyTables.registerOneLabel", { table: tbl.name }),
            onClick: () => onRegisterOne(tbl.name),
            loading: opening === tbl.name,
            disabled: opening !== null && opening !== tbl.name,
          };
    return {
      id: tbl.name,
      label: requiredOf(tbl).length > 0 ? `${LOOKUP_GLYPH} ${shown}` : shown,
      disabled: reason !== null,
      ...(reason !== null ? { reason } : {}),
      ...(action ? { action } : {}),
      ...(heading ? { group: heading } : {}),
    };
  };
  // With lookups listed apart, the tables that list rows have a heading of their own.
  const rowsHeading = lookups.length > 0 ? t("registerTableForm.tablesGroup") : undefined;
  return [
    ...tableGroups(listing).flatMap(([group, members]) =>
      members.map((tbl) => choice(tbl, group, group ?? rowsHeading)),
    ),
    ...lookups.map((tbl) => choice(tbl, tbl.group ?? null, t("registerTableForm.lookupsGroup"))),
  ];
}
