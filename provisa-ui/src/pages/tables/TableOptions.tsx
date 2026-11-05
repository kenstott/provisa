// Copyright (c) 2026 Kenneth Stott
// Canary: 699cba72-5936-40ec-a387-5c3f8281486c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Register Table form's table choices: a source's tables in the order it lists them, under the
// heading each is listed under where it has one (REQ-1954: a ServiceNow table's theme), shown by
// name and label. REQ-1986: a table that needs a parameter to be read (a lookup, such as a GraphQL
// root field with a required id) is listed apart, after the others, marked and naming what it
// needs, so a steward is not surprised by a required filter after picking it.

import { useTranslation } from "react-i18next";
import { LOOKUP_GLYPH, lookupNeeds } from "./lookups";
import { tableGroups } from "./tableGroups";

interface Listed {
  name: string;
  comment: string | null;
  group?: string | null;
}

export function TableOptions<T extends Listed>({
  tables,
  isRegistered,
  requiredOf,
}: {
  tables: T[];
  isRegistered: (table: T) => boolean;
  /** The parameters a read of the table must give; empty for one that lists rows with none. */
  requiredOf: (table: T) => string[];
}) {
  const { t } = useTranslation();
  const option = (tbl: T, group: string | null) => {
    const required = requiredOf(tbl);
    const shown =
      group != null && tbl.comment
        ? t("registerTableForm.tableWithLabel", { name: tbl.name, label: tbl.comment })
        : tbl.name;
    return (
      <option
        key={tbl.name}
        value={tbl.name}
        disabled={isRegistered(tbl)}
        title={required.length ? lookupNeeds(t, required) : undefined}
      >
        {required.length ? `${LOOKUP_GLYPH} ${shown}` : shown}
      </option>
    );
  };
  const lookups = tables.filter((tbl) => requiredOf(tbl).length > 0);
  const listing = tables.filter((tbl) => requiredOf(tbl).length === 0);
  const grouped = tableGroups(listing).map(([group, members]) => {
    const options = members.map((tbl) => option(tbl, group));
    if (group != null) {
      return (
        <optgroup key={group} label={group}>
          {options}
        </optgroup>
      );
    }
    // With lookups listed apart, the tables that list rows have a heading of their own.
    return lookups.length ? (
      <optgroup key="" label={t("registerTableForm.tablesGroup")}>
        {options}
      </optgroup>
    ) : (
      options
    );
  });
  if (!lookups.length) return grouped;
  return [
    ...grouped,
    <optgroup key="\0lookups" label={t("registerTableForm.lookupsGroup")}>
      {lookups.map((tbl) => option(tbl, tbl.group ?? null))}
    </optgroup>,
  ];
}
