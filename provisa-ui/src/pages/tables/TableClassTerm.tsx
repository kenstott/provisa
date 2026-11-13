// Copyright (c) 2026 Kenneth Stott
// Canary: 3c58aca1-83ca-48e2-baf8-a3df46f1d2df
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130 (class grounding): what a table's rows are -- "Rows are instances of <term>" -- shown
// read-only, linked to the term. It is set only from the glossary term (its "Grounded in tables"),
// where a term that owns columns is told apart. The table's class term is read from the meta domain
// as the user (registered_tables.classTerm), as the graph views read it.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Anchor, Text } from "@mantine/core";
import { Link } from "react-router-dom";
import { useAuth } from "../../context/AuthContext";
import { dataGraphql, metaReader, rootQuery } from "../../api/metaGraph";
import { detailHref } from "../../components/list/useDetailParam";

interface ClassTerm {
  id: number;
  name: string;
}

/** Why a table's term could not be read, as its reader is told it: an i18n key. */
class Unreadable extends Error {
  readonly reason: string;
  constructor(reason: string) {
    super(reason);
    this.reason = reason;
  }
}

/** ``tableId``'s class term (null: none), or why it could not be read. */
async function readClassTerm(role: string, tableId: number): Promise<ClassTerm | null> {
  const m = await metaReader(role);
  const query = rootQuery(
    m,
    "TableClassTerm",
    "registered_tables",
    {
      fields: ["id"],
      rels: { classTerm: { sel: { fields: ["id", "name"] } } },
    },
    { id: { eq: tableId } },
  );
  // The acting roles read no table of the model.
  if (query === null) throw new Unreadable("tableSections.classTermNoModel");
  const { registered_tables: rows } = await dataGraphql<{
    registered_tables: { id: number; classTerm?: ClassTerm | null }[];
  }>(query, {}, role);
  // The acting roles read the model's tables, but not this table's own entry in them.
  if (rows.length !== 1) throw new Unreadable("tableSections.classTermNoEntry");
  // undefined: the role's schema has no classTerm to read, which is not "none".
  if (rows[0].classTerm === undefined) throw new Unreadable("tableSections.classTermNoField");
  return rows[0].classTerm;
}

export function TableClassTerm({ tableId }: { tableId: number }) {
  const { t } = useTranslation();
  const { actingRoles } = useAuth();
  const [read, setRead] = useState<{ tableId: number; term: ClassTerm | null } | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(() => {
    if (actingRoles === null) return () => undefined;
    let live = true;
    readClassTerm(actingRoles, tableId).then(
      (term) => live && setRead({ tableId, term }),
      (e: Error) =>
        live &&
        setError(
          e instanceof Unreadable
            ? t(e.reason)
            : t("tableSections.classTermUnreadable", { error: e.message }),
        ),
    );
    return () => {
      live = false;
    };
  }, [actingRoles, tableId, t]);
  useEffect(load, [load]);
  // Polly grounds and ungrounds server-side (ChatPanel's provisa:glossary-changed): read again.
  useEffect(() => {
    const changed = () => void load();
    window.addEventListener("provisa:glossary-changed", changed);
    return () => window.removeEventListener("provisa:glossary-changed", changed);
  }, [load]);

  const current = read?.tableId === tableId ? read.term : undefined;

  if (error && current === undefined)
    return (
      <Text span size="sm" c="var(--destructive)" data-testid="table-class-term-error">
        {error}
      </Text>
    );
  if (current === undefined)
    return (
      <Text span size="sm" c="dimmed">
        {t("common.loading")}
      </Text>
    );
  return current ? (
    <Anchor
      component={Link}
      to={detailHref("ontology", current.id)}
      size="sm"
      data-testid="table-class-term-link"
    >
      {current.name}
    </Anchor>
  ) : (
    <Text span size="sm" c="dimmed" data-testid="table-class-term-none">
      {t("tableSections.noClassTerm")}
    </Text>
  );
}
