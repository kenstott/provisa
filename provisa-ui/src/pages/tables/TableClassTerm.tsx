// Copyright (c) 2026 Kenneth Stott
// Canary: 3c58aca1-83ca-48e2-baf8-a3df46f1d2df
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130 (class grounding): what a table's rows are -- "Rows are instances of <term>" -- set and
// cleared in place by an ontology curator, read by anyone. A table has at most one class term, so
// setting another first lets the old one go. The table's class term is read from the meta domain
// as the user (registered_tables.classTerm), as the graph views read it.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Anchor, Group, Select, Text } from "@mantine/core";
import { Link } from "react-router-dom";
import { useAuth } from "../../context/AuthContext";
import { useCapability } from "../../hooks/useCapability";
import { dataGraphql, rootQuery } from "../../api/metaGraph";
import { metaReader } from "../../components/graph3d/useProductGraph";
import {
  groundTermTable,
  listGlossaryTerms,
  ungroundTermTable,
  type GlossaryTermSummary,
} from "../../api/glossary";

interface ClassTerm {
  id: number;
  name: string;
}

/** ``tableId``'s class term (null: none), or why it could not be read. */
async function readClassTerm(role: string, tableId: number): Promise<ClassTerm | null> {
  const m = await metaReader(role);
  const query = rootQuery(
    m,
    "TableClassTerm",
    "registered_tables",
    { fields: ["id"], rels: { class_term: { alias: "classTerm", sel: { fields: ["id", "name"] } } } },
    { id: { eq: tableId } },
  );
  if (query === null) throw new Error("registered_tables");
  const { registered_tables: rows } = await dataGraphql<{
    registered_tables: { id: number; class_term?: ClassTerm | null }[];
  }>(query, {}, role);
  if (rows.length !== 1) throw new Error(`registered_tables ${tableId}`);
  // undefined: the role's schema has no classTerm to read, which is not "none".
  if (rows[0].class_term === undefined) throw new Error("classTerm");
  return rows[0].class_term;
}

export function TableClassTerm({ tableId }: { tableId: number }) {
  const { t } = useTranslation();
  const { actingRoles } = useAuth();
  const canEdit = useCapability("glossary_rw");
  const [read, setRead] = useState<{ tableId: number; term: ClassTerm | null } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [terms, setTerms] = useState<GlossaryTermSummary[] | null>(null);
  const [saving, setSaving] = useState(false);

  const load = useCallback(() => {
    if (actingRoles === null) return () => undefined;
    let live = true;
    readClassTerm(actingRoles, tableId).then(
      (term) => live && setRead({ tableId, term }),
      (e: Error) => live && setError(t("tableSections.classTermUnreadable", { error: e.message })),
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

  // The terms to choose from, read when the curator opens the picker.
  const loadTerms = () => {
    if (terms) return;
    listGlossaryTerms("", false).then(setTerms, (e: Error) => setError(e.message));
  };

  const current = read?.tableId === tableId ? read.term : undefined;

  const set = async (next: number | null) => {
    if (current === undefined) return;
    setSaving(true);
    setError(null);
    try {
      if (current && current.id !== next) await ungroundTermTable(current.id, tableId);
      if (next !== null && current?.id !== next) await groundTermTable(next, tableId);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
      load();
    }
  };

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
  const named = current ? (
    <Anchor
      component={Link}
      to={`/admin/ontology?term=${current.id}`}
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
  if (!canEdit) return named;
  const options = [
    ...(terms ?? []).map((x) => ({ value: String(x.id), label: x.name })),
    // The current term is named before the list is read.
    ...(current && !(terms ?? []).some((x) => x.id === current.id)
      ? [{ value: String(current.id), label: current.name }]
      : []),
  ];
  return (
    <Group gap="xs" wrap="nowrap" data-testid="table-class-term">
      <Select
        size="xs"
        data={options}
        value={current ? String(current.id) : null}
        onDropdownOpen={loadTerms}
        onChange={(v) => void set(v === null ? null : Number(v))}
        searchable
        clearable
        disabled={saving}
        placeholder={t("tableSections.classTermPlaceholder")}
        aria-label={t("tableSections.classTerm")}
        data-testid="table-class-term-select"
      />
      {error && (
        <Text span size="xs" c="var(--destructive)" data-testid="table-class-term-error">
          {error}
        </Text>
      )}
    </Group>
  );
}
