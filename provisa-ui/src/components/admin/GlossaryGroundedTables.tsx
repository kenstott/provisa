// Copyright (c) 2026 Kenneth Stott
// Canary: 6a5bf22a-4b7d-43ac-b8b1-3feb0f34f3d4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130 (class grounding): a term's "Grounded in tables", beside the columns it is. A term
// grounded in a table names what each of that table's rows is (a class); a table has at most one
// such term and needs a primary key, which the server checks and says why when it refuses. Each
// choice is saved as it is made, as the term's other relationships are.

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, MultiSelect, Stack, Text } from "@mantine/core";
import {
  groundTermTable,
  listTermTables,
  ungroundTermTable,
  type GlossaryTermTable,
} from "../../api/glossary";
import { useTables } from "../../hooks/useAdminQueries";

const label = (t: { domain_id: string; table_name: string }) => `${t.domain_id}.${t.table_name}`;

export function GlossaryGroundedTables({
  termId,
  canEdit,
  onChanged,
}: {
  termId: number;
  canEdit: boolean;
  /** After a table is grounded or ungrounded: the term's standing and role may have changed. */
  onChanged: () => void;
}) {
  const { t } = useTranslation();
  const { tables } = useTables();
  // Kept with the term it was read for: another term's answer is not this one's.
  const [read, setRead] = useState<{ termId: number; tables: GlossaryTermTable[] } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [seq, setSeq] = useState(0);

  useEffect(() => {
    let live = true;
    listTermTables(termId).then(
      (rows) => live && setRead({ termId, tables: rows }),
      (e: Error) => live && setError(e.message),
    );
    return () => {
      live = false;
    };
  }, [termId, seq]);

  const grounded = read?.termId === termId ? read.tables : null;
  const options = useMemo(() => {
    const byId = new Map<string, string>();
    for (const tb of tables)
      if (!tb.draft) byId.set(String(tb.id), label({ domain_id: tb.domainId, table_name: tb.tableName }));
    // A grounded table the table list does not hold (outside the domains it is read for) is named.
    for (const g of grounded ?? []) byId.set(String(g.table_id), label(g));
    return [...byId.entries()]
      .map(([value, text]) => ({ value, label: text }))
      .sort((a, b) => a.label.localeCompare(b.label));
  }, [tables, grounded]);

  const change = async (next: string[]) => {
    if (!grounded) return;
    const was = new Set(grounded.map((g) => String(g.table_id)));
    const now = new Set(next);
    setSaving(true);
    setError(null);
    try {
      for (const id of now) if (!was.has(id)) await groundTermTable(termId, Number(id));
      for (const id of was) if (!now.has(id)) await ungroundTermTable(termId, Number(id));
      // The term's standing and role follow (the page reads the term again).
      onChanged();
    } catch (e) {
      // Refused (no primary key, or another term's table): said here.
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
      // What was saved is read back, the refused change's earlier steps included.
      setSeq((n) => n + 1);
    }
  };

  return (
    <Stack gap={4} data-testid="glossary-grounded-tables">
      {error && (
        <Alert color="red" data-testid="glossary-grounded-tables-error">
          {error}
        </Alert>
      )}
      {grounded === null ? (
        <Text size="xs" c="dimmed">
          {t("common.loading")}
        </Text>
      ) : canEdit ? (
        <MultiSelect
          data={options}
          value={grounded.map((g) => String(g.table_id))}
          onChange={(v) => void change(v)}
          disabled={saving}
          searchable
          clearable
          placeholder={t("glossaryTab.groundedTablesPlaceholder")}
          aria-label={t("glossaryTab.groundedTablesTitle")}
          data-testid="glossary-grounded-tables-select"
        />
      ) : grounded.length === 0 ? (
        <Text size="xs" c="dimmed">
          {t("glossaryTab.noGroundedTables")}
        </Text>
      ) : (
        <Text size="sm" data-testid="glossary-grounded-tables-list">
          {grounded.map(label).join(", ")}
        </Text>
      )}
    </Stack>
  );
}
