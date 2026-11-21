// Copyright (c) 2026 Kenneth Stott
// Canary: 8b4f2d63-1e9a-4c75-a0d8-3f6e9c2b5a17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923 (amended 2026-11-20, THE QUERY IS SHOWN AND EDITED ON THE TABLE): a table defined by
// a SPARQL query shows it in Identity, editable by whoever may edit the table. A change
// re-derives the columns from the SELECT variables -- a column kept keeps every setting it has, a
// new one is text and granted to every role -- and warns before dropping a column a relationship,
// write grant or mask uses. The next replica load reads with the saved query.

import { useEffect, useState } from "react";
import { Alert, Button, Text, Textarea } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";
import { useRoles } from "../../hooks/useAdminQueries";
import type { RegisteredTable, Relationship, TableColumn } from "../../types/admin";
import { draftColumn } from "./draftTable";
import { useSparqlColumns } from "./useSparqlColumns";

/** What uses a column of `table`, by name: its masks, write grants and relationships. */
function columnUses(
  table: RegisteredTable,
  relationships: Relationship[],
  column: TableColumn,
): string[] {
  const name = column.columnName;
  const uses: string[] = [];
  if ((column.masks ?? []).length > 0) uses.push("mask");
  if ((table.writeGrants ?? []).some((g) => g.columns.includes(name))) uses.push("grant");
  if (
    relationships.some(
      (r) =>
        (r.sourceTableId === table.id && r.sourceColumn === name) ||
        (r.targetTableId === table.id && r.targetColumn === name),
    )
  ) {
    uses.push("relationship");
  }
  return uses;
}

/** The columns a query of `names` gives `table`: each kept column as it is, each new one text,
 * granted to `roleIds`; numbered below zero until the server gives it an id. */
function rederived(table: RegisteredTable, names: string[], roleIds: string[]): TableColumn[] {
  return names.map(
    (name, i) =>
      table.columns.find((c) => c.columnName === name) ?? {
        ...draftColumn({ name, dataType: "text", visibleTo: roleIds }, i),
        id: -(i + 1),
      },
  );
}

export function SparqlQueryField({
  editingTable,
  setEditingTable,
  relationships,
}: {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
  /** The relationships of the org: a column one of them joins on is not dropped unannounced. */
  relationships: Relationship[];
}) {
  const { t } = useTranslation();
  // A new column is granted to every role, as at registration.
  const { roles, loading: rolesLoading } = useRoles();
  const saved = editingTable.queryTemplate ?? "";
  // The text being edited; the table takes it once its columns are read and any column it drops
  // that something uses has been agreed to.
  const [text, setText] = useState(saved);
  const parsed = useSparqlColumns(text.trim() === saved.trim() ? "" : text);
  const [agreed, setAgreed] = useState<string | null>(null);

  const names = parsed.state === "read" ? parsed.columns.map((c) => c.name) : null;
  const dropped =
    names === null ? [] : editingTable.columns.filter((c) => !names.includes(c.columnName));
  const inUse = dropped
    .map((c) => ({ column: c.columnName, uses: columnUses(editingTable, relationships, c) }))
    .filter((d) => d.uses.length > 0);
  const needsAgreement = inUse.length > 0 && agreed !== text;

  useEffect(() => {
    if (names === null || needsAgreement || rolesLoading) return;
    const roleIds = roles.map((r) => r.id);
    setEditingTable((current) =>
      current === null
        ? current
        : {
            ...current,
            queryTemplate: text.trim(),
            columns: rederived(current, names, roleIds),
          },
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps -- applied once per read of the query
  }, [parsed, needsAgreement, rolesLoading]);

  return (
    <>
      <FormField size="full" help={t("tableEditForm.sparqlQueryHelp")}>
        <Textarea
          autosize
          minRows={4}
          label={t("tableEditForm.sparqlQueryLabel")}
          value={text}
          onChange={(e) => setText(e.currentTarget.value)}
          styles={{ input: { fontFamily: "var(--mantine-font-family-monospace)" } }}
          data-testid="table-edit-sparql-query"
        />
      </FormField>
      {parsed.state === "refused" && (
        <FormField size="full">
          <Text c="red" size="sm" data-testid="table-edit-sparql-query-problem">
            {parsed.problem}
          </Text>
        </FormField>
      )}
      {needsAgreement && (
        <FormField size="full">
          <Alert color="yellow" data-testid="table-edit-sparql-drops">
            <Text size="sm">
              {t("tableEditForm.sparqlQueryDrops", {
                columns: inUse
                  .map((d) =>
                    t("tableEditForm.sparqlQueryDropUse", {
                      column: d.column,
                      uses: d.uses.map((u) => t(`tableEditForm.sparqlQueryUse_${u}`)).join(", "),
                    }),
                  )
                  .join("; "),
              })}
            </Text>
            <Button
              mt="xs"
              size="xs"
              color="yellow"
              variant="light"
              onClick={() => setAgreed(text)}
              data-testid="table-edit-sparql-drop-agree"
            >
              {t("tableEditForm.sparqlQueryDropAgree")}
            </Button>
          </Alert>
        </FormField>
      )}
    </>
  );
}
