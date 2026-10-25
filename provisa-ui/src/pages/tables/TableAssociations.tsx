// Copyright (c) 2026 Kenneth Stott
// Canary: ab8e5482-81ff-49d4-93c6-018fc14d3ccf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { Anchor, Stack, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import type { RegisteredTable, Relationship } from "../../types/admin";
import { fetchRelatedGlossaryTerms, type RelatedGlossaryTerm } from "../../api/glossary";
import { TagControl } from "../../components/TagControl";
import { DetailFields, type DetailField } from "../../components/detail/DetailFields";
import { TableClassTerm } from "./TableClassTerm";

/**
 * What a table is associated with besides its data product, the same in its read view and its
 * edit form: its tags, the glossary terms that name its columns, and the relationships it takes
 * part in. The two lists are read-only; a relationship is changed on the Relationships page, a
 * term in the glossary.
 */
export function TableAssociations({
  table,
  relationships,
  leading = [],
}: {
  table: RegisteredTable;
  relationships: Relationship[];
  /** Fields listed first: the data product, where it is read rather than chosen. */
  leading?: DetailField[];
}) {
  const { t } = useTranslation();
  const terms = useColumnTerms(table.id);
  const related = relationships.filter(
    (r) => r.sourceTableId === table.id || r.targetTableId === table.id,
  );
  return (
    <DetailFields
      testId="table-associations"
      fields={[
        ...leading,
        {
          // REQ-2130: what each of its rows is, set and cleared in place.
          label: t("tableSections.classTerm"),
          testId: "table-associations-class-term",
          value: <TableClassTerm tableId={table.id} />,
        },
        {
          label: t("tableSections.tags"),
          value: <TagControl objectType="table" tableId={table.id} />,
        },
        {
          label: t("tableSections.glossaryTerms"),
          testId: "table-associations-terms",
          value:
            terms === "loading" ? (
              <Text span size="sm" c="dimmed">
                {t("common.loading")}
              </Text>
            ) : terms instanceof Error ? (
              <Text span size="sm" c="var(--destructive)">
                {terms.message}
              </Text>
            ) : terms.length === 0 ? (
              <Text span size="sm" c="dimmed">
                {t("tableSections.noTerms")}
              </Text>
            ) : (
              terms.map((term) => term.name).join(", ")
            ),
        },
        {
          label: t("tableSections.relationships"),
          testId: "table-associations-relationships",
          value:
            related.length === 0 ? (
              <Text span size="sm" c="dimmed">
                {t("tableSections.noRelationships")}
              </Text>
            ) : (
              <Stack gap={2}>
                {related.map((r) => (
                  <Anchor
                    key={r.id}
                    component={Link}
                    to={`/relationships?search=${encodeURIComponent(r.sourceTableName)}`}
                    size="sm"
                    data-testid={`table-association-relationship-${r.id}`}
                  >
                    {`${r.sourceTableName}.${r.sourceColumn} → ${r.targetTableName}${
                      r.targetColumn ? `.${r.targetColumn}` : ""
                    }`}
                  </Anchor>
                ))}
              </Stack>
            ),
        },
      ]}
    />
  );
}

/**
 * The glossary terms that name this table's columns: the terms holding a reference to one of its
 * columns. The server's related-terms answer also carries the abstract terms reachable from those;
 * they name no column, so they are not listed here.
 */
function useColumnTerms(tableId: number): RelatedGlossaryTerm[] | "loading" | Error {
  // Kept with the table it was read for: another table's answer is not this one's.
  const [read, setRead] = useState<{ tableId: number; terms: RelatedGlossaryTerm[] | Error }>();
  useEffect(() => {
    let live = true;
    fetchRelatedGlossaryTerms([tableId]).then(
      (all) => live && setRead({ tableId, terms: all.filter((term) => !term.is_abstract) }),
      (err: Error) => live && setRead({ tableId, terms: err }),
    );
    return () => {
      live = false;
    };
  }, [tableId]);
  return read?.tableId === tableId ? read.terms : "loading";
}
