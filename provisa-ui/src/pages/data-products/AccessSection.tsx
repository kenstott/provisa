// Copyright (c) 2026 Kenneth Stott
// Canary: 99265788-0c9b-4b93-b5d6-1412ad66e5da
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 (THE PRODUCT PAGE IS SUMMARY ROWS): a product's Access section, its ways in -- a query
// to copy or run now on its first dataset, and each dataset's other surfaces (GraphQL, SQL,
// JSON:API, OpenAPI, gRPC) and the relationships among them, each with its link on every surface
// that reads it (REQ-2252), as the relationship's own detail panel links it.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { Code, Group, List, Stack, Text } from "@mantine/core";
import { Braces, Cable, FileJson, Globe, Table as TableIcon } from "lucide-react";
import { CopyButton } from "../../components/CopyButton";
import {
  QueryLinkIcon,
  RelationshipQueryLinks,
} from "../../components/relationships/RelationshipQueryLinks";
import {
  relationshipQueryRef,
  type RelationshipQueryRef,
} from "../../components/relationships/relationshipQueries";
import { useRelationshipSurfaces } from "../../components/relationships/useRelationshipSurfaces";
import type { MetaReader } from "../../api/metaGraph";
import type { ListedTable, Relationship } from "../../types/admin";
import { tableSqlRef } from "../../naming";
import { Empty, Muted } from "./DetailCard";
import { modelName, type ProductCore } from "./productCore";
import {
  graphqlExample,
  grpcExample,
  jsonApiExample,
  restExample,
  sqlExample,
  type ExampleLink,
} from "./tableExamples";

/** The relationships among a product's tables (both ends members) that reach a table, each with
 * the query reference its links are built from (REQ-2252). */
function productRelationships(
  core: ProductCore,
  relationships: readonly Relationship[],
  tables: readonly ListedTable[],
): { rel: Relationship; ref: RelationshipQueryRef }[] {
  const memberIds = new Set((core.tables ?? []).map((tb) => tb.id));
  return relationships
    .filter((r) => memberIds.has(r.sourceTableId))
    .filter((r) => r.targetTableId !== null && memberIds.has(r.targetTableId))
    .flatMap((rel) => {
      const ref = relationshipQueryRef(rel, tables);
      return ref ? [{ rel, ref }] : [];
    });
}

export function AccessSection({
  core,
  reader,
  relationships,
  tables,
  actingRole,
}: {
  core: ProductCore;
  reader: MetaReader | null;
  /** The model's relationships: those among the product's tables are listed with their links. */
  relationships: readonly Relationship[];
  /** Every registered table the reader lists: a relationship's tables are named from them. */
  tables: readonly ListedTable[];
  /** The acting roles, whose names a relationship's links use. */
  actingRole: string | null;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const product = core.product;
  const outputTables = core.tables?.filter((tb) => tb.dqContract == null) ?? [];
  const first = outputTables[0];
  const snippet = first ? `select * from ${tableSqlRef(first)} limit 10` : null;
  const open = (example: ExampleLink) => navigate(example.to, { state: example.state });
  const productRels = productRelationships(core, relationships, tables);
  const surfaces = useRelationshipSurfaces(actingRole);

  const icon = (label: string, testId: string, onClick: () => void, glyph: ReactNode) => (
    <QueryLinkIcon label={label} testId={testId} onClick={onClick}>
      {glyph}
    </QueryLinkIcon>
  );

  return (
    <Stack gap={4} data-testid={`data-product-query-now-${product.id}`}>
      <Text size="xs" c="dimmed">
        {t("dataProductOffers.queryItNow")}
      </Text>
      {snippet ? (
        <Group gap={4} wrap="nowrap" align="center">
          <Code
            style={{ flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis" }}
            data-testid={`data-product-query-snippet-${product.id}`}
          >
            {snippet}
          </Code>
          <CopyButton text={snippet} />
          {icon(
            t("dataProductsTab.sqlQueryAria", { table: first.tableName }),
            `data-product-query-run-${product.id}`,
            () => open({ to: "/sql", state: { sql: snippet, autoRun: true } }),
            <TableIcon size={14} />,
          )}
        </Group>
      ) : (
        <Empty text={t("dataProductsTab.graphqlQueriesEmpty")} />
      )}
      {outputTables.length > 0 && (
        <Text size="xs" c="dimmed">
          {t("dataProductsTab.tablesLabel")}
        </Text>
      )}
      {outputTables.length > 0 && (
        <List size="sm" data-testid={`data-product-detail-graphql-queries-${product.id}`}>
          {outputTables.map((tb) => {
            const graphql = graphqlExample(reader, tb);
            const rest = restExample(reader, tb);
            const grpc = grpcExample(reader, tb);
            return (
              <List.Item key={tb.id} ff="monospace">
                <Group gap={4} wrap="nowrap">
                  <Text component="span" size="sm" ff="monospace">
                    {modelName(tb)}
                  </Text>
                  {graphql &&
                    icon(
                      t("dataProductsTab.graphqlQueryAria", { table: tb.tableName }),
                      `data-product-detail-graphql-query-${tb.id}`,
                      () => open(graphql),
                      <Braces size={14} />,
                    )}
                  {icon(
                    t("dataProductsTab.sqlQueryAria", { table: tb.tableName }),
                    `data-product-detail-sql-query-${tb.id}`,
                    () => open(sqlExample(tb)),
                    <TableIcon size={14} />,
                  )}
                  {icon(
                    t("dataProductsTab.jsonapiQueryAria", { table: tb.tableName }),
                    `data-product-detail-jsonapi-query-${tb.id}`,
                    () => open(jsonApiExample(tb)),
                    <FileJson size={14} />,
                  )}
                  {rest &&
                    icon(
                      t("dataProductsTab.openApiQueryAria", { table: tb.tableName }),
                      `data-product-detail-openapi-query-${tb.id}`,
                      () => open(rest),
                      <Globe size={14} />,
                    )}
                  {grpc &&
                    icon(
                      t("dataProductsTab.grpcQueryAria", { table: tb.tableName }),
                      `data-product-detail-grpc-query-${tb.id}`,
                      () => open(grpc),
                      <Cable size={14} />,
                    )}
                  {tb.description && <Muted>— {tb.description}</Muted>}
                </Group>
              </List.Item>
            );
          })}
          {productRels.map(({ rel, ref }) => (
            <List.Item key={`rel-${rel.id}`} ff="monospace">
              <Group gap={4} wrap="nowrap">
                <Text component="span" size="sm" ff="monospace">
                  {String(rel.id)}
                </Text>
                <RelationshipQueryLinks
                  rel={ref}
                  name={String(rel.id)}
                  surfaces={surfaces}
                  testIdPrefix={`data-product-detail-relationship-${rel.id}`}
                />
              </Group>
            </List.Item>
          ))}
        </List>
      )}
    </Stack>
  );
}
