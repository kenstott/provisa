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
// JSON:API, OpenAPI, gRPC) and the Cypher relationships among them.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { ActionIcon, Code, Group, List, Stack, Text } from "@mantine/core";
import { Braces, Cable, FileJson, Globe, Share2, Table as TableIcon } from "lucide-react";
import { CopyButton } from "../../components/CopyButton";
import { Tip } from "../../components/Tip";
import type { MetaReader } from "../../api/metaGraph";
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

/** The Cypher relationship types among a product's tables that resolve to a type. */
function cypherTypesOf(core: ProductCore): string[] {
  const memberIds = new Set((core.tables ?? []).map((tb) => tb.id));
  return [
    ...new Set(
      (core.tables ?? []).flatMap((tb) =>
        (tb.sourceRelationships ?? [])
          .filter((r) => r.targetTable != null && memberIds.has(r.targetTable.id))
          .filter((r) => !r.disableCypher && r.cypherType)
          .map((r) => r.cypherType as string),
      ),
    ),
  ];
}

export function AccessSection({ core, reader }: { core: ProductCore; reader: MetaReader | null }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const product = core.product;
  const outputTables = core.tables?.filter((tb) => tb.dqContract == null) ?? [];
  const first = outputTables[0];
  const snippet = first ? `select * from ${tableSqlRef(first)} limit 10` : null;
  const open = (example: ExampleLink) => navigate(example.to, { state: example.state });
  const cypherTypes = cypherTypesOf(core);

  const icon = (label: string, testId: string, onClick: () => void, glyph: ReactNode) => (
    <Tip text={label} named>
      <ActionIcon
        variant="subtle"
        size="sm"
        aria-label={label}
        data-testid={testId}
        onClick={onClick}
      >
        {glyph}
      </ActionIcon>
    </Tip>
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
          {cypherTypes.map((type) => (
            <List.Item key={`cy-${type}`} ff="monospace">
              <Group gap={4} wrap="nowrap">
                <Text component="span" size="sm" ff="monospace">
                  {type}
                </Text>
                {icon(
                  t("dataProductsTab.cypherQueryAria", { type }),
                  `data-product-detail-cypher-query-${type}`,
                  () =>
                    navigate("/graph", {
                      state: {
                        query: `MATCH ()-[r:${type}]->() RETURN r LIMIT 25`,
                        autoRun: true,
                      },
                    }),
                  <Share2 size={14} />,
                )}
              </Group>
            </List.Item>
          ))}
        </List>
      )}
    </Stack>
  );
}
