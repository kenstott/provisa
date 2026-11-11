// Copyright (c) 2026 Kenneth Stott
// Canary: 9e3b6a07-4c1d-4f82-a6e9-d5b0c2f7e184
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A data product's panel header, above its tabs: who owns it, its domain, how fresh its data is, a
// query to run on it now with every way in to each of its tables, and every value of the product
// (the read view shows them all) under the labels its form gives them.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { ActionIcon, Badge, Code, Group, List, Stack, Text } from "@mantine/core";
import { Braces, Cable, FileJson, Globe, Share2, Table as TableIcon } from "lucide-react";
import { CopyButton } from "../../components/CopyButton";
import { CollapsibleItem, Collapsibles } from "../../components/collapsible/Collapsible";
import { DetailFields, type DetailField } from "../../components/detail/DetailFields";
import { OwnerResolutionInline } from "../../components/OwnerResolution";
import { Tip } from "../../components/Tip";
import type { MetaReader } from "../../api/metaGraph";
import { tableSqlRef } from "../../naming";
import { Empty, Muted } from "./DetailCard";
import { modelName, type CoreTable, type ProductCore } from "./productCore";
import {
  graphqlExample,
  grpcExample,
  jsonApiExample,
  restExample,
  sqlExample,
  type ExampleLink,
} from "./tableExamples";

/** REQ-2118: a table that is loaded once and never refreshed. */
const NEVER_EXPIRES = -1;

/** The product's freshness: the stalest its tables may be, in words. */
function useFreshness(tables: CoreTable[]): string {
  const { t } = useTranslation();
  const ttls = tables.map((tb) => tb.cacheTtl).filter((v): v is number => v != null);
  if (ttls.length === 0) return t("dataProductOffers.freshnessLive");
  if (ttls.every((v) => v === NEVER_EXPIRES)) return t("dataProductsTab.contract.loadedOnce");
  return t("dataProductsTab.contract.freshness", {
    seconds: Math.max(...ttls.filter((v) => v !== NEVER_EXPIRES)),
  });
}

export function ProductHeader({
  core,
  reader,
  fields,
  extraFields,
}: {
  core: ProductCore;
  reader: MetaReader | null;
  /** Every value of the product, under the labels its form gives them. */
  fields: DetailField[];
  /** Shown under the values: the product's own custom properties. */
  extraFields?: ReactNode;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const product = core.product;
  const outputTables = core.tables?.filter((tb) => tb.dqContract == null) ?? [];
  const freshness = useFreshness(outputTables);
  const first = outputTables[0];
  const snippet = first ? `select * from ${tableSqlRef(first)} limit 10` : null;
  const open = (example: ExampleLink) => navigate(example.to, { state: example.state });
  const customPropertyEntries = Object.entries(product.customProperties ?? {});
  // REQ-1634: Cypher click-through for the relationships among its tables that resolve to a type.
  const memberIds = new Set((core.tables ?? []).map((tb) => tb.id));
  const cypherTypes = [
    ...new Set(
      (core.tables ?? []).flatMap((tb) =>
        (tb.sourceRelationships ?? [])
          .filter((r) => r.targetTable != null && memberIds.has(r.targetTable.id))
          .filter((r) => !r.disableCypher && r.cypherType)
          .map((r) => r.cypherType as string),
      ),
    ),
  ];

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
    <Stack gap="xs" data-testid={`data-product-header-${product.id}`}>
      <Group gap="md" wrap="wrap">
        <Group gap={6} wrap="nowrap">
          <Text size="xs" c="dimmed">
            {t("dataProductsTab.ownerLabel")}
          </Text>
          <Text size="sm" ff="monospace" data-testid={`data-product-header-owner-${product.id}`}>
            {product.ownerRole ?? "—"}
          </Text>
          {product.ownerRole && <OwnerResolutionInline refs={[product.ownerRole]} />}
        </Group>
        <Group gap={6} wrap="nowrap">
          <Text size="xs" c="dimmed">
            {t("dataProductsTab.domainLabel")}
          </Text>
          <Text size="sm" ff="monospace">
            {product.domainId}
          </Text>
        </Group>
        <Group gap={6} wrap="nowrap">
          <Text size="xs" c="dimmed">
            {t("dataProductOffers.freshness")}
          </Text>
          <Badge
            size="sm"
            variant="light"
            data-testid={`data-product-header-freshness-${product.id}`}
          >
            {freshness}
          </Badge>
        </Group>
      </Group>
      {/* Query it now: a statement to copy or run, and each table's other ways in. */}
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
      {/* Every value of the product: the read view shows them all. */}
      <Collapsibles id="data-product.header" testId={`data-product-card-details-${product.id}`}>
        <CollapsibleItem
          value="details"
          defaultOpen={false}
          controlTestId={`data-product-card-details-${product.id}-toggle`}
          title={
            <Text size="sm" fw={500}>
              {t("dataProductsTab.detail.field.details")}
            </Text>
          }
        >
          <DetailFields testId={`data-product-read-${product.id}`} fields={fields} />
          <Text size="xs" c="dimmed" fw={500} mt="xs" tt="uppercase">
            {t("dataProductsTab.detail.field.customProperties")}
          </Text>
          {customPropertyEntries.length === 0 ? (
            <Empty text={t("dataProductsTab.detail.customPropertiesEmpty")} />
          ) : (
            <List size="sm" data-testid={`data-product-detail-custom-properties-${product.id}`}>
              {customPropertyEntries.map(([key, val]) => (
                <List.Item key={key} ff="monospace">
                  {key}
                  <Muted>= {String(val)}</Muted>
                </List.Item>
              ))}
            </List>
          )}
          {extraFields}
        </CollapsibleItem>
      </Collapsibles>
    </Stack>
  );
}
