// Copyright (c) 2026 Kenneth Stott
// Canary: 3d8f0b61-2e4a-4c97-b5d1-a7c9e3f2b046
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2126, REQ-2174: a data product's Related tab -- the products sharing its datasets, the tables
// directly related to its own, the relationships among its tables, the ontology terms it reaches
// (in the ontology graph, focused on it), its tags, its commands and, for its owner, who reads it.
// Each dataset's associations are the tables page's (REQ-2239).

import { useTranslation } from "react-i18next";
import { Anchor, Group, List, Stack, Text } from "@mantine/core";
import type { ProductLineage } from "../../../api/lineage";
import { CollapsibleItem, Collapsibles } from "../../../components/collapsible/Collapsible";
import { TagControl } from "../../../components/TagControl";
import { OntologySnippet } from "../../../components/graph3d/OntologyGraph";
import { productPreset } from "../../../components/graph3d/ontologyPresets";
import type { Relationship } from "../../../types/admin";
import { TableAssociations } from "../../tables/TableAssociations";
import { ClampedDetail } from "../ClampedDetail";
import { ConsumersList } from "../dataProductDq";
import { Empty, Muted } from "../DetailCard";
import { modelName, type CoreEdge, type ProductCore } from "../productCore";
import { LineagePending } from "./LineagePending";

/** A product sharing one or more of this product's datasets. */
export interface SharingProduct {
  id: string;
  name: string;
  /** The shared datasets, as the model names them. */
  datasets: string[];
}

function Section({
  testId,
  label,
  children,
}: {
  testId: string;
  label: string;
  children: React.ReactNode;
}) {
  return (
    <Stack gap={4} data-testid={testId}>
      <Text size="sm" fw={500}>
        {label}
      </Text>
      {children}
    </Stack>
  );
}

function relatedLabel(e: CoreEdge): string {
  return e.targetTable ? modelName(e.targetTable) : String(e.targetTableId);
}

export function RelatedTab({
  productId,
  core,
  canEdit,
  seesConsumers,
  lineageGraph,
  lineageLoading,
  lineageError,
  sharing,
  relationships,
  onTagsChanged,
  openProduct,
}: {
  productId: string;
  core: ProductCore;
  canEdit: boolean;
  seesConsumers: boolean;
  lineageGraph: ProductLineage | null;
  lineageLoading: boolean;
  lineageError: string;
  sharing: SharingProduct[];
  relationships: Relationship[];
  onTagsChanged: () => void;
  openProduct: (id: string) => void;
}) {
  const { t } = useTranslation();
  const outputTables = core.tables?.filter((tb) => tb.dqContract == null) ?? [];
  const related = core.edges?.filter((e) => e.kind === "related");
  const memberIds = new Set((core.tables ?? []).map((tb) => tb.id));
  const internalRelationships = (core.tables ?? []).flatMap((tb) =>
    (tb.sourceRelationships ?? [])
      .filter((r) => r.targetTable != null && memberIds.has(r.targetTable.id))
      .map((r) => ({ ...r, source: tb })),
  );
  return (
    <Stack gap="md" data-testid={`data-product-related-${productId}`}>
      <Section
        testId={`data-product-card-sharing-${productId}`}
        label={t("dataProductOffers.sharingProducts")}
      >
        {sharing.length === 0 ? (
          <Empty text={t("dataProductOffers.noSharingProducts")} />
        ) : (
          <List size="sm">
            {sharing.map((p) => (
              <List.Item key={p.id}>
                <Anchor
                  component="button"
                  type="button"
                  size="sm"
                  onClick={() => openProduct(p.id)}
                >
                  {p.name}
                </Anchor>
                <Muted>— {p.datasets.join(", ")}</Muted>
              </List.Item>
            ))}
          </List>
        )}
      </Section>
      {related !== undefined && (
        <Section
          testId={`data-product-card-relatedTables-${productId}`}
          label={t("dataProductsTab.detail.field.relatedTables")}
        >
          {related.length === 0 ? (
            <Empty text={t("dataProductsTab.detail.relatedTablesEmpty")} />
          ) : (
            <List size="sm" data-testid={`data-product-detail-related-tables-${productId}`}>
              {related.map((e) => (
                <List.Item key={e.id} ff="monospace">
                  <ClampedDetail
                    testId={`data-product-related-${productId}-${e.targetProductId ?? e.targetTableId}`}
                  >
                    {relatedLabel(e)}
                    {e.targetProduct && (
                      <Muted>
                        ({t("dataProductCore.inProduct", { product: e.targetProduct.name })})
                      </Muted>
                    )}
                    <Text size="xs" c="var(--text-muted)">
                      {[
                        e.aliases,
                        e.pathCount > 1 ? t("dataProductCore.paths", { count: e.pathCount }) : null,
                      ]
                        .filter(Boolean)
                        .join(" · ")}
                    </Text>
                    {e.targetTable?.description && (
                      <Text size="xs" c="var(--text-muted)">
                        {e.targetTable.description}
                      </Text>
                    )}
                  </ClampedDetail>
                </List.Item>
              ))}
            </List>
          )}
        </Section>
      )}
      <Section
        testId={`data-product-card-relationships-${productId}`}
        label={t("dataProductsTab.detail.field.relationships")}
      >
        {internalRelationships.length === 0 ? (
          <Empty text={t("dataProductsTab.detail.relationshipsEmpty")} />
        ) : (
          <List size="sm" data-testid={`data-product-detail-relationships-${productId}`}>
            {internalRelationships.map((r) => (
              <List.Item key={r.id} ff="monospace">
                {[r.graphqlField, r.cypherType].filter(Boolean).join("/​")}
                <Muted>
                  {r.source.tableName}.{r.sourceColumn}
                  {" → "}
                  {r.targetTable?.tableName}
                  {r.targetColumn ? `.${r.targetColumn}` : ""} ({r.cardinality})
                </Muted>
              </List.Item>
            ))}
          </List>
        )}
      </Section>
      {core.terms !== undefined && (
        <Section
          testId={`data-product-card-relatedTerms-${productId}`}
          label={t("dataProductsTab.relatedTermsLabel")}
        >
          {core.terms.length === 0 ? (
            <Empty text={t("dataProductsTab.relatedTermsEmpty")} />
          ) : (
            <List size="sm" data-testid={`data-product-detail-related-terms-${productId}`}>
              {core.terms.map((term) => (
                <List.Item key={term.id} ff="monospace">
                  {term.name}
                  {term.isAbstract && <Muted>({t("dataProductsTab.relatedTermsConcept")})</Muted>}
                  {term.definition && <Muted>— {term.definition}</Muted>}
                </List.Item>
              ))}
            </List>
          )}
        </Section>
      )}
      {/* REQ-2174: the ontology, focused on the product: the terms it reaches and beyond. */}
      <Section testId={`data-product-card-graph-${productId}`} label={t("graph3d.panelTitle")}>
        <OntologySnippet
          preset={productPreset(productId)}
          name={core.product.name}
          testId="graph-panel"
        />
      </Section>
      {core.tags !== undefined && (
        <Section testId={`data-product-card-tags-${productId}`} label={t("dataProductCore.tags")}>
          {canEdit ? (
            <Group gap="xs" wrap="wrap">
              {core.tags.length === 0 && <Empty text={t("dataProductCore.tagsEmpty")} />}
              <TagControl objectType="product" productId={productId} onChanged={onTagsChanged} />
            </Group>
          ) : core.tags.length === 0 ? (
            <Empty text={t("dataProductCore.tagsEmpty")} />
          ) : (
            <List size="sm" data-testid={`data-product-detail-tags-${productId}`}>
              {core.tags.map((tag) => (
                <List.Item key={tag} ff="monospace">
                  {tag}
                </List.Item>
              ))}
            </List>
          )}
        </Section>
      )}
      {core.commands !== undefined && (
        <Section
          testId={`data-product-card-commands-${productId}`}
          label={t("dataProductsTab.commandsLabel")}
        >
          {core.commands.length === 0 ? (
            <Empty text={t("dataProductsTab.commandsEmpty")} />
          ) : (
            <List size="sm" data-testid={`data-product-detail-commands-${productId}`}>
              {core.commands.map((fn) => (
                <List.Item key={fn.name} ff="monospace">
                  {fn.name}
                  {fn.description && <Muted>— {fn.description}</Muted>}
                </List.Item>
              ))}
            </List>
          )}
        </Section>
      )}
      {/* REQ-2160, REQ-2219: who reads the product, one hop downstream; its owner's alone. */}
      {seesConsumers && (
        <Section
          testId={`data-product-card-consumers-${productId}`}
          label={t("dataProductCore.consumers")}
        >
          <Text size="xs" c="dimmed">
            {t("dataProductCore.consumersHint")}
          </Text>
          {lineageGraph ? (
            <ConsumersList
              consumers={lineageGraph.consumers}
              ports={core.tables}
              testId={`data-product-detail-consumers-${productId}`}
            />
          ) : (
            <LineagePending loading={lineageLoading} error={lineageError} />
          )}
        </Section>
      )}
      {outputTables.length > 0 && (
        <Section
          testId={`data-product-card-associations-${productId}`}
          label={t("dataProductOffers.datasetAssociations")}
        >
          <Collapsibles id={`data-product.associations.${productId}`}>
            {outputTables.map((tb) => (
              <CollapsibleItem
                key={tb.id}
                value={String(tb.id)}
                defaultOpen={false}
                mountWhenOpen
                controlTestId={`data-product-associations-${tb.id}-toggle`}
                title={
                  <Text size="sm" ff="monospace">
                    {modelName(tb)}
                  </Text>
                }
              >
                <TableAssociations table={tb} relationships={relationships} />
              </CollapsibleItem>
            ))}
          </Collapsibles>
        </Section>
      )}
    </Stack>
  );
}
