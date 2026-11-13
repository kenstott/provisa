// Copyright (c) 2026 Kenneth Stott
// Canary: 9e3b6a07-4c1d-4f82-a6e9-d5b0c2f7e184
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 (amended 2026-11-11, THE PRODUCT PAGE IS SUMMARY ROWS): a data product's short header,
// above its summary rows: who owns it and its domain; at most three facts -- its consumers (to its
// owner alone, REQ-2160), its quality as checks passing out of those promised (never a score), its
// freshness; its support contact; its tags, parameterized ones included, with the tag chip every
// taggable object has; and every value of the product under the labels its form gives them,
// collapsed.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Anchor, Badge, Group, List, Stack, Text } from "@mantine/core";
import { CollapsibleItem, Collapsibles } from "../../components/collapsible/Collapsible";
import { DetailFields, type DetailField } from "../../components/detail/DetailFields";
import { OwnerResolutionInline } from "../../components/OwnerResolution";
import { TagControl } from "../../components/TagControl";
import { Empty, Muted } from "./DetailCard";
import type { CoreTable, ProductCore } from "./productCore";
import type { QualitySummary } from "./productQuality";
import { WorstBadge } from "./QualityStatus";

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

/** A header fact: its label, then its value. */
function Fact({ label, children }: { label: string; children: ReactNode }) {
  return (
    <Group gap={6} wrap="nowrap">
      <Text size="xs" c="dimmed">
        {label}
      </Text>
      {children}
    </Group>
  );
}

export function ProductHeader({
  core,
  fields,
  extraFields,
  quality,
  consumers,
  canEdit,
  onTagsChanged,
}: {
  core: ProductCore;
  /** Every value of the product, under the labels its form gives them. */
  fields: DetailField[];
  /** Shown under the values: the product's own custom properties. */
  extraFields?: ReactNode;
  /** The product's promised checks summarised; null while they are read. */
  quality: QualitySummary | null;
  /** How many read the product one hop downstream; null for a reader who is not its owner. */
  consumers: number | null;
  /** The reader may put tags on the product and take them off. */
  canEdit: boolean;
  onTagsChanged: () => void;
}) {
  const { t } = useTranslation();
  const product = core.product;
  const outputTables = core.tables?.filter((tb) => tb.dqContract == null) ?? [];
  const freshness = useFreshness(outputTables);
  const customPropertyEntries = Object.entries(product.customProperties ?? {});

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
        {consumers !== null && (
          <Fact label={t("dataProductCore.consumers")}>
            <Text size="sm" data-testid={`data-product-header-consumers-${product.id}`}>
              {consumers}
            </Text>
          </Fact>
        )}
        {quality !== null && (
          <Fact label={t("dataProductOffers.section_quality")}>
            <Group gap={6} wrap="nowrap" data-testid={`data-product-header-quality-${product.id}`}>
              {quality.worst && <WorstBadge worst={quality.worst} />}
              <Text size="sm">
                {t("dataProductOffers.qualityPassing", {
                  passing: quality.passing,
                  count: quality.total,
                })}
              </Text>
            </Group>
          </Fact>
        )}
        <Fact label={t("dataProductsTab.supportContactLabel")}>
          {product.supportContact ? (
            <Anchor
              size="sm"
              href={
                product.supportContact.includes("@") && !product.supportContact.includes("://")
                  ? `mailto:${product.supportContact}`
                  : product.supportContact
              }
              data-testid={`data-product-header-support-${product.id}`}
            >
              {product.supportContact}
            </Anchor>
          ) : (
            <Text size="sm" c="dimmed" data-testid={`data-product-header-support-${product.id}`}>
              —
            </Text>
          )}
        </Fact>
      </Group>
      {/* Its tags, parameterized ones included (residency:eu), as the tag chip draws them. */}
      <Group gap="xs" wrap="wrap" data-testid={`data-product-header-tags-${product.id}`}>
        <TagControl
          objectType="product"
          productId={product.id}
          readOnly={!canEdit}
          onChanged={onTagsChanged}
        />
      </Group>
      {/* Every value of the product: the read view shows them all. */}
      <Collapsibles id="data-product.header" testId={`data-product-card-details-${product.id}`}>
        {/* Closed at first, as the viewer left it after: the rows are the panel's working surface. */}
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
