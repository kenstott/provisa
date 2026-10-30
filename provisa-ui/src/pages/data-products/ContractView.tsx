// Copyright (c) 2026 Kenneth Stott
// Canary: ff511d1f-2c93-40ca-84b7-d92d048c2720
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a product member's contract, its output port, as its open row in the product's contracts
// list shows it: every ODCS 3.1 element, declared or derived (REQ-2118), in the same dense card
// panels the product uses -- details, fields promised, SLAs and freshness, quality promises,
// consumers, versions and changes. An element nothing provides is shown as not declared. Elements
// are named by their ODCS names, the standard's own vocabulary.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Group, Stack, Text } from "@mantine/core";
import type { OdcsContract, OdcsProperty, OdcsRecord } from "../../api/dataProductContracts";
import type { ProductConsumer, ProductDqEntry } from "../../api/lineage";
import { Collapsibles, CollapsibleItem } from "../../components/collapsible/Collapsible";
import { DetailCard } from "./DetailCard";
import { ClaimsCallout, ConsumersList } from "./dataProductDq";

/** The Details card's further ODCS sections, each a row collapsed to a one-line summary. */
const DETAIL_SECTIONS = ["support", "servers", "pricing", "custom"] as const;
type Section = (typeof DETAIL_SECTIONS)[number];

/** A value as one line of text: a list joined, a record as its fields. */
function shown(value: unknown): string {
  if (Array.isArray(value)) return value.map(shown).join(", ");
  if (value !== null && typeof value === "object") return record(value as OdcsRecord);
  return String(value);
}

/** A record (a rule, a server, a service level) as its set fields, in the order ODCS gives them. */
function record(r: OdcsRecord): string {
  return Object.entries(r)
    .filter(([, v]) => v !== null && v !== undefined)
    .map(([k, v]) => `${k}: ${shown(v)}`)
    .join(" · ");
}

function present(value: unknown): boolean {
  return !(value === null || value === undefined || (Array.isArray(value) && value.length === 0));
}

function NotDeclared() {
  const { t } = useTranslation();
  return (
    <Text component="span" size="sm" c="dimmed" fs="italic">
      {t("dataProductsTab.contract.notDeclared")}
    </Text>
  );
}

/** One element: its ODCS name and its value, or that it is not declared. */
function Element({ name, value }: { name: string; value: unknown }) {
  return (
    <Group gap={6} wrap="nowrap" align="baseline">
      <Text size="xs" ff="monospace" c="var(--text-muted)" style={{ flexShrink: 0 }}>
        {name}
      </Text>
      {present(value) ? (
        <Text size="sm" style={{ minWidth: 0, overflowWrap: "anywhere" }}>
          {shown(value)}
        </Text>
      ) : (
        <NotDeclared />
      )}
    </Group>
  );
}

/** A list element: each item on its line, or that it is not declared. */
function Records({ name, items, testId }: { name: string; items?: unknown[]; testId?: string }) {
  if (!items || items.length === 0) return <Element name={name} value={undefined} />;
  return (
    <Stack gap={0} data-testid={testId}>
      <Text size="xs" ff="monospace" c="var(--text-muted)">
        {name}
      </Text>
      {items.map((item, i) => (
        <Text key={i} size="sm" pl="sm" style={{ overflowWrap: "anywhere" }}>
          {shown(item)}
        </Text>
      ))}
    </Stack>
  );
}

/** A property's elements as ODCS 3.1 names them, each shown declared or not; any further element
 * the property carries follows them. */
const PROPERTY_ELEMENTS = [
  "name",
  "logicalType",
  "physicalType",
  "logicalTypeOptions",
  "required",
  "unique",
  "primaryKey",
  "primaryKeyPosition",
  "classification",
  "description",
  "examples",
] as const;

/** The schema's properties: a row each, its name and a one-line summary; a row opens to every
 * element of the property, each on its own wrapping line, so a property is read whole in a card of
 * any width. */
function Properties({
  properties,
  tableId,
  testId,
}: {
  properties: OdcsProperty[];
  tableId: number;
  testId: string;
}) {
  const flags = (p: OdcsProperty) =>
    (["required", "unique", "primaryKey"] as const).filter((k) => p[k] === true);
  return (
    <Stack gap={2} data-testid={testId}>
      <Text size="xs" ff="monospace" c="var(--text-muted)">
        properties
      </Text>
      <Collapsibles id={`data-product.contract.${tableId}.properties`}>
        {properties.map((p) => {
          const extra = Object.keys(p).filter(
            (k) => !(PROPERTY_ELEMENTS as readonly string[]).includes(k) && k !== "quality",
          );
          return (
            <CollapsibleItem
              key={p.name}
              value={p.name}
              defaultOpen={false}
              mountWhenOpen
              testId={`${testId}-${p.name}`}
              controlTestId={`${testId}-${p.name}-toggle`}
              title={
                <Group gap={8} wrap="nowrap" style={{ minWidth: 0 }}>
                  <Text size="sm" ff="monospace" style={{ flexShrink: 0 }}>
                    {p.name}
                  </Text>
                  <Text
                    size="sm"
                    c="dimmed"
                    truncate="end"
                    style={{ minWidth: 0 }}
                    data-testid={`${testId}-${p.name}-summary`}
                  >
                    {[p.logicalType, p.physicalType, ...flags(p), p.classification]
                      .filter(Boolean)
                      .join(" · ")}
                  </Text>
                </Group>
              }
            >
              <Stack gap={2}>
                {PROPERTY_ELEMENTS.map((k) => (
                  <Element key={k} name={k} value={p[k]} />
                ))}
                <Records name="quality" items={p.quality} />
                {extra.map((k) => (
                  <Element key={k} name={k} value={p[k]} />
                ))}
              </Stack>
            </CollapsibleItem>
          );
        })}
      </Collapsibles>
    </Stack>
  );
}

/** Each quality rule, with the property it is on where it is on one. */
function qualityRules(contract: OdcsContract): OdcsRecord[] {
  return (contract.schema ?? []).flatMap((object) => [
    ...(object.quality ?? []),
    ...(object.properties ?? []).flatMap((p) =>
      (p.quality ?? []).map((rule) => ({ property: p.name, ...rule })),
    ),
  ]);
}

export function ContractView({
  contract,
  tableId,
  promises,
  consumers,
}: {
  contract: OdcsContract;
  tableId: number;
  /** The product's promises on this port (REQ-2132); null while the product's lineage is read. */
  promises: ProductDqEntry[] | null;
  /** Who reads this port, one hop downstream; null when the reader is not shown them (REQ-2160). */
  consumers: ProductConsumer[] | null;
}) {
  const { t } = useTranslation();
  const status = t(`dataProductsTab.contract.status.${contract.status}`);
  const notDeclared = t("dataProductsTab.contract.notDeclared");
  const object = contract.schema?.[0];
  const properties = object?.properties ?? [];
  const rules = qualityRules(contract);
  const sla = contract.slaProperties ?? [];
  const price = contract.price;
  const custom = contract.customProperties ?? [];
  const tags = contract.tags ?? [];
  const count = (key: string, n: number) =>
    t(`dataProductsTab.contract.summary.${key}`, { count: n });
  const summaries: Record<Section, string> = {
    support: [
      count("supportChannels", contract.support?.length ?? 0),
      count("roles", contract.roles?.length ?? 0),
    ].join(" · "),
    servers: contract.servers?.length
      ? contract.servers.map((s) => String(s.server ?? s.type)).join(", ")
      : notDeclared,
    pricing: price ? record(price) : notDeclared,
    custom: [count("customProperties", custom.length), count("tags", tags.length)].join(" · "),
  };
  const bodies: Record<Section, ReactNode> = {
    support: (
      <>
        <Records name="support" items={contract.support} />
        <Element name="team.name" value={contract.team?.name} />
        <Element name="team.description" value={contract.team?.description} />
        <Records name="team.members" items={contract.team?.members} />
        <Records name="roles" items={contract.roles} />
      </>
    ),
    servers: <Records name="servers" items={contract.servers} />,
    pricing: (
      <>
        <Element name="priceAmount" value={price?.priceAmount} />
        <Element name="priceCurrency" value={price?.priceCurrency} />
        <Element name="priceUnit" value={price?.priceUnit} />
      </>
    ),
    custom: (
      <>
        <Records
          name="customProperties"
          items={custom.map((c) => `${c.property}: ${shown(c.value)}`)}
        />
        <Element name="tags" value={tags} />
      </>
    ),
  };
  const card = (key: string) => ({
    section: `contract.${key}`,
    testId: `data-contract-${tableId}-${key}`,
    label: t(`dataProductsTab.contract.card.${key}`),
  });
  return (
    <div
      style={{ columnWidth: "320px", columnGap: "var(--mantine-spacing-sm)" }}
      data-testid={`data-contract-${tableId}`}
    >
      <DetailCard {...card("details")}>
        <Stack gap={2}>
          <Element name="id" value={contract.id} />
          <Element name="name" value={contract.name} />
          <Element name="status" value={status} />
          <Element name="domain" value={contract.domain} />
          <Element name="dataProduct" value={contract.dataProduct} />
          <Element name="tenant" value={contract.tenant} />
          <Element name="description.purpose" value={contract.description?.purpose} />
          <Element name="description.limitations" value={contract.description?.limitations} />
          <Element name="description.usage" value={contract.description?.usage} />
          <Collapsibles id={`data-product.contract.${tableId}`}>
            {DETAIL_SECTIONS.map((section) => (
              <CollapsibleItem
                key={section}
                value={section}
                defaultOpen={false}
                mountWhenOpen
                testId={`data-contract-${tableId}-${section}`}
                controlTestId={`data-contract-${tableId}-${section}-toggle`}
                title={
                  <Group gap={8} wrap="nowrap" style={{ minWidth: 0 }}>
                    <Text size="sm" fw={500} style={{ flexShrink: 0 }}>
                      {t(`dataProductsTab.contract.section.${section}`)}
                    </Text>
                    <Text
                      size="sm"
                      c="dimmed"
                      truncate="end"
                      style={{ minWidth: 0 }}
                      data-testid={`data-contract-${tableId}-${section}-summary`}
                    >
                      {summaries[section]}
                    </Text>
                  </Group>
                }
              >
                <Stack gap={2}>{bodies[section]}</Stack>
              </CollapsibleItem>
            ))}
          </Collapsibles>
        </Stack>
      </DetailCard>
      <DetailCard {...card("fields")}>
        <Stack gap={2}>
          <Element name="name" value={object?.name} />
          <Element name="physicalName" value={object?.physicalName} />
          <Element name="physicalType" value={object?.physicalType} />
          <Element name="description" value={object?.description} />
          <Element name="businessName" value={object?.businessName} />
          <Element name="dataGranularityDescription" value={object?.dataGranularityDescription} />
          <Records name="relationships" items={object?.relationships} />
          <Element name="tags" value={object?.tags} />
          <Properties
            properties={properties}
            tableId={tableId}
            testId={`data-contract-columns-${tableId}`}
          />
        </Stack>
      </DetailCard>
      <DetailCard {...card("sla")}>
        <Stack gap={2}>
          <Element name="slaDefaultElement" value={contract.slaDefaultElement} />
          <Records name="slaProperties" items={sla} testId={`data-contract-sla-${tableId}`} />
        </Stack>
      </DetailCard>
      <DetailCard {...card("quality")}>
        <Stack gap={4}>
          {/* REQ-2132: the product's promises on this port, as its Output Ports card shows them. */}
          {promises && (
            <ClaimsCallout
              kind="promises"
              checks={promises}
              testId={`data-contract-promises-${tableId}`}
            />
          )}
          <Records name="quality" items={rules} />
        </Stack>
      </DetailCard>
      {consumers && (
        <DetailCard {...card("consumers")}>
          <ConsumersList consumers={consumers} testId={`data-contract-consumers-${tableId}`} />
        </DetailCard>
      )}
      <DetailCard {...card("versions")}>
        <Stack gap={2}>
          <Element name="version" value={contract.version} />
          <Element name="status" value={status} />
          <Element name="contractCreatedTs" value={contract.contractCreatedTs} />
        </Stack>
      </DetailCard>
    </div>
  );
}
