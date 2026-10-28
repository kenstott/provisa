// Copyright (c) 2026 Kenneth Stott
// Canary: ff511d1f-2c93-40ca-84b7-d92d048c2720
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a product member's contract, its output port, as the product's panel shows it: every
// ODCS 3.1 element, declared or derived (REQ-2118), in the standard's sections as expanding rows,
// each collapsed to a one-line summary until opened. An element nothing provides is shown as not
// declared. Elements are named by their ODCS names, the standard's own vocabulary.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Badge, Group, Stack, Table, Text } from "@mantine/core";
import type { OdcsContract, OdcsProperty, OdcsRecord } from "../../api/dataProductContracts";
import { Collapsibles, CollapsibleItem } from "../../components/collapsible/Collapsible";
import { ListCell } from "../../components/list/ListCell";

const CONTRACT_SECTIONS = [
  "fundamentals",
  "schema",
  "quality",
  "sla",
  "support",
  "servers",
  "pricing",
  "custom",
] as const;
type Section = (typeof CONTRACT_SECTIONS)[number];

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

const yes = (v: boolean | undefined, mark: string) => (v ? mark : "");
/** The schema table's columns shown whole; the rest are cut to one line. */
const WHOLE = new Set([
  "name",
  "logicalType",
  "required",
  "unique",
  "primaryKey",
  "classification",
]);

/** The schema's properties: one row each, a single line of text tall. */
function Properties({ properties, testId }: { properties: OdcsProperty[]; testId: string }) {
  const { t } = useTranslation();
  const mark = t("dataProductsTab.contract.yes");
  const heads = [
    "name",
    "logicalType",
    "physicalType",
    "required",
    "unique",
    "primaryKey",
    "classification",
    "description",
    "examples",
  ];
  return (
    <Table
      withTableBorder
      striped
      fz="sm"
      horizontalSpacing="xs"
      data-testid={testId}
      className="contract-properties"
      style={{ lineHeight: "var(--mantine-line-height-sm)" }}
    >
      <Table.Thead>
        <Table.Tr>
          {heads.map((h) => (
            <Table.Th key={h} ff="monospace" fz="xs" fw={500}>
              {h}
            </Table.Th>
          ))}
        </Table.Tr>
      </Table.Thead>
      <Table.Tbody>
        {properties.map((p) => {
          const cells: [string, string][] = [
            ["name", p.name],
            ["logicalType", p.logicalType ?? ""],
            ["physicalType", p.physicalType ?? ""],
            ["required", yes(p.required, mark)],
            ["unique", yes(p.unique, mark)],
            ["primaryKey", yes(p.primaryKey, mark)],
            ["classification", p.classification ?? ""],
            ["description", p.description ?? ""],
            ["examples", p.examples ? shown(p.examples) : ""],
          ];
          return (
            <Table.Tr key={p.name}>
              {/* Every cell is one line, so the row is one line tall: a name, a type or a flag is
                  shown whole (the table scrolls in its card), longer text cut with an ellipsis. */}
              {cells.map(([k, v]) => (
                <Table.Td
                  key={k}
                  ff={k.endsWith("Type") || k === "name" ? "monospace" : undefined}
                  style={WHOLE.has(k) ? { whiteSpace: "nowrap" } : undefined}
                >
                  {WHOLE.has(k) ? v : <ListCell text={v} oneLine />}
                </Table.Td>
              ))}
            </Table.Tr>
          );
        })}
      </Table.Tbody>
    </Table>
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

export function ContractView({ contract, tableId }: { contract: OdcsContract; tableId: number }) {
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
  const summaries: Record<Section, string> = {
    fundamentals: [`v${contract.version}`, status, contract.domain].filter(Boolean).join(" · "),
    schema: t("dataProductsTab.contract.summary.schema", {
      object: object?.physicalName ?? object?.name ?? notDeclared,
      n: properties.length,
    }),
    quality: t("dataProductsTab.contract.summary.quality", { n: rules.length }),
    sla: t("dataProductsTab.contract.summary.sla", { n: sla.length }),
    support: t("dataProductsTab.contract.summary.support", {
      channels: contract.support?.length ?? 0,
      roles: contract.roles?.length ?? 0,
    }),
    servers: contract.servers?.length
      ? contract.servers.map((s) => String(s.server ?? s.type)).join(", ")
      : notDeclared,
    pricing: price ? record(price) : notDeclared,
    custom: t("dataProductsTab.contract.summary.custom", {
      props: custom.length,
      tags: tags.length,
    }),
  };
  const bodies: Record<Section, ReactNode> = {
    fundamentals: (
      <>
        <Element name="id" value={contract.id} />
        <Element name="name" value={contract.name} />
        <Element name="version" value={contract.version} />
        <Element name="status" value={status} />
        <Element name="domain" value={contract.domain} />
        <Element name="dataProduct" value={contract.dataProduct} />
        <Element name="tenant" value={contract.tenant} />
        <Element name="description.purpose" value={contract.description?.purpose} />
        <Element name="description.limitations" value={contract.description?.limitations} />
        <Element name="description.usage" value={contract.description?.usage} />
        <Element name="contractCreatedTs" value={contract.contractCreatedTs} />
      </>
    ),
    schema: (
      <>
        <Element name="name" value={object?.name} />
        <Element name="physicalName" value={object?.physicalName} />
        <Element name="physicalType" value={object?.physicalType} />
        <Element name="description" value={object?.description} />
        <Element name="businessName" value={object?.businessName} />
        <Element name="dataGranularityDescription" value={object?.dataGranularityDescription} />
        <Records name="relationships" items={object?.relationships} />
        <Element name="tags" value={object?.tags} />
        <Properties properties={properties} testId={`data-contract-columns-${tableId}`} />
      </>
    ),
    quality: <Records name="quality" items={rules} />,
    sla: (
      <>
        <Element name="slaDefaultElement" value={contract.slaDefaultElement} />
        <Records name="slaProperties" items={sla} testId={`data-contract-sla-${tableId}`} />
      </>
    ),
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
  return (
    <Stack gap={4} data-testid={`data-contract-${tableId}`}>
      <Group gap="xs" wrap="wrap">
        <Text size="sm" ff="monospace" fw={500}>
          {object?.physicalName ?? contract.name}
        </Text>
        <Badge size="sm" variant="light" data-testid={`data-contract-version-${tableId}`}>
          {t("dataProductsTab.contract.versionBadge", { version: contract.version })}
        </Badge>
        <Badge
          size="sm"
          variant="outline"
          color={contract.status === "active" ? "green" : "gray"}
          data-testid={`data-contract-status-${tableId}`}
        >
          {status}
        </Badge>
      </Group>
      <Collapsibles id={`data-product.contract.${tableId}`}>
        {CONTRACT_SECTIONS.map((section) => (
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
  );
}
