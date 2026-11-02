// Copyright (c) 2026 Kenneth Stott
// Canary: 212a9f45-9e3d-49c5-ba81-448e76cce1fa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-1949, REQ-2193

// One entity set in the OData explorer's detail panel: its entity type as $metadata declares it,
// a builder of the read's system query options, the URL they write (to copy), and the read's rows.

import { useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  Badge,
  Button,
  Checkbox,
  Code,
  Group,
  MultiSelect,
  NumberInput,
  Stack,
  Text,
  TextInput,
} from "@mantine/core";
import { Play } from "lucide-react";
import { CopyButton } from "../../components/CopyButton";
import { DetailFields } from "../../components/detail/DetailFields";
import { FormChecks, FormField, FormLayout } from "../../components/form/FormLayout";
import type { ODataEntitySet, ODataNavigation } from "./metadata";
import { buildQueryUrl, EMPTY_QUERY, type ODataQuery } from "./queryUrl";
import { ODataResults } from "./ODataResults";
import { usePagedRead } from "./usePagedRead";

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <Stack gap={6}>
      <Text size="sm" fw={600}>
        {title}
      </Text>
      {children}
    </Stack>
  );
}

function typeOf(precision: number | null, scale: string | null, type: string): string {
  if (precision === null) return type;
  return scale === null ? `${type}(${precision})` : `${type}(${precision}, ${scale})`;
}

function NavigationTarget({ nav }: { nav: ODataNavigation }) {
  const { t } = useTranslation();
  const target = nav.targetSet ?? nav.targetType;
  return (
    <Group gap={6} wrap="wrap">
      <Text size="sm" ff="monospace">
        {nav.collection
          ? t("odataPage.navigationMany", { target })
          : t("odataPage.navigationOne", { target })}
      </Text>
      {nav.constraints.map((c) => (
        <Text key={c.property} size="xs" c="dimmed" ff="monospace">
          {c.property} = {target}.{c.referenced}
        </Text>
      ))}
    </Group>
  );
}

/** `name` and `name desc` for each property: the $orderby keys offered. */
function orderChoices(names: string[], ascending: string, descending: string) {
  return names.flatMap((n) => [
    { value: n, label: `${n} ${ascending}` },
    { value: `${n} desc`, label: `${n} ${descending}` },
  ]);
}

export function EntitySetPanel({
  entity,
  role,
  root,
}: {
  entity: ODataEntitySet;
  role: string;
  /** The service root as a client reaches it (absolute), which the shown URL starts from. */
  root: string;
}) {
  const { t } = useTranslation();
  const [query, setQuery] = useState<ODataQuery>(EMPTY_QUERY);
  const read = usePagedRead(role);
  const set = <K extends keyof ODataQuery>(key: K, value: ODataQuery[K]) =>
    setQuery((q) => ({ ...q, [key]: value }));
  const url = buildQueryUrl(root, entity.name, query);

  const propertyNames = entity.properties.map((p) => p.name);
  const orderValues = query.orderBy.map((k) => (k.descending ? `${k.property} desc` : k.property));
  // A property is ordered by once: its other direction is not offered beside a chosen one.
  const ordered = new Set(query.orderBy.map((k) => k.property));
  const orderData = orderChoices(
    propertyNames,
    t("odataPage.ascending"),
    t("odataPage.descending"),
  ).filter((c) => orderValues.includes(c.value) || !ordered.has(c.value.split(" ")[0]));

  const typeFields = [
    { label: t("odataPage.entityType"), value: entity.typeName, testId: "odata-entity-type" },
    {
      label: t("odataPage.key"),
      value: entity.key.length ? entity.key.join(", ") : t("odataPage.noKey"),
      testId: "odata-entity-key",
    },
  ];
  const propertyFields = entity.properties.map((p) => ({
    label: p.name,
    testId: `odata-property-${p.name}`,
    value: (
      <Group gap={6} wrap="nowrap">
        <Text size="sm" ff="monospace">
          {typeOf(p.precision, p.scale, p.type)}
        </Text>
        {entity.key.includes(p.name) && (
          <Badge size="xs" variant="light">
            {t("odataPage.keyBadge")}
          </Badge>
        )}
      </Group>
    ),
  }));
  const navigationFields = entity.navigations.map((n) => ({
    label: n.name,
    testId: `odata-navigation-${n.name}`,
    value: <NavigationTarget nav={n} />,
  }));

  return (
    <Stack gap="md" data-testid={`odata-set-${entity.name}`}>
      <DetailFields fields={typeFields} />
      <Section title={t("odataPage.properties")}>
        <DetailFields fields={propertyFields} testId="odata-properties" />
      </Section>
      {navigationFields.length > 0 && (
        <Section title={t("odataPage.navigations")}>
          <DetailFields fields={navigationFields} testId="odata-navigations" />
        </Section>
      )}

      <Section title={t("odataPage.queryTitle")}>
        <FormLayout
          component="form"
          onSubmit={(e) => {
            e.preventDefault();
            read.run(url);
          }}
          data-testid="odata-query-form"
        >
          <FormField size="long" hint={t("odataPage.filterHint")}>
            <TextInput
              size="xs"
              label="$filter"
              ff="monospace"
              placeholder={t("odataPage.filterPlaceholder")}
              value={query.filter}
              onChange={(e) => set("filter", e.currentTarget.value)}
              data-testid="odata-filter"
            />
          </FormField>
          <FormField size="long" hint={t("odataPage.selectHint")}>
            <MultiSelect
              size="xs"
              label="$select"
              data={propertyNames}
              value={query.select}
              onChange={(v) => set("select", v)}
              searchable
              clearable
              data-testid="odata-select"
            />
          </FormField>
          {entity.navigations.length > 0 && (
            <FormField size="long" hint={t("odataPage.expandHint")}>
              <MultiSelect
                size="xs"
                label="$expand"
                data={entity.navigations.map((n) => n.name)}
                value={query.expand}
                onChange={(v) => set("expand", v)}
                clearable
                data-testid="odata-expand"
              />
            </FormField>
          )}
          <FormField size="long" hint={t("odataPage.orderByHint")}>
            <MultiSelect
              size="xs"
              label="$orderby"
              data={orderData}
              value={orderValues}
              onChange={(v) =>
                set(
                  "orderBy",
                  v.map((s) => ({ property: s.split(" ")[0], descending: s.endsWith(" desc") })),
                )
              }
              searchable
              clearable
              data-testid="odata-orderby"
            />
          </FormField>
          <FormField size="short">
            <NumberInput
              size="xs"
              label="$top"
              min={0}
              allowDecimal={false}
              value={query.top ?? ""}
              onChange={(v) => set("top", typeof v === "number" ? v : null)}
              data-testid="odata-top"
            />
          </FormField>
          <FormField size="short">
            <NumberInput
              size="xs"
              label="$skip"
              min={0}
              allowDecimal={false}
              value={query.skip ?? ""}
              onChange={(v) => set("skip", typeof v === "number" ? v : null)}
              data-testid="odata-skip"
            />
          </FormField>
          <FormChecks>
            <Checkbox
              size="xs"
              label={t("odataPage.countLabel")}
              checked={query.count}
              onChange={(e) => set("count", e.currentTarget.checked)}
              data-testid="odata-count"
            />
          </FormChecks>
        </FormLayout>
      </Section>

      <Section title={t("odataPage.queryUrl")}>
        <Group gap="xs" wrap="nowrap" align="flex-start">
          <Code block className="odata-url" data-testid="odata-url">
            {url}
          </Code>
          <CopyButton text={url} hint={t("odataPage.copyUrl")} />
        </Group>
        <Group gap="xs">
          <Button
            size="xs"
            leftSection={<Play size={14} />}
            onClick={() => read.run(url)}
            loading={read.loading}
            data-testid="odata-run"
          >
            {t("odataPage.run")}
          </Button>
        </Group>
      </Section>

      <ODataResults read={read} setName={entity.name} />
    </Stack>
  );
}
