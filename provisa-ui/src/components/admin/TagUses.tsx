// Copyright (c) 2026 Kenneth Stott
// Canary: 5687d257-3b75-4131-9bb8-f708d6f2a8b9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { Group, Stack, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { ContextPlace, ContextTagUse } from "../../api/contextViews";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";
import { EntityLink, EntityPageLink } from "../EntityLink";
import { FilterInput } from "./FilterInput";
import { matchesQuery } from "../../lib/searchItems";
import { placeSource } from "../../lib/tagUseSummary";

// REQ-2208 (amended 2026-11-26): the Tags list shows one line per tag with counts; the detail panel
// lists where it is applied and what depends on it, grouped and collapsible, each name a link.

/** The place's name within its source: ``schema.table`` or ``schema.table.column``. */
function nameInSource(p: ContextPlace): string {
  const inSource = p.tableRef.slice(p.tableRef.indexOf(".") + 1);
  return p.column === null ? inSource : `${inSource}.${p.column}`;
}

/** The places of one object kind, grouped by source: the source a link, then each place a link. */
function PlacesBySource({ places, testId }: { places: ContextPlace[]; testId: string }) {
  const bySource = new Map<string, ContextPlace[]>();
  for (const p of places) {
    const source = placeSource(p);
    bySource.set(source, [...(bySource.get(source) ?? []), p]);
  }
  return (
    <Stack gap="xs">
      {[...bySource.entries()]
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([source, held]) => (
          <Stack gap={2} key={source} data-testid={`${testId}-source-${source}`}>
            <Text size="sm" fw={500}>
              <EntityLink kind="source" id={source} testId={`${testId}-source-link`}>
                {source}
              </EntityLink>
            </Text>
            <Group gap={8} pl="sm">
              {held.map((p) => (
                <Text size="sm" component="span" key={nameInSource(p)}>
                  {p.column === null ? (
                    <EntityLink kind="table" id={p.tableId} testId={`${testId}-place`}>
                      {nameInSource(p)}
                    </EntityLink>
                  ) : (
                    <EntityLink
                      kind="column"
                      id={p.column}
                      table={p.tableId}
                      testId={`${testId}-place`}
                    >
                      {nameInSource(p)}
                    </EntityLink>
                  )}
                </Text>
              ))}
            </Group>
          </Stack>
        ))}
    </Stack>
  );
}

/** The panel's Where applied: tables, then columns, each grouped by source; those the viewer does
 * not read are counted. */
export function TagWhereApplied({ tagId, use }: { tagId: string; use: ContextTagUse }) {
  const { t } = useTranslation();
  const testId = `tags-where-${tagId}`;
  const tables = use.usages.filter((p) => p.column === null);
  const columns = use.usages.filter((p) => p.column !== null);
  return (
    <Stack gap="xs" data-testid={testId}>
      <Text size="sm" fw={500} c="dimmed">
        {t("contextViews.colWhereApplied")}
      </Text>
      {use.usages.length === 0 && use.hiddenUsages === 0 && (
        <Text size="sm" c="dimmed">
          {t("contextViews.nowhere")}
        </Text>
      )}
      {use.usages.length > 0 && (
        <Collapsibles id="context-tags.where">
          {tables.length > 0 && (
            <CollapsibleItem
              value="tables"
              title={t("contextViews.groupTables", { count: tables.length })}
              controlTestId={`${testId}-tables-toggle`}
              testId={`${testId}-tables`}
            >
              <PlacesBySource places={tables} testId={`${testId}-tables`} />
            </CollapsibleItem>
          )}
          {columns.length > 0 && (
            <CollapsibleItem
              value="columns"
              title={t("contextViews.groupColumns", { count: columns.length })}
              controlTestId={`${testId}-columns-toggle`}
              testId={`${testId}-columns`}
            >
              <PlacesBySource places={columns} testId={`${testId}-columns`} />
            </CollapsibleItem>
          )}
        </Collapsibles>
      )}
      {use.hiddenUsages > 0 && (
        <Text size="sm" c="dimmed" data-testid={`${testId}-hidden`}>
          {t("contextViews.hiddenPlaces", { count: use.hiddenUsages })}
        </Text>
      )}
    </Stack>
  );
}

/** The classification kinds that assign the tag, filtered by the kind's name. */
function ClassificationGroup({
  tagId,
  kinds,
}: {
  tagId: string;
  kinds: ContextTagUse["classificationKinds"];
}) {
  const { t } = useTranslation();
  const [filter, setFilter] = useState("");
  // REQ-2147 (amended 2026-11-25): a custom kind is named by the org's label.
  const label = (k: ContextTagUse["classificationKinds"][number]) =>
    k.custom ? k.label : t(`classificationKinds.kind.${k.id}`);
  const shown = kinds.filter((k) => matchesQuery([k.id, label(k)], filter));
  return (
    <Stack gap="xs">
      <FilterInput
        value={filter}
        onChange={setFilter}
        placeholder={t("contextViews.filterClassifications")}
        testId={`tags-classifications-filter-${tagId}`}
        remember={false}
      />
      <Group gap={8}>
        {shown.map((k) => (
          <Text size="sm" component="span" key={k.id}>
            <EntityLink
              kind="classification"
              id={k.id}
              testId={`tags-classification-${tagId}-${k.id}`}
            >
              {label(k)}
            </EntityLink>
          </Text>
        ))}
      </Group>
    </Stack>
  );
}

/** The panel's Depends on it, grouped by why the dependent depends on the tag: each group's count
 * in its header; the classifications, many on a tag like pii, closed until opened. */
export function TagDependents({ tagId, use }: { tagId: string; use: ContextTagUse }) {
  const { t } = useTranslation();
  const testId = `tags-dependents-${tagId}`;
  const none = use.rowFilters === 0 && use.classificationKinds.length === 0;
  return (
    <Stack gap="xs" data-testid={testId}>
      <Text size="sm" fw={500} c="dimmed">
        {t("contextViews.colDependents")}
      </Text>
      {none ? (
        <Text size="sm" c="dimmed">
          {t("contextViews.noDependents")}
        </Text>
      ) : (
        <Collapsibles id="context-tags.dependents">
          {use.classificationKinds.length > 0 && (
            <CollapsibleItem
              value="classification"
              title={t("contextViews.groupClassification", {
                count: use.classificationKinds.length,
              })}
              defaultOpen={false}
              controlTestId={`${testId}-classification-toggle`}
              testId={`${testId}-classification`}
            >
              <ClassificationGroup tagId={tagId} kinds={use.classificationKinds} />
            </CollapsibleItem>
          )}
          {use.rowFilters > 0 && (
            <CollapsibleItem
              value="rowRules"
              title={t("contextViews.groupRowRules", { count: use.rowFilters })}
              controlTestId={`${testId}-row-rules-toggle`}
              testId={`${testId}-row-rules`}
            >
              {/* The read names no rule, only counts them: the row rules page lists them. */}
              <Text size="sm">
                <EntityPageLink href="/security/rls" testId={`${testId}-row-rules-link`}>
                  {t("contextViews.rowRuleCount", { count: use.rowFilters })}
                </EntityPageLink>
              </Text>
            </CollapsibleItem>
          )}
        </Collapsibles>
      )}
    </Stack>
  );
}
