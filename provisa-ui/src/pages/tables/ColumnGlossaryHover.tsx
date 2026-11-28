// Copyright (c) 2026 Kenneth Stott
// Canary: 7f2e9c14-5a8b-4d36-b1e0-9c4a7d2f6e83
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387: hovering a column name on the Tables surface pops a compact summary
// card of the column's glossary term (name, deprecation, definition, experts,
// related refs, related terms). Fetch is lazy — first hover per table_id+column_name — and
// both hits and misses (404 → null by API contract) are cached module-wide so
// repeated hovers never refetch.

import { useState, type ReactNode } from "react";
import { Badge, Group, Popover, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  fetchGlossaryTermByQueryName,
  fetchGlossaryTermByRef,
  type GlossaryTermDetail,
} from "../../api/glossary";
import { columnGlossaryCache, columnGlossaryKey } from "./columnGlossaryCache";

/** One attribute of a column, as the hover lists it. */
export interface ColumnDefinitionLine {
  label: string;
  value: string;
}

interface ColumnGlossaryHoverProps {
  /** Absent for a column of a table that is not registered yet: there is no glossary to read. */
  tableId?: number;
  columnName: string;
  /** ``columnName`` is the name the column's consumers query it by -- its SQL alias where one is
   *  set -- not its physical name: a data product's consumer view (REQ-2158, amended 2026-11-25). */
  queried?: boolean;
  /**
   * The column's full definition, shown read-only above its glossary term: every attribute the
   * editor holds, whichever tab it is edited on. Lines with no value are left out.
   */
  definition?: ColumnDefinitionLine[];
  children: ReactNode;
}

export function ColumnGlossaryHover({
  tableId,
  columnName,
  queried = false,
  definition,
  children,
}: ColumnGlossaryHoverProps) {
  const { t } = useTranslation();
  const [term, setTerm] = useState<GlossaryTermDetail | null>(null);
  const [hovered, setHovered] = useState(false);

  const handleEnter = () => {
    setHovered(true);
    if (tableId === undefined) return;
    const key = columnGlossaryKey(tableId, columnName, queried);
    if (columnGlossaryCache.has(key)) {
      setTerm(columnGlossaryCache.get(key) ?? null);
      return;
    }
    // No .catch: 404 arrives as null by the fetchGlossaryTermByRef contract;
    // any other failure is a real error and must surface, not be swallowed.
    const lookup = queried ? fetchGlossaryTermByQueryName : fetchGlossaryTermByRef;
    void lookup(tableId, columnName).then((detail) => {
      columnGlossaryCache.set(key, detail);
      setTerm(detail);
    });
  };

  const lines = (definition ?? []).filter((line) => line.value !== "");

  const edges = term
    ? [
        ...term.edges_out.map((e) => ({ ...e, reverse: false })),
        ...term.edges_in.map((e) => ({ ...e, reverse: true })),
      ]
    : [];

  return (
    <Popover
      opened={hovered && (term !== null || lines.length > 0)}
      position="bottom-start"
      withArrow
      shadow="md"
      transitionProps={{ duration: 0 }}
    >
      <Popover.Target>
        {/* Reached by keyboard too when it has a definition to show. */}
        <span
          onMouseEnter={handleEnter}
          onMouseLeave={() => setHovered(false)}
          onFocus={handleEnter}
          onBlur={() => setHovered(false)}
          tabIndex={definition === undefined ? undefined : 0}
          data-testid={definition === undefined ? undefined : `column-definition-${columnName}`}
        >
          {children}
        </span>
      </Popover.Target>
      {(term || lines.length > 0) && (
        <Popover.Dropdown maw={340} px="0.75rem" py="0.5rem">
          {lines.length > 0 && (
            <dl className="column-definition" data-testid="column-definition-card">
              {lines.map((line) => (
                <div key={line.label}>
                  <dt>{line.label}</dt>
                  <dd>{line.value}</dd>
                </div>
              ))}
            </dl>
          )}
          {term && (
            <>
              <Group gap={6} mb={2}>
                <Text fw={700} fz="0.85rem">
                  {term.name}
                </Text>
                {term.deprecated && (
                  <Badge size="xs" color="red" variant="light">
                    {t("glossaryTab.deprecated")}
                  </Badge>
                )}
              </Group>
              {term.definition ? (
                <Text fz="0.8rem">{term.definition}</Text>
              ) : (
                <Text fz="0.8rem" c="dimmed">
                  {t("glossaryTab.hoverNoDefinition")}
                </Text>
              )}
              {term.refs.length > 0 && (
                <Text fz="0.75rem" mt={4}>
                  {t("glossaryTab.hoverRefsLabel")}{" "}
                  {term.refs.map((r) => `${r.alias ?? r.table_name}.${r.column_name}`).join(", ")}
                </Text>
              )}
              {term.experts.length > 0 && (
                <Text fz="0.75rem" mt={4}>
                  {t("glossaryTab.hoverExpertsLabel")}{" "}
                  {term.experts.map((e) => e.user_id).join(", ")}
                </Text>
              )}
              {edges.length > 0 && (
                <Text fz="0.7rem" c="dimmed" mt={2}>
                  {edges
                    .map((e) => {
                      const label = t(`glossaryTab.rel_${e.rel_type}`);
                      // Direction is conveyed by word order, not a separate reverse verb
                      // (REQ-1590): "{connected} {rel}" for incoming, "{rel}: {connected}" outgoing.
                      return e.reverse ? `${e.name} ${label}` : `${label}: ${e.name}`;
                    })
                    .join(" · ")}
                </Text>
              )}
            </>
          )}
        </Popover.Dropdown>
      )}
    </Popover>
  );
}
