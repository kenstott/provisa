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
import { fetchGlossaryTermByRef, type GlossaryTermDetail } from "../../api/glossary";
import { columnGlossaryCache, columnGlossaryKey } from "./columnGlossaryCache";

interface ColumnGlossaryHoverProps {
  tableId: number;
  columnName: string;
  children: ReactNode;
}

export function ColumnGlossaryHover({ tableId, columnName, children }: ColumnGlossaryHoverProps) {
  const { t } = useTranslation();
  const [term, setTerm] = useState<GlossaryTermDetail | null>(null);
  const [hovered, setHovered] = useState(false);

  const handleEnter = () => {
    setHovered(true);
    const key = columnGlossaryKey(tableId, columnName);
    if (columnGlossaryCache.has(key)) {
      setTerm(columnGlossaryCache.get(key) ?? null);
      return;
    }
    // No .catch: 404 arrives as null by the fetchGlossaryTermByRef contract;
    // any other failure is a real error and must surface, not be swallowed.
    void fetchGlossaryTermByRef(tableId, columnName).then((detail) => {
      columnGlossaryCache.set(key, detail);
      setTerm(detail);
    });
  };

  const edges = term
    ? [
        ...term.edges_out.map((e) => ({ ...e, reverse: false })),
        ...term.edges_in.map((e) => ({ ...e, reverse: true })),
      ]
    : [];

  return (
    <Popover
      opened={hovered && term !== null}
      position="bottom-start"
      withArrow
      shadow="md"
      transitionProps={{ duration: 0 }}
    >
      <Popover.Target>
        <span onMouseEnter={handleEnter} onMouseLeave={() => setHovered(false)}>
          {children}
        </span>
      </Popover.Target>
      {term && (
        <Popover.Dropdown maw={340} px="0.75rem" py="0.5rem">
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
              {t("glossaryTab.hoverExpertsLabel")} {term.experts.map((e) => e.user_id).join(", ")}
            </Text>
          )}
          {edges.length > 0 && (
            <Text fz="0.7rem" c="dimmed" mt={2}>
              {edges
                .map(
                  (e) =>
                    `${
                      e.reverse
                        ? t(`glossaryTab.rel_${e.rel_type}_reverse`, {
                            defaultValue: t(`glossaryTab.rel_${e.rel_type}`),
                          })
                        : t(`glossaryTab.rel_${e.rel_type}`)
                    }: ${e.name}`,
                )
                .join(" · ")}
            </Text>
          )}
        </Popover.Dropdown>
      )}
    </Popover>
  );
}
