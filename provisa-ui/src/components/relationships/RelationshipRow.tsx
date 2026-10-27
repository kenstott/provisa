// Copyright (c) 2026 Kenneth Stott
// Canary: 937af58d-09ba-4ca8-97f2-baf0fb7e0ced
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { useTranslation } from "react-i18next";
import { Badge, Group, Text } from "@mantine/core";
import type { Relationship } from "../../types/admin";
import { relationshipTargetLabel } from "./relationship-types";
import { TagControl } from "../TagControl";
import { ListRow } from "../list/ListTable";

interface RelationshipRowProps {
  rel: Relationship;
  /** This row's detail is the one the page's right-hand panel shows. */
  selected: boolean;
  onToggle: () => void;
  tableDomainById: Record<string, string>;
  domainsEnabled: boolean;
}

export function RelationshipRow({
  rel: r,
  selected,
  onToggle,
  tableDomainById,
  domainsEnabled,
}: RelationshipRowProps) {
  const { t } = useTranslation();

  const targetLabel = relationshipTargetLabel(r, tableDomainById, domainsEnabled);

  return (
    <React.Fragment>
      <ListRow
        onClick={onToggle}
        selected={selected}
        // REQ-2010: a demo's tour points at a relationship by its alias.
        data-tour-relationship={r.alias ?? undefined}
        role="button"
        tabIndex={0}
        aria-expanded={selected}
        aria-label={t("relationshipRow.toggleDetails")}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            onToggle();
          }
        }}
      >
        {domainsEnabled && (
          <td style={{ whiteSpace: "nowrap" }}>{r.sourceDomainId || t("relationshipRow.none")}</td>
        )}
        <td style={{ whiteSpace: "nowrap" }}>
          <Group gap="0.4rem" wrap="nowrap">
            {r.autoSuggested && (
              <Badge
                title={t("relationshipRow.autoTrackedTitle")}
                size="xs"
                color="gray"
                variant="filled"
                style={{ flexShrink: 0 }}
              >
                {t("relationshipRow.fkBadge")}
              </Badge>
            )}
            {r.viaTableName && (
              <Badge
                title={t("relationshipRow.junctionBadgeTitle", { table: r.viaTableName })}
                size="xs"
                color="grape"
                variant="filled"
                style={{ flexShrink: 0 }}
              >
                {t("relationshipRow.junctionBadge")}
              </Badge>
            )}
            <span>{`${r.sourceTableName}.${r.sourceColumn}`}</span>
            <TagControl objectType="relationship" relationshipId={String(r.id)} />
          </Group>
        </td>
        <td style={{ whiteSpace: "nowrap" }}>{targetLabel}</td>
        <td>
          <div style={{ fontSize: "0.8rem", lineHeight: 1.4 }}>
            <div>
              <Text span c="dimmed">
                {t("relationshipRow.gqlLabel")}
              </Text>{" "}
              <code>{r.graphqlAlias ?? t("relationshipRow.none")}</code>
            </div>
            <div>
              <Text span c="dimmed">
                {t("relationshipRow.cqlLabel")}
              </Text>{" "}
              <code>
                {/* REQ-1586: a junction-backed edge is exposed under its nomination, not its
                    alias, so the row shows the type a Cypher pattern actually matches. */}
                {r.viaTableName
                  ? (r.cypherType ?? t("relationshipRow.none"))
                  : (r.alias ?? (
                      <Text span c="dimmed" fs="italic">
                        {r.computedCypherAlias ?? t("relationshipRow.none")}
                      </Text>
                    ))}
              </code>
            </div>
          </div>
        </td>
        <td>{r.cardinality}</td>
        <td>{r.materialize ? t("relationshipRow.yes") : t("relationshipRow.no")}</td>
        <td>{r.materialize ? r.refreshInterval : t("relationshipRow.none")}</td>
      </ListRow>
    </React.Fragment>
  );
}
