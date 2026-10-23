// Copyright (c) 2026 Kenneth Stott
// Canary: 48650ebd-6c7f-4190-af45-4e6681f5d558
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130: a term's role in the ontology, by what grounds it: Class (tables), Property (columns),
// Concept (neither). Its tip says what the role means.

import { useTranslation } from "react-i18next";
import { Badge, Group } from "@mantine/core";
import { termRole, type GlossaryTermSummary, type TermRole } from "../../api/glossary";
import { Tip } from "../Tip";
import { toReview } from "./termFindings";

const COLOUR: Record<TermRole, string> = { class: "grape", property: "blue", concept: "gray" };

export function TermRoleBadge({
  term,
  dangling = false,
  testId,
}: {
  term: { table_count: number; ref_count: number };
  /** A concept connected to no term that names data: said in red. */
  dangling?: boolean;
  testId?: string;
}) {
  const { t } = useTranslation();
  const role = termRole(term);
  return (
    <Tip text={dangling ? t("glossaryTab.danglingHelp") : t(`glossaryTab.roleHelp_${role}`)}>
      <Badge
        size="xs"
        variant="light"
        color={dangling ? "red" : COLOUR[role]}
        data-testid={testId}
        data-role={role}
        data-dangling={dangling || undefined}
      >
        {t(`glossaryTab.role_${role}`)}
      </Badge>
    </Tip>
  );
}

/** A term's badges in the term list: its role, then its standing, then its column count. */
export function TermListBadges({ term }: { term: GlossaryTermSummary }) {
  const { t } = useTranslation();
  return (
    <Group gap={4} wrap="nowrap">
      <TermRoleBadge
        term={term}
        dangling={termRole(term) === "concept" && !term.grounded}
        testId={`glossary-role-${term.id}`}
      />
      {/* What it needs looked at is in its panel's To review section; the list marks it only. */}
      {toReview(term) && (
        <Badge size="xs" variant="light" color="orange" data-testid={`glossary-review-${term.id}`}>
          {t("glossaryTab.toReview")}
        </Badge>
      )}
      {term.deprecated && (
        <Badge size="xs" color="gray" variant="light">
          {t("glossaryTab.deprecated")}
        </Badge>
      )}
      {term.retired && (
        <Badge size="xs" color="orange" variant="light">
          {t("glossaryTab.retired")}
        </Badge>
      )}
      {!term.live && !term.retired && !term.deprecated && (
        <Badge size="xs" color="yellow" variant="light">
          {t("glossaryTab.proposed")}
        </Badge>
      )}
      {termRole(term) === "property" && (
        <Badge size="xs" variant="default">
          {term.ref_count}
        </Badge>
      )}
    </Group>
  );
}
