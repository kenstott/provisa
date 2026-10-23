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

const COLOUR: Record<TermRole, string> = { class: "grape", property: "blue", concept: "gray" };

export function TermRoleBadge({
  term,
  testId,
}: {
  term: { table_count: number; ref_count: number };
  testId?: string;
}) {
  const { t } = useTranslation();
  const role = termRole(term);
  return (
    <Tip text={t(`glossaryTab.roleHelp_${role}`)}>
      <Badge size="xs" variant="light" color={COLOUR[role]} data-testid={testId} data-role={role}>
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
      <TermRoleBadge term={term} testId={`glossary-role-${term.id}`} />
      {term.identity_gap && (
        <Tip text={t("glossaryTab.identityGapHelp")}>
          <Badge size="xs" variant="light" color="orange" data-testid={`glossary-gap-${term.id}`}>
            {t("glossaryTab.identityGap")}
          </Badge>
        </Tip>
      )}
      {term.is_abstract && (
        <Badge
          size="xs"
          variant="light"
          color={term.grounded ? undefined : "red"}
          data-testid={term.grounded ? undefined : `glossary-dangling-${term.id}`}
        >
          {t("glossaryTab.abstract")}
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
      {!term.is_abstract && (
        <Badge size="xs" variant="default">
          {term.ref_count}
        </Badge>
      )}
    </Group>
  );
}
