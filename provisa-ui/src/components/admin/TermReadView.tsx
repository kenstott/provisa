// Copyright (c) 2026 Kenneth Stott
// Canary: 7822d30c-3517-4263-8f1f-680c0f4b3639
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A term's panel read: the sections its Edit form has (TermEditForm.tsx), in the same order, each
// value shown and nothing changed here -- the name and definition, export and domains, what follows
// from its columns and tables, the columns it names, the tables it is grounded in, its
// relationships and its experts.

import { useTranslation } from "react-i18next";
import { Badge, Group, Stack, Table, Text, Title } from "@mantine/core";
import {
  addGlossaryEdge,
  removeGlossaryEdge,
  retypeGlossaryEdge,
  type GlossaryTermDetail,
} from "../../api/glossary";
import { DetailFields } from "../detail/DetailFields";
import { ListRow } from "../list/ListTable";
import { SortGroupTable } from "../list/SortGroupTable";
import { GlossaryGroundedTables } from "./GlossaryGroundedTables";
import { GlossaryRelationships } from "./GlossaryRelationships";
import { TermRoleBadge } from "./TermRoleBadge";

type Option = { value: string; label: string };

export function TermReadView({
  term,
  relTypeOptions,
  otherTermOptions,
  relLabel,
  navigateToTerm,
}: {
  term: GlossaryTermDetail;
  relTypeOptions: Option[];
  otherTermOptions: Option[];
  relLabel: (options: Option[], relType: string) => string;
  navigateToTerm: (termId: number) => void;
}) {
  const { t } = useTranslation();
  const yesNo = (v: boolean) => (v ? t("detailFields.yes") : t("detailFields.no"));
  return (
    <Stack gap="md" pb="md">
      <DetailFields
        testId="glossary-read-fields"
        fields={[
          { label: t("glossaryTab.nameLabel"), value: term.name },
          {
            label: t("glossaryTab.definitionLabel"),
            value: term.definition || t("glossaryTab.hoverNoDefinition"),
          },
          {
            // REQ-1591: derived from the refs for a rooted term, declared for a concept; none
            // means unscoped, which the create form names as the enterprise.
            label: t("glossaryTab.domainsLabel"),
            value: term.domains.length
              ? term.domains.join(", ")
              : t("glossaryTab.enterpriseDomainLabel"),
          },
          {
            label: t("glossaryTab.roleLabel"),
            value: <TermRoleBadge term={term} testId="glossary-detail-role" />,
          },
          { label: t("glossaryTab.proposed"), value: yesNo(!term.live) },
          { label: t("glossaryTab.deprecated"), value: yesNo(term.deprecated) },
          { label: t("glossaryTab.retired"), value: yesNo(term.retired) },
          { label: t("glossaryTab.excludeFromExportLabel"), value: yesNo(term.export_excluded) },
        ]}
      />

      <Title order={5}>{t("glossaryTab.refsTitle")}</Title>
      {term.refs.length === 0 ? (
        <Text size="xs" c="dimmed">
          {t("glossaryTab.noRefs")}
        </Text>
      ) : (
        <SortGroupTable
          testPrefix="glossary-refs"
          minWidth={480}
          rows={term.refs}
          columns={[
            { key: "column", label: t("glossaryTab.colColumn"), sortValue: (r) => r.column_name },
            {
              key: "table",
              label: t("glossaryTab.colTable"),
              sortValue: (r) => r.alias || r.table_name,
            },
            {
              key: "source",
              label: t("glossaryTab.colSource"),
              sortValue: (r) => r.source_id,
              groupValue: (r) => r.source_id,
            },
            {
              key: "domain",
              label: t("glossaryTab.colDomain"),
              sortValue: (r) => r.domain_id,
              groupValue: (r) => r.domain_id,
            },
          ]}
          headers={[{ col: "column" }, { col: "table" }, { col: "source" }, { col: "domain" }]}
          colSpan={4}
          rowKey={(ref) => `${ref.table_id}:${ref.column_name}`}
          render={(ref) => (
            <ListRow
              key={`${ref.table_id}:${ref.column_name}`}
              testId={`glossary-ref-${ref.table_id}-${ref.column_name}`}
            >
              <Table.Td>{ref.column_name}</Table.Td>
              <Table.Td>{ref.alias || ref.table_name}</Table.Td>
              <Table.Td>{ref.source_id}</Table.Td>
              <Table.Td>{ref.domain_id}</Table.Td>
            </ListRow>
          )}
        />
      )}

      <Title order={5}>{t("glossaryTab.groundedTablesTitle")}</Title>
      <GlossaryGroundedTables termId={term.id} canEdit={false} onChanged={() => undefined} />

      <Title order={5}>{t("glossaryTab.relationshipsTitle")}</Title>
      <GlossaryRelationships
        detail={term}
        canEdit={false}
        relEditMode={false}
        relTypeOptions={relTypeOptions}
        otherTermOptions={otherTermOptions}
        edgeTermId={null}
        setEdgeTermId={() => undefined}
        edgeRelType={null}
        setEdgeRelType={() => undefined}
        edgeFlipped={false}
        setEdgeFlipped={() => undefined}
        navigateToTerm={navigateToTerm}
        act={async (fn) => fn()}
        relLabel={relLabel}
        t={t}
        retypeGlossaryEdge={retypeGlossaryEdge}
        removeGlossaryEdge={removeGlossaryEdge}
        addGlossaryEdge={addGlossaryEdge}
      />

      <Title order={5}>{t("glossaryTab.expertsTitle")}</Title>
      <Stack gap={4}>
        {term.experts.map((expert) => (
          <Group key={expert.user_id} gap="xs" data-testid={`glossary-expert-${expert.user_id}`}>
            <Text size="sm">{expert.user_id}</Text>
            <Badge size="sm" variant="light">
              {t(`glossaryTab.kind_${expert.kind}`)}
            </Badge>
          </Group>
        ))}
        {term.experts.length === 0 && (
          <Text size="xs" c="dimmed">
            {t("glossaryTab.noExperts")}
          </Text>
        )}
      </Stack>
    </Stack>
  );
}
