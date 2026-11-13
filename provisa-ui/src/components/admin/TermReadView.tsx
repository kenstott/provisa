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
import type { TFunction } from "i18next";
import type { ReactNode } from "react";
import { Badge, Group, Stack, Table, Text } from "@mantine/core";
import {
  termRole,
  addGlossaryEdge,
  removeGlossaryEdge,
  retypeGlossaryEdge,
  type GlossaryTermDetail,
} from "../../api/glossary";
import { FormRead, ReadRow } from "../form/FormLayout";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";
import { TagControl } from "../TagControl";
import { ListRow } from "../list/ListTable";
import { SortGroupTable } from "../list/SortGroupTable";
import { GlossaryGroundedTables } from "./GlossaryGroundedTables";
import { GlossaryRelationships } from "./GlossaryRelationships";
import { TermGroundingChip, TermRoleBadge, TermStatusBadge } from "./TermRoleBadge";
import { RetiredTermRecord } from "./RetiredTermRecord";
import { OntologySnippet } from "../graph3d/OntologyGraph";
import { termPreset } from "../graph3d/ontologyPresets";

type Option = { value: string; label: string };

/** One section of the term's page: the application's collapsible, its open or closed state
 * remembered per section as a product's detail cards remember theirs (DetailCard). */
function TermSection({
  section,
  title,
  defaultOpen,
  children,
}: {
  section: string;
  title: string;
  defaultOpen?: boolean;
  children: ReactNode;
}) {
  return (
    <Collapsibles id={`glossary-term.${section}`}>
      <CollapsibleItem
        value={section}
        title={title}
        defaultOpen={defaultOpen}
        controlTestId={`glossary-term-${section}-toggle`}
      >
        {children}
      </CollapsibleItem>
    </Collapsibles>
  );
}

/** What keeps a term from reaching data, or from reaching it cleanly, beside its Grounding. */
function groundingHint(
  term: GlossaryTermDetail,
  t: TFunction,
): { hint?: string; hintTestId?: string } {
  if (term.grounding === "not_grounded" && (term.status === "draft" || term.status === "approved"))
    return { hint: t("glossaryTab.draftUngrounded"), hintTestId: "glossary-ungrounded" };
  if (term.grounding === "grounded_pending" && term.pending_on.length > 0)
    return {
      hint: t("glossaryTab.pendingOn", { terms: term.pending_on.map((p) => p.name).join(", ") }),
      hintTestId: "glossary-pending-on",
    };
  return {};
}

export function TermReadView({
  term,
  relTypeOptions,
  otherTermOptions,
  relLabel,
  navigateToTerm,
  canEdit,
  onTagsChanged,
}: {
  term: GlossaryTermDetail;
  relTypeOptions: Option[];
  otherTermOptions: Option[];
  relLabel: (options: Option[], relType: string) => string;
  navigateToTerm: (termId: number) => void;
  /** The reader curates the glossary: the term's tags are changed here, with the chip. */
  canEdit: boolean;
  /** A tag was put on the term or taken off; the term is read again. */
  onTagsChanged: () => void;
}) {
  const { t } = useTranslation();
  const yesNo = (v: boolean) => (v ? t("detailFields.yes") : t("detailFields.no"));
  return (
    <Stack gap="md" pb="md">
      {/* REQ-1968: the form layout's read form, the fields in the edit form's order
          (TermEditForm.tsx) and then what the term has that the form does not set. The name and
          what it means take the panel's width; the rest flow. A state's hint sits under the value
          it explains. */}
      <FormRead testId="glossary-read-fields">
        <ReadRow label={t("glossaryTab.nameLabel")} value={term.name} size="full" />
        <ReadRow
          label={t("glossaryTab.definitionLabel")}
          value={term.definition || t("glossaryTab.hoverNoDefinition")}
          size="full"
        />
        {/* REQ-2243: how the measure is computed, when the term says. */}
        {term.calculation_method && (
          <ReadRow
            label={t("glossaryTab.calculationMethodLabel")}
            value={term.calculation_method}
            size="full"
          />
        )}
        <ReadRow
          label={t("glossaryTab.excludeFromExportLabel")}
          value={yesNo(term.export_excluded)}
          size="short"
        />
        {/* REQ-2145: pii on a Property, which every column it means carries. */}
        {termRole(term) === "property" && (
          <ReadRow
            label={t("glossaryTab.piiLabel")}
            size="short"
            value={
              term.tags.some((g) => g.tag_id === "pii") ? (
                <Group gap={6} data-testid="glossary-detail-pii">
                  <Badge size="xs" color="red" variant="light">
                    {t("glossaryTab.piiLabel")}
                  </Badge>
                  <Text size="sm">{t("glossaryTab.piiMarks", { count: term.ref_count })}</Text>
                </Group>
              ) : (
                yesNo(false)
              )
            }
          />
        )}
        {/* REQ-1591: derived from the refs for a rooted term, declared for a concept; none means
            unscoped, which the create form names as the enterprise. */}
        <ReadRow
          label={t("glossaryTab.domainsLabel")}
          size="medium"
          value={
            term.domains.length ? term.domains.join(", ") : t("glossaryTab.enterpriseDomainLabel")
          }
        />
        <ReadRow
          label={t("glossaryTab.roleLabel")}
          size="short"
          value={<TermRoleBadge term={term} testId="glossary-detail-role" />}
        />
        {/* REQ-2145, REQ-2208: the term's registry tags, with the chip every taggable object has.
            Which tags a term may take, the server says. */}
        <ReadRow
          label={t("glossaryTab.tagsLabel")}
          size="medium"
          value={
            <TagControl
              objectType="term"
              termId={term.id}
              readOnly={!canEdit}
              onChanged={onTagsChanged}
            />
          }
        />
        {/* A draft's tip says which of the two admission tests it still fails. */}
        <ReadRow
          label={t("glossaryTab.statusLabel")}
          size="medium"
          value={
            <TermStatusBadge
              status={term.status}
              testId="glossary-detail-status"
              draftHelp={
                (term.definition ?? "").trim()
                  ? t("glossaryTab.draftDefined")
                  : t("glossaryTab.draftUndefined")
              }
            />
          }
        />
        {/* REQ-1387: computed, beside the status a person set and never with it; what keeps it
            from reaching data, or from reaching it cleanly. */}
        <ReadRow
          label={t("glossaryTab.groundingLabel")}
          size="medium"
          value={
            <Group gap={6} data-testid="glossary-detail-grounding">
              <TermGroundingChip
                grounding={term.grounding}
                columns={term.ref_count}
                tables={term.table_count}
              />
              <Text size="sm">{t(`glossaryTab.groundingValue_${term.grounding}`)}</Text>
            </Group>
          }
          {...groundingHint(term, t)}
        />
        {/* A deprecation or a retirement says why, who said so, and a deprecation when it ends. */}
        {(term.status === "deprecated" || term.status === "retired") && (
          <>
            <ReadRow
              label={t("glossaryTab.statusReasonLabel")}
              size="medium"
              value={term.status_reason ?? "—"}
            />
            <ReadRow
              label={t("glossaryTab.statusByLabel")}
              size="short"
              value={term.status_by ?? "—"}
            />
          </>
        )}
        {term.status === "deprecated" && (
          <ReadRow
            label={t("glossaryTab.retireOnLabel")}
            size="short"
            value={term.retire_on ?? "—"}
          />
        )}
        {/* REQ-2237: the IRI of the imported concept the term was aligned with as the same entity,
            or was made from. It is curator work: the term is kept when its columns leave. */}
        {term.iri && (
          <ReadRow
            label={t("ontologyImport.iriLabel")}
            size="full"
            testId="glossary-detail-iri"
            value={
              <Stack gap={2}>
                <Text size="sm" ff="monospace" style={{ wordBreak: "break-all" }}>
                  {term.iri}
                </Text>
                <Text size="xs" c="dimmed" data-testid="glossary-detail-curated-by-iri">
                  {t("ontologyImport.curatedByIri")}
                </Text>
              </Stack>
            }
          />
        )}
      </FormRead>

      {term.status === "retired" && (
        <RetiredTermRecord term={term} navigateToTerm={navigateToTerm} />
      )}

      <TermSection section="graph" title={t("glossaryTab.termGraphTitle")} defaultOpen>
        {/* REQ-2174: the ontology, focused on the term: its ancestors and descendants, and the
            columns, tables and products it reaches. */}
        <OntologySnippet
          key={term.id}
          preset={termPreset(term.id)}
          name={term.name}
          testId="glossary-term-dag"
        />
      </TermSection>

      <TermSection section="grounded-columns" title={t("glossaryTab.refsTitle")}>
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
      </TermSection>

      <TermSection section="grounded-tables" title={t("glossaryTab.groundedTablesTitle")}>
        <GlossaryGroundedTables termId={term.id} canEdit={false} onChanged={() => undefined} />
      </TermSection>

      <TermSection section="relationships" title={t("glossaryTab.relationshipsTitle")}>
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
      </TermSection>

      <TermSection section="experts" title={t("glossaryTab.expertsTitle")}>
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
      </TermSection>
    </Stack>
  );
}
