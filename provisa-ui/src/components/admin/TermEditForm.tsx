// Copyright (c) 2026 Kenneth Stott
// Canary: ade35390-96e5-41ed-9160-8900ebf1430e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A term's panel in Edit mode: one form holding every field a curator can change -- the name, the
// definition, export, the declared domains of a concept, the columns it names (moved away,
// or claimed from another term), the tables it is grounded in, its relationships and its experts.
// Save makes the changes (termDraft.ts) and Cancel drops them. What follows from the term's columns,
// tables and relationships is shown with why it cannot be set here. A class (tables) holds its tables'
// keys, shown and not claimed; a property (columns) claims attribute columns, the keys and its
// tables offered disabled; a concept may do either.

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Checkbox,
  Group,
  MultiSelect,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Trash2 } from "lucide-react";
import {
  generateGlossaryDefinition,
  listTermTables,
  type GlossaryEdge,
  type GlossaryExpertKind,
  type GlossaryRelType,
  type GlossaryTermDetail,
} from "../../api/glossary";
import { useAuth } from "../../context/AuthContext";
import { useRelationships, useTables } from "../../hooks/useAdminQueries";
import { DescriptionField } from "../../pages/tables/DescriptionField";
import { relationName } from "../../naming";
import { DetailFields } from "../detail/DetailFields";
import { FieldGroup, FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { columnKey, columnTerms, type ColumnTerm } from "./claimColumns";
import { GlossaryRelationships } from "./GlossaryRelationships";
import { TermRoleBadge, TermStatusBadge } from "./TermRoleBadge";
import { changes, draftOf, type Change, type TermDraft } from "./termDraft";

type Option = { value: string; label: string };

export function TermEditForm({
  term,
  otherTermOptions,
  relTypeOptions,
  kindOptions,
  memberOptions,
  domainOptions,
  domainsEnabled,
  relLabel,
  navigateToTerm,
  onSaved,
  onCancel,
}: {
  term: GlossaryTermDetail;
  otherTermOptions: Option[];
  relTypeOptions: Option[];
  kindOptions: Option[];
  memberOptions: Option[];
  /** The domains a concept can declare. */
  domainOptions: Option[];
  domainsEnabled: boolean;
  relLabel: (options: Option[], relType: string) => string;
  navigateToTerm: (termId: number) => void;
  /** After Save (all of it or up to a refusal): the term is read again; ``removed`` when a move
   * left it with nothing and the server removed it. */
  onSaved: (done: boolean, removed: boolean) => Promise<void>;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const { actingRoles } = useAuth();
  const { tables } = useTables();
  const { relationships } = useRelationships();
  // The term's tables as read, and the draft made from the term and them.
  const [read, setRead] = useState<{ termId: number; tables: number[] } | null>(null);
  const [draft, setDraft] = useState<TermDraft | null>(null);
  const [owners, setOwners] = useState<Map<string, ColumnTerm> | null>(null);
  const [refused, setRefused] = useState<{ field: Change["field"]; message: string } | null>(null);
  const [saving, setSaving] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [edgeTermId, setEdgeTermId] = useState<string | null>(null);
  const [edgeRelType, setEdgeRelType] = useState<string | null>(null);
  const [edgeFlipped, setEdgeFlipped] = useState(false);
  const [expertUserId, setExpertUserId] = useState<string | null>(null);
  const [expertKind, setExpertKind] = useState<string>("expert");

  useEffect(() => {
    let live = true;
    listTermTables(term.id).then(
      (rows) => {
        if (!live) return;
        const ids = rows.map((r) => r.table_id);
        setRead({ termId: term.id, tables: ids });
        // The draft is made once; read again after a refused Save, it keeps what was typed.
        setDraft((d) => d ?? draftOf(term, ids));
      },
      (e: Error) => live && setRefused({ field: "tables", message: e.message }),
    );
    return () => {
      live = false;
    };
  }, [term]);

  useEffect(() => {
    if (actingRoles === null) return;
    let live = true;
    columnTerms(actingRoles).then(
      (m) => live && setOwners(m ?? new Map()),
      (e: Error) => live && setRefused({ field: "columns", message: e.message }),
    );
    return () => {
      live = false;
    };
  }, [actingRoles]);

  const nameOf = (id: number) => otherTermOptions.find((o) => o.value === String(id))?.label ?? "";
  const own = useMemo(
    () => new Set(term.refs.map((r) => columnKey(r.table_id, r.column_name))),
    [term.refs],
  );
  const claimOptions = useMemo(() => {
    const out: (Option & { from: ColumnTerm | null })[] = [];
    for (const tb of tables) {
      if (tb.draft) continue;
      for (const c of tb.columns ?? []) {
        const key = columnKey(tb.id, c.columnName);
        if (own.has(key)) continue;
        out.push({
          value: key,
          label: `${relationName(tb)}.${c.columnName}`,
          from: owners?.get(key) ?? null,
        });
      }
    }
    return out.sort((a, b) => a.label.localeCompare(b.label));
  }, [tables, own, owners]);
  const tableOptions = useMemo(
    () =>
      tables
        .filter((tb) => !tb.draft)
        .map((tb) => ({ value: String(tb.id), label: relationName(tb) }))
        .sort((a, b) => a.label.localeCompare(b.label)),
    [tables],
  );

  if (draft === null || read === null)
    return (
      <Text size="sm" c="dimmed">
        {t("common.loading")}
      </Text>
    );

  const set = (patch: Partial<TermDraft>) => setDraft({ ...draft, ...patch });
  // A key column -- a table's primary key, or the column of a declared many-to-one relationship,
  // whatever it is named -- follows from its table's class: no term claims it, and none moves it.
  const keys = new Set<string>();
  for (const tb of tables)
    for (const c of tb.columns ?? []) if (c.isPrimaryKey) keys.add(columnKey(tb.id, c.columnName));
  for (const r of relationships)
    if (r.cardinality === "many-to-one" && r.viaTableId === null)
      keys.add(columnKey(r.sourceTableId, r.sourceColumn));
  // What the term would be after Save: a class (tables), a property (columns) or a concept.
  const keepsColumns =
    term.refs.some(
      (r) =>
        !keys.has(columnKey(r.table_id, r.column_name)) &&
        draft.moves[columnKey(r.table_id, r.column_name)] === undefined,
    ) || draft.claims.length > 0;
  const role = draft.tables.length > 0 ? "class" : keepsColumns ? "property" : "concept";
  const said = (field: Change["field"]) =>
    refused?.field === field ? (
      <Alert color="red" data-testid={`glossary-edit-refused-${field}`}>
        {refused.message}
      </Alert>
    ) : null;

  const save = async () => {
    setSaving(true);
    setRefused(null);
    let removed = false;
    let done = true;
    try {
      for (const change of changes(term, read.tables, draft)) {
        try {
          const result = await change.run();
          if (result && result.termRemoved) removed = true;
        } catch (e) {
          setRefused({ field: change.field, message: e instanceof Error ? e.message : String(e) });
          done = false;
          break;
        }
      }
    } finally {
      setSaving(false);
    }
    await onSaved(done, removed);
  };

  const generate = async () => {
    setGenerating(true);
    try {
      const text = await generateGlossaryDefinition(term.id);
      if (text) set({ definition: text });
    } catch (e) {
      setRefused({ field: "fields", message: e instanceof Error ? e.message : String(e) });
    } finally {
      setGenerating(false);
    }
  };

  // The relationships editor changes the draft, not the server.
  const shown: GlossaryTermDetail = { ...term, edges_out: draft.edgesOut, edges_in: draft.edgesIn };
  const addEdge = async (from: number, to: number, rel: GlossaryRelType) =>
    setDraft((d) =>
      d === null
        ? d
        : from === term.id
          ? { ...d, edgesOut: [...d.edgesOut, { term_id: to, rel_type: rel, name: nameOf(to) }] }
          : { ...d, edgesIn: [...d.edgesIn, { term_id: from, rel_type: rel, name: nameOf(from) }] },
    );
  const removeEdge = async (from: number, to: number, rel: GlossaryRelType) =>
    setDraft((d) =>
      d === null
        ? d
        : from === term.id
          ? { ...d, edgesOut: d.edgesOut.filter((e) => !(e.term_id === to && e.rel_type === rel)) }
          : { ...d, edgesIn: d.edgesIn.filter((e) => !(e.term_id === from && e.rel_type === rel)) },
    );
  const retypeEdge = async (
    from: number,
    to: number,
    rel: GlossaryRelType,
    next: GlossaryRelType,
  ) =>
    setDraft((d) => {
      if (d === null) return d;
      const swap = (other: number) => (e: GlossaryEdge) =>
        e.term_id === other && e.rel_type === rel ? { ...e, rel_type: next } : e;
      return from === term.id
        ? { ...d, edgesOut: d.edgesOut.map(swap(to)) }
        : { ...d, edgesIn: d.edgesIn.map(swap(from)) };
    });

  return (
    <Stack gap="md" pb="md" data-testid="glossary-edit-form">
      <FormLayout>
        <FormField size="medium">
          <TextInput
            label={t("glossaryTab.nameLabel")}
            value={draft.name}
            onChange={(e) => set({ name: e.currentTarget.value })}
            data-testid="glossary-name-input"
          />
        </FormField>
        <FormField size="long">
          <Stack gap={2} data-testid="glossary-definition-input">
            <Text size="sm" fw={500}>
              {t("glossaryTab.definitionLabel")}
            </Text>
            <DescriptionField
              value={draft.definition}
              onChange={(definition) => set({ definition })}
              placeholder={t("glossaryTab.definitionLabel")}
              rows={2}
              generating={generating}
              onGenerate={() => void generate()}
            />
          </Stack>
        </FormField>
        <FormChecks>
          <FormField size="short" help={t("glossaryTab.excludeFromExportHelp")}>
            <Checkbox
              label={t("glossaryTab.excludeFromExportLabel")}
              checked={draft.exportExcluded}
              onChange={(e) => set({ exportExcluded: e.currentTarget.checked })}
              data-testid="glossary-export-excluded-checkbox"
            />
          </FormField>
        </FormChecks>
        {domainsEnabled && term.is_abstract && (
          <FormField size="medium">
            <MultiSelect
              label={t("glossaryTab.domainsLabel")}
              data={domainOptions}
              value={draft.domains}
              onChange={(domains) => set({ domains })}
              searchable
              data-testid="glossary-domains-input"
            />
          </FormField>
        )}
      </FormLayout>
      {said("fields")}

      {/* What follows from the term's columns, tables and relationships: shown, with why. */}
      <DetailFields
        testId="glossary-derived-fields"
        fields={[
          ...(domainsEnabled && !term.is_abstract
            ? [
                {
                  label: t("glossaryTab.domainsLabel"),
                  value: (
                    <Stack gap={0}>
                      <span>
                        {term.domains.length
                          ? term.domains.join(", ")
                          : t("glossaryTab.enterpriseDomainLabel")}
                      </span>
                      <Text size="xs" c="dimmed">
                        {t("glossaryTab.domainsDerived")}
                      </Text>
                    </Stack>
                  ),
                },
              ]
            : []),
          {
            label: t("glossaryTab.roleLabel"),
            value: <TermRoleBadge term={term} testId="glossary-detail-role" />,
          },
          {
            // Changed by Deprecate, Retire and Restore, each with a reason, not here.
            label: t("glossaryTab.statusLabel"),
            value: <TermStatusBadge status={term.status} testId="glossary-edit-status" />,
          },
        ]}
      />
      <Text size="xs" c="dimmed" data-testid="glossary-derived-reason">
        {t("glossaryTab.derivedReason")}
      </Text>

      <Title order={5}>{t("glossaryTab.refsTitle")}</Title>
      {term.refs.length > 0 && (
        <Table data-testid="glossary-edit-refs">
          <Table.Tbody>
            {term.refs.map((ref) => {
              const key = columnKey(ref.table_id, ref.column_name);
              const to = draft.moves[key];
              return (
                <Table.Tr key={key} data-testid={`glossary-ref-${ref.table_id}-${ref.column_name}`}>
                  <Table.Td>{`${ref.domain_id}.${ref.alias || ref.table_name}.${ref.column_name}`}</Table.Td>
                  <Table.Td>
                    {keys.has(key) ? (
                      <Text size="xs" c="dimmed" data-testid={`glossary-key-ref-${key}`}>
                        {t("glossaryTab.keyColumn")}
                      </Text>
                    ) : (
                      <Select
                        size="xs"
                        data={otherTermOptions}
                        value={to === undefined ? null : String(to)}
                        placeholder={t("glossaryTab.moveToPlaceholder")}
                        onChange={(v) => {
                          const moves = { ...draft.moves };
                          if (v === null) delete moves[key];
                          else moves[key] = Number(v);
                          set({ moves });
                        }}
                        searchable
                        clearable
                        aria-label={t("glossaryTab.colMove")}
                        data-testid={`glossary-move-select-${ref.table_id}-${ref.column_name}`}
                      />
                    )}
                  </Table.Td>
                </Table.Tr>
              );
            })}
          </Table.Tbody>
        </Table>
      )}
      {role === "class" ? (
        <Text size="xs" c="dimmed" data-testid="glossary-class-keys">
          {t("glossaryTab.classKeys")}
        </Text>
      ) : (
        <FormLayout>
          <FormField size="long" hint={t("glossaryTab.claimHint")}>
            <MultiSelect
              label={t("glossaryTab.claimColumns")}
              data={claimOptions.map((o) => ({
                value: o.value,
                label: o.label,
                disabled: keys.has(o.value),
              }))}
              value={draft.claims.map((c) => columnKey(c.table_id, c.column_name))}
              onChange={(keys) =>
                set({
                  claims: keys.map((key) => {
                    const at = key.indexOf(":");
                    return {
                      table_id: Number(key.slice(0, at)),
                      column_name: key.slice(at + 1),
                      from: owners?.get(key) ?? null,
                    };
                  }),
                })
              }
              renderOption={({ option }) => {
                const from = claimOptions.find((o) => o.value === option.value)?.from;
                return (
                  <Stack gap={0}>
                    <span>{option.label}</span>
                    {keys.has(option.value) ? (
                      <Text size="xs" c="dimmed">
                        {t("glossaryTab.keyColumnClaim")}
                      </Text>
                    ) : (
                      from && (
                        <Text size="xs" c="dimmed">
                          {from.draft
                            ? t("glossaryTab.claimFromDraft", { term: from.name })
                            : t("glossaryTab.claimFrom", { term: from.name })}
                        </Text>
                      )
                    )}
                  </Stack>
                );
              }}
              searchable
              limit={200}
              data-testid="glossary-claim-columns"
            />
          </FormField>
        </FormLayout>
      )}
      {said("columns")}

      <Title order={5}>{t("glossaryTab.groundedTablesTitle")}</Title>
      <FormLayout>
        <FormField
          size="long"
          hint={role === "property" ? t("glossaryTab.groundDisabled") : undefined}
        >
          <MultiSelect
            aria-label={t("glossaryTab.groundedTablesTitle")}
            data={tableOptions}
            value={draft.tables.map(String)}
            onChange={(ids) => set({ tables: ids.map(Number) })}
            disabled={role === "property"}
            searchable
            clearable
            placeholder={t("glossaryTab.groundedTablesPlaceholder")}
            data-testid="glossary-grounded-tables-select"
          />
        </FormField>
      </FormLayout>
      {said("tables")}

      <Title order={5}>{t("glossaryTab.relationshipsTitle")}</Title>
      <GlossaryRelationships
        detail={shown}
        canEdit
        relEditMode
        relTypeOptions={relTypeOptions}
        otherTermOptions={otherTermOptions}
        edgeTermId={edgeTermId}
        setEdgeTermId={setEdgeTermId}
        edgeRelType={edgeRelType}
        setEdgeRelType={setEdgeRelType}
        edgeFlipped={edgeFlipped}
        setEdgeFlipped={setEdgeFlipped}
        navigateToTerm={navigateToTerm}
        act={async (fn) => {
          await fn();
          setEdgeTermId(null);
          setEdgeRelType(null);
        }}
        relLabel={relLabel}
        t={t}
        retypeGlossaryEdge={retypeEdge}
        removeGlossaryEdge={removeEdge}
        addGlossaryEdge={addEdge}
      />
      {said("relationships")}

      <Title order={5}>{t("glossaryTab.expertsTitle")}</Title>
      <Stack gap={4}>
        {draft.experts.map((expert) => (
          <Group key={expert.user_id} gap="xs" data-testid={`glossary-expert-${expert.user_id}`}>
            <Text size="sm">{expert.user_id}</Text>
            <Badge size="sm" variant="light">
              {t(`glossaryTab.kind_${expert.kind}`)}
            </Badge>
            <ActionIcon
              variant="subtle"
              color="red"
              size="sm"
              aria-label={t("glossaryTab.removeExpert")}
              onClick={() =>
                set({ experts: draft.experts.filter((e) => e.user_id !== expert.user_id) })
              }
            >
              <Trash2 size={13} />
            </ActionIcon>
          </Group>
        ))}
      </Stack>
      <FormLayout>
        <FieldGroup row>
          <FormField size="medium">
            <Select
              label={t("glossaryTab.expertUserLabel")}
              placeholder={t("glossaryTab.expertUserPlaceholder")}
              data={memberOptions.filter((m) => !draft.experts.some((e) => e.user_id === m.value))}
              value={expertUserId}
              onChange={setExpertUserId}
              searchable
              data-testid="glossary-expert-user-input"
            />
          </FormField>
          <FormField size="short">
            <Select
              label={t("glossaryTab.expertKindLabel")}
              data={kindOptions}
              value={expertKind}
              onChange={(v) => v && setExpertKind(v)}
              allowDeselect={false}
              data-testid="glossary-expert-kind-select"
            />
          </FormField>
          <Button
            variant="default"
            disabled={!expertUserId}
            onClick={() => {
              set({
                experts: [
                  ...draft.experts,
                  { user_id: expertUserId as string, kind: expertKind as GlossaryExpertKind },
                ],
              });
              setExpertUserId(null);
            }}
            data-testid="glossary-expert-add-btn"
          >
            {t("glossaryTab.addExpert")}
          </Button>
        </FieldGroup>
      </FormLayout>
      {said("experts")}

      <FormLayout>
        <FormFooter
          onSave={save}
          onCancel={onCancel}
          saving={saving}
          saveProps={{ disabled: !draft.name.trim(), "data-testid": "glossary-edit-save" }}
          cancelProps={{ "data-testid": "glossary-edit-cancel" }}
        />
      </FormLayout>
    </Stack>
  );
}
