// Copyright (c) 2026 Kenneth Stott
// Canary: de91a53c-421b-44f1-b5e6-e6a80adebb9b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useMemo, useState } from "react";
import { useRememberedChoice } from "../../hooks/useRememberedChoice";
import { useDetailParam } from "../list/useDetailParam";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Checkbox,
  Group,
  List as MantineList,
  Pagination,
  SegmentedControl,
  Select,
  Stack,
  Switch,
  Table,
  TextInput,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { List, Lock, Pencil, Plus, Tag as TagIcon, Trash2, X } from "lucide-react";
import {
  useTags,
  useTagAssignments,
  useSaveTag,
  useDeleteTag,
  useDeleteTags,
  useUpsertTagParamValue,
  useDeleteTagParamValue,
  useTagCitations,
  citationKey,
  pickedCitation,
  type CitationPlace,
} from "../../hooks/useTagQueries";
import type {
  LibraryCitation,
  MutationResult,
  Tag,
  TagFieldPolicy,
  TagObjectType,
  TagOutsideAssignment,
  TagParamPolicy,
} from "../../types/admin";
import { baseTagId } from "../../types/admin";
import { FilterInput } from "./FilterInput";
import { HelpBubble } from "../HelpBubble";
import { ListRow } from "../list/ListTable";
import { SortGroupTable } from "../list/SortGroupTable";
import { useListSelection } from "../list/listSelection";
import { ListBulkBar } from "../list/ListBulkBar";
import { FormChecks, FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { Tip } from "../Tip";
import { TagCitations } from "../TagCitations";
import { useDependentsDialog } from "../../hooks/useDependentsDialog";
import { matchesQuery } from "../../lib/searchItems";
import { MultiSelect } from "../MultiSelect";
import { useDomains } from "../../hooks/useAdminQueries";
import { useDomainFilter } from "../../context/DomainFilterContext";
import { assignableDomainIds } from "../../lib/internalDomains";

const PAGE_SIZE = 50;

// REQ-1634, REQ-2145, REQ-2208: a data product, a glossary term, a row rule, a role, a metric, and a
// reference library document and its sections are tagged as the other objects are.
const SCOPES: TagObjectType[] = [
  "source",
  "table",
  "column",
  "relationship",
  "command",
  "product",
  "term",
  "rls_rule",
  "role",
  "metric",
  "document",
  "section",
];

const done: MutationResult = { success: true, message: "" };

const POLICIES: TagFieldPolicy[] = ["hidden", "optional", "required"];

// REQ-1467: a tag either takes no parameter or demands one. There is no "optional" — a bare
// `entity` alongside `entity:customer` would need a reading, and the only reading available is a
// guessed entity type.
const PARAM_POLICIES: TagParamPolicy[] = ["none", "required"];

// REQ-1374: one org-level tag registry, viewed one scope at a time. The
// SegmentedControl filters the single list; a multi-scope tag appears under
// each scope its appliesTo names.
export function TagsTab() {
  const { t } = useTranslation();
  const { tags } = useTags();
  const { tagAssignments } = useTagAssignments();
  const { createTag, updateTag } = useSaveTag();
  const { deleteTag } = useDeleteTag();
  const refusal = useDependentsDialog();
  const { upsertTagParamValue } = useUpsertTagParamValue();
  const { deleteTagParamValue } = useDeleteTagParamValue();
  const { citeDefinition } = useTagCitations();
  const { domains } = useDomains();
  const { domainsEnabled } = useDomainFilter();

  const [rememberedScope, setRememberedScope] = useRememberedChoice<TagObjectType>(
    "filter.tags.scope",
    SCOPES,
    "source",
  );
  // A tag link's scope is that visit's, not the reader's choice: it is not saved over theirs.
  const [linkedScope, setLinkedScope] = useState<TagObjectType | null>(null);
  const scope = linkedScope ?? rememberedScope;
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);
  const [showForm, setShowForm] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  // `?tag=<id>` (a graph's or a refused delete's link, detailHref): the list shows that tag,
  // under a scope it applies to, its row marked.
  const linkedTag = useDetailParam("tags").id;
  const [seededTag, setSeededTag] = useState<string | null>(null);
  const linked = tags.find((g) => g.id === linkedTag);
  if (linked && linkedTag !== seededTag) {
    setSeededTag(linkedTag);
    if (linked.appliesTo.length > 0) setLinkedScope(linked.appliesTo[0]);
    setSearch(linked.id);
  }
  const [formId, setFormId] = useState("");
  const [formDescription, setFormDescription] = useState("");
  const [formScopes, setFormScopes] = useState<string[]>([]);
  const [formReasonPolicy, setFormReasonPolicy] = useState<TagFieldPolicy>("optional");
  const [formExpiresPolicy, setFormExpiresPolicy] = useState<TagFieldPolicy>("optional");
  const [formParamPolicy, setFormParamPolicy] = useState<TagParamPolicy>("none");
  const [formRequiresCitation, setFormRequiresCitation] = useState(false);
  const [formRequired, setFormRequired] = useState(false);
  // REQ-2208: the domains whose objects the tag is offered on, as a source's; none = every domain.
  const [formDomains, setFormDomains] = useState<string[]>([]);
  // The assignments the last save left outside the tag's domains: they stay, and are listed.
  const [outside, setOutside] = useState<TagOutsideAssignment[]>([]);
  // The sections that say why the tag exists, edited here and saved with the tag.
  const [formCited, setFormCited] = useState<LibraryCitation[]>([]);
  // REQ-1467: which tag's value list is open, and the draft new value/description beneath it.
  const [valuesTagId, setValuesTagId] = useState<string | null>(null);
  const [newValue, setNewValue] = useState("");
  const [newValueDescription, setNewValueDescription] = useState("");

  const scopeLabel = (s: TagObjectType) => t(`tagsTab.scope_${s}`);
  const policyLabel = (p: TagFieldPolicy) => t(`tagsTab.policy_${p}`);
  const policyOptions = POLICIES.map((p) => ({ label: policyLabel(p), value: p }));
  const paramPolicyOptions = PARAM_POLICIES.map((p) => ({
    label: t(`tagsTab.paramPolicy_${p}`),
    value: p,
  }));

  const assignmentCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    // REQ-1467: counted by base id — "entity:customer" and "entity:vendor" are both uses of the
    // `entity` row this table lists, and counting the full ids would report every one as unused.
    for (const a of tagAssignments) {
      const base = baseTagId(a.tagId);
      counts[base] = (counts[base] ?? 0) + 1;
    }
    return counts;
  }, [tagAssignments]);

  const filtered = tags
    .filter((tag) => tag.appliesTo.includes(scope))
    .filter((tag) => matchesQuery([tag.id, tag.description], search))
    .sort((a, b) => (a.isSystem === b.isSystem ? a.id.localeCompare(b.id) : a.isSystem ? -1 : 1));
  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safePage = Math.min(page, totalPages);
  const [grouped, setGrouped] = useState(false);
  // REQ-2240: tags selected and deleted together; a system tag is refused by its own delete.
  const selection = useListSelection(filtered.map((tag) => tag.id));
  const deleteTags = useDeleteTags();

  const resetForm = () => {
    setEditingId(null);
    setFormId("");
    setFormDescription("");
    setFormScopes([]);
    setFormReasonPolicy("optional");
    setFormExpiresPolicy("optional");
    setFormParamPolicy("none");
    setFormRequiresCitation(false);
    setFormRequired(false);
    setFormDomains([]);
    setOutside([]);
    setFormCited([]);
  };

  const closeForm = () => {
    setShowForm(false);
    resetForm();
  };

  const openCreate = () => {
    resetForm();
    setFormScopes([scope]);
    setShowForm(true);
  };

  const openEdit = (tag: Tag) => {
    setEditingId(tag.id);
    setFormId(tag.id);
    setFormDescription(tag.description);
    setFormScopes([...tag.appliesTo]);
    setFormReasonPolicy(tag.reasonPolicy);
    setFormExpiresPolicy(tag.expiresPolicy);
    setFormParamPolicy(tag.paramPolicy);
    setFormRequiresCitation(tag.requiresCitation);
    setFormRequired(tag.required);
    setFormDomains([...tag.allowedDomains]);
    setOutside([]);
    setFormCited(tag.citations);
    setShowForm(true);
  };

  const citeInForm = async (place: CitationPlace, documentTitle: string) => {
    const key = citationKey(place);
    setFormCited((held) =>
      held.some((c) => citationKey(c) === key)
        ? held
        : [...held, pickedCitation(place, documentTitle)],
    );
    return done;
  };
  const uncite = async (c: LibraryCitation) => {
    setFormCited((held) => held.filter((h) => citationKey(h) !== citationKey(c)));
    return done;
  };
  // Confirm is the one act that records a citation's text anew: the saved tag cites it again at
  // once, and the form shows it current, as the next read will.
  const confirmInForm = async (c: LibraryCitation) => {
    if (editingId === null) return done;
    const result = await citeDefinition(editingId, c);
    if (result.success) {
      setFormCited((held) =>
        held.map((h) =>
          citationKey(h) === citationKey(c) ? { ...h, state: "current", needsReview: false } : h,
        ),
      );
    }
    return result;
  };

  const handleSubmit = async () => {
    if (!formId.trim() || formScopes.length === 0) return;
    // A new tag is created; the open one updated (a create refuses an id that exists).
    const save = editingId === null ? createTag : updateTag;
    const result = await save(
      formId.trim(),
      formDescription.trim(),
      formScopes,
      formReasonPolicy,
      formExpiresPolicy,
      formParamPolicy,
      formRequiresCitation,
      formRequired,
      formCited.map(({ documentId, sectionHeading, sectionPage }) => ({
        documentId,
        sectionHeading,
        sectionPage,
      })),
      formDomains,
    );
    if (result.success) {
      notifications.show({ color: "green", message: t("tagsTab.saved", { id: formId.trim() }) });
      // REQ-2208: a save that narrowed the tag's domains keeps the form open on the saved tag,
      // listing the assignments now outside them; they stay.
      const left = (result.params as { outside?: TagOutsideAssignment[] } | null)?.outside ?? [];
      if (left.length > 0) {
        setEditingId(formId.trim());
        setOutside(left);
        return;
      }
      setShowForm(false);
      resetForm();
    } else {
      notifications.show({ color: "red", message: result.message });
    }
  };

  // An outside assignment as its object is known: a table's or column's address, else its id.
  const outsideName = (o: TagOutsideAssignment) => {
    const held = tagAssignments.find(
      (a) => a.objectKey === o.objectKey && baseTagId(a.tagId) === baseTagId(o.tagId),
    );
    if (held?.tableRef)
      return held.columnName ? `${held.tableRef}.${held.columnName}` : held.tableRef;
    return o.objectKey.slice(o.objectKey.indexOf(":") + 1);
  };

  const toggleValues = (tagId: string) => {
    setNewValue("");
    setNewValueDescription("");
    setValuesTagId(valuesTagId === tagId ? null : tagId);
  };

  const handleAddValue = async (tagId: string) => {
    const result = await upsertTagParamValue(tagId, newValue.trim(), newValueDescription.trim());
    if (result.success) {
      setNewValue("");
      setNewValueDescription("");
    } else {
      notifications.show({ color: "red", message: result.message });
    }
  };

  const handleDeleteValue = async (tagId: string, value: string) => {
    const result = await deleteTagParamValue(tagId, value);
    if (!result.success) {
      notifications.show({ color: "red", message: result.message });
    }
  };

  const handleDelete = async (tag: Tag) => {
    if (!window.confirm(t("tagsTab.deleteConfirm", { id: tag.id }))) return;
    const result = await deleteTag(tag.id);
    // REQ-1384: a tag that carries a row filter is not deleted; the dialog names the filter.
    if (refusal.refused(result, tag.id)) return;
    if (result.success) {
      notifications.show({ message: t("tagsTab.deleted", { id: tag.id }) });
    } else {
      notifications.show({ color: "red", message: result.message });
    }
  };

  return (
    <Stack gap="md">
      {refusal.dialog}
      <Group justify="space-between" wrap="wrap">
        <Group gap="xs">
          <TagIcon size={18} />
          <Title order={3}>{t("tagsTab.heading")}</Title>
        </Group>
        <FilterInput
          value={search}
          onChange={(v) => {
            setSearch(v);
            setPage(1);
          }}
          placeholder={t("tagsTab.filterPlaceholder")}
        />
        <Group gap="xs">
          {!showForm && (
            <Button
              leftSection={<Plus size={14} />}
              onClick={openCreate}
              data-testid="tags-create-button"
            >
              {t("tagsTab.addTag")}
            </Button>
          )}
          <HelpBubble
            title={t("tagsTab.purposeTitle")}
            paragraphs={[t("tagsTab.purposeBody"), t("tagsTab.purposeAdd")]}
            ariaLabel={t("tagsTab.purposeAria")}
            testId="tags-purpose-help"
          />
        </Group>
      </Group>

      <SegmentedControl
        size="xs"
        value={scope}
        onChange={(v) => {
          setLinkedScope(null);
          setRememberedScope(v as TagObjectType);
          setPage(1);
        }}
        data={SCOPES.map((s) => ({ label: scopeLabel(s), value: s }))}
        data-testid="tags-scope-bar"
        // REQ-2208: twelve object kinds do not fit one row, in German not even at 1900px; the bar
        // wraps onto further rows instead of clipping its labels, each row filled edge to edge.
        fullWidth
        styles={{ root: { flexWrap: "wrap" }, control: { flex: "1 0 auto" } }}
      />

      {showForm && (
        <FormLayout>
          <FormIdentifiers>
            <FormField hint={t("tagsTab.idDesc")} size="medium">
              <TextInput
                label={t("tagsTab.idLabel")}
                placeholder={t("tagsTab.idPlaceholder")}
                value={formId}
                disabled={editingId !== null}
                onChange={(e) => setFormId(e.currentTarget.value)}
              />
            </FormField>
          </FormIdentifiers>
          <FormField size="long">
            <TextInput
              label={t("tagsTab.descriptionLabel")}
              placeholder={t("tagsTab.descriptionPlaceholder")}
              value={formDescription}
              onChange={(e) => setFormDescription(e.currentTarget.value)}
            />
          </FormField>
          <FormField hint={t("tagsTab.appliesToDesc")} size="full">
            <Checkbox.Group
              label={t("tagsTab.appliesToLabel")}
              value={formScopes}
              onChange={setFormScopes}
            >
              <Group gap="md" mt="xs">
                {SCOPES.map((s) => (
                  <Checkbox key={s} value={s} label={scopeLabel(s)} />
                ))}
              </Group>
            </Checkbox.Group>
          </FormField>
          {domainsEnabled && (
            <FormField
              size="long"
              hint={t("sourceFormFieldsExtended.allowedDomainsHint")}
              data-testid="tags-allowed-domains"
            >
              <MultiSelect
                label={t("sourceFormFieldsExtended.allowedDomains")}
                options={assignableDomainIds(domains.map((d) => d.id)).map((id) => ({
                  id,
                  label: id,
                }))}
                value={formDomains}
                onChange={setFormDomains}
              />
            </FormField>
          )}
          <FormField size="medium">
            <Select
              label={t("tagsTab.reasonPolicyLabel")}
              data={policyOptions}
              value={formReasonPolicy}
              allowDeselect={false}
              onChange={(v) => setFormReasonPolicy(v as TagFieldPolicy)}
              data-testid="tags-reason-policy"
            />
          </FormField>
          <FormField size="medium">
            <Select
              label={t("tagsTab.expiresPolicyLabel")}
              data={policyOptions}
              value={formExpiresPolicy}
              allowDeselect={false}
              onChange={(v) => setFormExpiresPolicy(v as TagFieldPolicy)}
              data-testid="tags-expires-policy"
            />
          </FormField>
          <FormField hint={t("tagsTab.paramPolicyDesc")} size="medium">
            <Select
              label={t("tagsTab.paramPolicyLabel")}
              data={paramPolicyOptions}
              value={formParamPolicy}
              allowDeselect={false}
              onChange={(v) => setFormParamPolicy(v as TagParamPolicy)}
              data-testid="tags-param-policy"
            />
          </FormField>
          <FormChecks>
            <FormField help={t("tagCitations.requiresCitationDesc")} size="medium">
              <Switch
                label={t("tagCitations.requiresCitationLabel")}
                checked={formRequiresCitation}
                onChange={(e) => setFormRequiresCitation(e.currentTarget.checked)}
                data-testid="tags-requires-citation"
              />
            </FormField>
            <FormField help={t("tagsTab.requiredDesc")} size="medium">
              <Switch
                label={t("tagsTab.requiredLabel")}
                checked={formRequired}
                onChange={(e) => setFormRequired(e.currentTarget.checked)}
                data-testid="tags-required"
              />
            </FormField>
          </FormChecks>
          <FormField size="full">
            <TagCitations
              testId="tags-defined-by"
              label={t("tagCitations.definitionLabel")}
              description={t("tagCitations.definitionDesc")}
              citations={formCited}
              onAdd={citeInForm}
              onRemove={uncite}
              onConfirm={confirmInForm}
            />
          </FormField>
          {outside.length > 0 && (
            <FormField size="full" data-testid="tags-outside-assignments">
              <Alert color="yellow" variant="light" title={t("tagsTab.outsideTitle")}>
                <Stack gap="xs">
                  <span>{t("tagsTab.outsideBody")}</span>
                  <MantineList size="sm">
                    {outside.map((o) => (
                      <MantineList.Item key={o.objectKey}>
                        {t("tagsTab.outsideItem", {
                          kind: scopeLabel(o.objectType),
                          name: outsideName(o),
                          domains: o.domains.join(", "),
                        })}
                      </MantineList.Item>
                    ))}
                  </MantineList>
                </Stack>
              </Alert>
            </FormField>
          )}
          <FormFooter
            onSave={handleSubmit}
            onCancel={closeForm}
            saveProps={{
              "data-testid": "tags-save",
              disabled: !formId.trim() || formScopes.length === 0,
            }}
          />
        </FormLayout>
      )}

      <ListBulkBar
        selection={selection}
        nameOf={(id) => id}
        onDelete={deleteTags}
        testId="tags-bulk"
      />
      <SortGroupTable
        selection={selection}
        testPrefix="tags"
        minWidth={640}
        rows={filtered}
        columns={[
          { key: "id", label: t("tagsTab.colId"), sortValue: (g) => g.id },
          {
            key: "description",
            label: t("tagsTab.colDescription"),
            sortValue: (g) => g.description,
          },
          {
            key: "appliesTo",
            label: t("tagsTab.colAppliesTo"),
            sortValue: (g) => g.appliesTo.join(", "),
            groupValue: (g) => g.appliesTo.join(", "),
          },
          {
            key: "assignments",
            label: t("tagsTab.colAssignments"),
            sortValue: (g) => assignmentCounts[baseTagId(g.id)] ?? 0,
          },
        ]}
        headers={[
          { col: "id" },
          { col: "description" },
          { col: "appliesTo" },
          { col: "assignments" },
          t("tagsTab.colActions"),
        ]}
        colSpan={5}
        rowKey={(tag) => tag.id}
        empty={t("tagsTab.empty")}
        page={{ index: safePage - 1, size: PAGE_SIZE }}
        onGroupedChange={setGrouped}
        render={(tag) => [
          <ListRow key={tag.id} testId={`tags-row-${tag.id}`} selected={tag.id === linkedTag}>
            <Table.Td>
              {tag.id}
              {(tag.required ||
                tag.reasonPolicy !== "optional" ||
                tag.expiresPolicy !== "optional" ||
                tag.requiresCitation ||
                tag.needsReview) && (
                <Group gap={4} mt={2}>
                  {tag.required && (
                    <Badge size="xs" variant="light" color="gray" data-testid="tags-required-badge">
                      {t("tagsTab.requiredLabel")}
                    </Badge>
                  )}
                  {tag.requiresCitation && (
                    <Badge size="xs" variant="light" color="gray">
                      {t("tagCitations.requiresCitationBadge")}
                    </Badge>
                  )}
                  {tag.needsReview && (
                    <Tip text={t("tagCitations.definitionReviewHelp")}>
                      <Badge
                        size="xs"
                        variant="light"
                        color="orange"
                        data-testid={`tags-needs-review-${tag.id}`}
                      >
                        {t("tagCitations.needsReview")}
                      </Badge>
                    </Tip>
                  )}
                  {tag.reasonPolicy !== "optional" && (
                    <Badge size="xs" variant="light" color="gray">
                      {t("tagsTab.reasonPolicyBadge", {
                        policy: policyLabel(tag.reasonPolicy),
                      })}
                    </Badge>
                  )}
                  {tag.expiresPolicy !== "optional" && (
                    <Badge size="xs" variant="light" color="gray">
                      {t("tagsTab.expiresPolicyBadge", {
                        policy: policyLabel(tag.expiresPolicy),
                      })}
                    </Badge>
                  )}
                </Group>
              )}
            </Table.Td>
            <Table.Td>{tag.description || "—"}</Table.Td>
            <Table.Td>
              <Group gap={4}>
                {tag.appliesTo.map((s) => (
                  <Badge key={s} size="xs" variant="light">
                    {scopeLabel(s)}
                  </Badge>
                ))}
              </Group>
            </Table.Td>
            {/* REQ-1443: a derived tag is computed off each table's registration and is never
                    assigned, so a count of stored assignments would read as "unused" when the tag
                    may hold on every table in the estate. */}
            <Table.Td>
              {tag.derived ? (
                <Badge size="xs" variant="light" color="teal" data-testid="tags-derived">
                  {t("tagsTab.derivedTag")}
                </Badge>
              ) : (
                (assignmentCounts[tag.id] ?? 0)
              )}
            </Table.Td>
            <Table.Td>
              <Group gap="xs">
                {/* REQ-1467: the value list is editable even on a system tag — the tag itself
                        is code-defined, but which values it admits is the maintainer's call. */}
                {tag.paramPolicy === "required" && (
                  <ActionIcon
                    variant="subtle"
                    aria-label={t("tagsTab.editValues", { id: tag.id })}
                    data-testid={`tags-values-${tag.id}`}
                    onClick={() => toggleValues(tag.id)}
                  >
                    <List size={14} />
                  </ActionIcon>
                )}
                {tag.isSystem ? (
                  <Tip text={t(tag.derived ? "tagsTab.derivedTagHelp" : "tagsTab.systemTag")} named>
                    <Lock
                      size={13}
                      aria-label={t(tag.derived ? "tagsTab.derivedTagHelp" : "tagsTab.systemTag")}
                    />
                  </Tip>
                ) : (
                  <>
                    <ActionIcon
                      variant="subtle"
                      aria-label={t("tagsTab.editTag", { id: tag.id })}
                      onClick={() => openEdit(tag)}
                    >
                      <Pencil size={14} />
                    </ActionIcon>
                    <ActionIcon
                      variant="subtle"
                      color="red"
                      aria-label={t("tagsTab.deleteTag", { id: tag.id })}
                      data-testid={`tags-delete-${tag.id}`}
                      onClick={() => handleDelete(tag)}
                    >
                      <Trash2 size={14} />
                    </ActionIcon>
                  </>
                )}
              </Group>
            </Table.Td>
          </ListRow>,
          valuesTagId === tag.id && (
            <Table.Tr
              className="list-expand"
              key={`${tag.id}-values`}
              data-testid={`tags-values-row-${tag.id}`}
            >
              <Table.Td colSpan={5}>
                <Stack gap="xs">
                  <Group gap={4}>
                    {tag.paramValues.length === 0 && (
                      <span style={{ fontSize: "0.8rem", color: "var(--text-muted)" }}>
                        {t("tagsTab.noValues")}
                      </span>
                    )}
                    {tag.paramValues.map((v) => (
                      <Tip key={v.value} text={v.description}>
                        <Badge
                          size="sm"
                          variant="light"
                          rightSection={
                            <ActionIcon
                              size="xs"
                              variant="transparent"
                              color="gray"
                              aria-label={t("tagsTab.deleteValue", { value: v.value })}
                              data-testid={`tags-value-delete-${tag.id}-${v.value}`}
                              onClick={() => handleDeleteValue(tag.id, v.value)}
                            >
                              <X size={10} />
                            </ActionIcon>
                          }
                        >
                          {v.value}
                        </Badge>
                      </Tip>
                    ))}
                  </Group>
                  <Group gap="xs" align="flex-end">
                    <TextInput
                      size="xs"
                      label={t("tagsTab.newValueLabel")}
                      value={newValue}
                      onChange={(e) => setNewValue(e.currentTarget.value)}
                      data-testid={`tags-new-value-${tag.id}`}
                    />
                    <TextInput
                      size="xs"
                      label={t("tagsTab.newValueDescriptionLabel")}
                      value={newValueDescription}
                      onChange={(e) => setNewValueDescription(e.currentTarget.value)}
                      data-testid={`tags-new-value-desc-${tag.id}`}
                    />
                    <Button
                      size="compact-xs"
                      disabled={newValue.trim() === ""}
                      onClick={() => void handleAddValue(tag.id)}
                      data-testid={`tags-add-value-${tag.id}`}
                    >
                      {t("tagsTab.addValue")}
                    </Button>
                  </Group>
                </Stack>
              </Table.Td>
            </Table.Tr>
          ),
        ]}
      />
      {totalPages > 1 && !grouped && (
        <Group justify="flex-end">
          <Pagination
            total={totalPages}
            value={safePage}
            onChange={setPage}
            size="sm"
            aria-label={t("tagsTab.pagination")}
          />
        </Group>
      )}
    </Stack>
  );
}
