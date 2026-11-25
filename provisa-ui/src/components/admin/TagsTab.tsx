// Copyright (c) 2026 Kenneth Stott
// Canary: de91a53c-421b-44f1-b5e6-e6a80adebb9b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useMemo, useState } from "react";
import { useRememberedDetail } from "../list/useDetailParam";
import { useUrlFilter, useUrlTab } from "../list/useUrlFilter";
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
  Select,
  Stack,
  Switch,
  Table,
  Tabs,
  Text,
  TextInput,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Lock, Pencil, Tag as TagIcon, X } from "lucide-react";
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
import { PageTitle } from "../PageTitle";
import { ListRow } from "../list/ListTable";
import { SortGroupTable } from "../list/SortGroupTable";
import { useListSelection } from "../list/listSelection";
import { ListBulkBar } from "../list/ListBulkBar";
import { PanelActions, SplitPane } from "../list/SplitPane";
import { PanelDelete } from "../list/PanelDelete";
import { DetailFields } from "../detail/DetailFields";
import { FormChecks, FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { Tip } from "../Tip";
import { TagCitations } from "../TagCitations";
import { useDependentsDialog } from "../../hooks/useDependentsDialog";
import { matchesQuery } from "../../lib/searchItems";
import { MultiSelect } from "../MultiSelect";
import { useDomains } from "../../hooks/useAdminQueries";
import { useDomainFilter } from "../../context/DomainFilterContext";
import { assignableDomainIds } from "../../lib/internalDomains";
import { fetchContextTags, type ContextTagUse } from "../../api/contextViews";
import { ContextPlaces } from "../ContextPlaces";
import { EntityLink } from "../EntityLink";

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

// REQ-1374: one org-level tag registry, viewed one scope at a time. The scope tabs filter the
// single list; a multi-scope tag appears under each scope its appliesTo names.
// REQ-2277: the standard page anatomy -- the header (title, filter, add), the scope tabs, and the
// list with its detail panel: a row opens ?tag=<id> read, with Edit and Delete in the panel's head;
// the add opens the same panel. The scope and the filter are in the URL.
// REQ-2259: ``readOnly`` is the Context group's Tags page: no control that writes, and beside each
// tag its tables and columns and what depends on it. Editing stays in Admin.
export function TagsTab({ readOnly = false, title }: { readOnly?: boolean; title: string }) {
  const { t } = useTranslation();
  const [uses, setUses] = useState<Map<string, ContextTagUse> | null>(null);
  const [usesError, setUsesError] = useState<string | null>(null);
  useEffect(() => {
    if (!readOnly) return;
    fetchContextTags().then(
      (r) => setUses(new Map(r.tags.map((u) => [u.id, u]))),
      (e: Error) => setUsesError(e.message),
    );
  }, [readOnly]);
  const { tags, loading: tagsLoading } = useTags();
  const { tagAssignments } = useTagAssignments();
  const { createTag, updateTag } = useSaveTag();
  const { deleteTag } = useDeleteTag();
  const refusal = useDependentsDialog();
  const { upsertTagParamValue } = useUpsertTagParamValue();
  const { deleteTagParamValue } = useDeleteTagParamValue();
  const { citeDefinition } = useTagCitations();
  const { domains } = useDomains();
  const { domainsEnabled } = useDomainFilter();

  // `?tag=<id>` (a graph's or a refused delete's link, detailHref) opens that tag's panel; the one
  // left open is open again on coming back (REQ-2260).
  const detail = useRememberedDetail("tags", tagsLoading ? null : tags.map((g) => g.id));
  const openTag = detail.id === null ? undefined : tags.find((g) => g.id === detail.id);
  // A link naming a tag and no scope shows the tag under the first scope it applies to.
  const [scope, setScope] = useUrlTab<TagObjectType>(
    "scope",
    SCOPES,
    openTag !== undefined && openTag.appliesTo.length > 0 ? openTag.appliesTo[0] : SCOPES[0],
  );
  const [search, setSearch] = useUrlFilter();
  const [page, setPage] = useState(1);
  // The panel holds the create form while it is open; else the open tag, read or in its form.
  const [creating, setCreating] = useState(false);
  const [editing, setEditing] = useState(false);
  const editingId = editing && openTag !== undefined ? openTag.id : null;
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
  // REQ-1467: the draft new value/description of the open tag's value list.
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
    setCreating(false);
    setEditing(false);
    resetForm();
  };

  const closePanel = () => {
    closeForm();
    detail.close();
  };

  const openCreate = () => {
    // One thing in the panel at a time: the form replaces whatever tag was open.
    detail.close();
    setEditing(false);
    resetForm();
    setFormScopes([scope]);
    setCreating(true);
  };

  const openRow = (id: string) => {
    closeForm();
    setNewValue("");
    setNewValueDescription("");
    if (detail.id === id) detail.close();
    else detail.open(id);
  };

  const openEdit = (tag: Tag) => {
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
    setEditing(true);
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
    const id = formId.trim();
    // A new tag is created; the open one updated (a create refuses an id that exists).
    const save = editingId === null ? createTag : updateTag;
    const result = await save(
      id,
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
      notifications.show({ color: "green", message: t("tagsTab.saved", { id }) });
      // The saved tag is the panel's, read -- or, where the save narrowed its domains, still in its
      // form listing the assignments now outside them (REQ-2208); they stay.
      const left = (result.params as { outside?: TagOutsideAssignment[] } | null)?.outside ?? [];
      setCreating(false);
      detail.open(id);
      if (left.length > 0) {
        setEditing(true);
        setOutside(left);
        return;
      }
      setEditing(false);
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

  // REQ-2198: the panel's Delete (PanelDelete) shows the delete's progress and says why one failed.
  const handleDelete = async (tag: Tag) => {
    const result = await deleteTag(tag.id);
    // REQ-1384: a tag that carries a row filter is not deleted; the dialog names the filter.
    if (refusal.refused(result, tag.id)) return false;
    if (!result.success) throw new Error(result.message);
    notifications.show({ message: t("tagsTab.deleted", { id: tag.id }) });
  };

  const form = (
    <FormLayout data-testid="tags-form">
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
  );

  // The tag read: every value its form sets, under the form's labels, then what it is used by.
  const readView = (tag: Tag) => (
    <Stack gap="sm" data-testid={`tags-detail-${tag.id}`}>
      {!readOnly && (
        <PanelActions>
          <fieldset className="panel-action-set">
            <Group gap="xs">
              {tag.isSystem ? (
                <Tip text={t(tag.derived ? "tagsTab.derivedTagHelp" : "tagsTab.systemTag")} named>
                  <Lock
                    size={13}
                    aria-label={t(tag.derived ? "tagsTab.derivedTagHelp" : "tagsTab.systemTag")}
                  />
                </Tip>
              ) : (
                <>
                  <Tip text={t("tagsTab.editTag", { id: tag.id })} named>
                    <ActionIcon
                      aria-label={t("tagsTab.editTag", { id: tag.id })}
                      data-testid={`tags-edit-${tag.id}`}
                      onClick={(e) => {
                        e.stopPropagation();
                        openEdit(tag);
                      }}
                    >
                      <Pencil size={14} />
                    </ActionIcon>
                  </Tip>
                  <PanelDelete
                    itemKey={tag.id}
                    title={t("tagsTab.deleteTag", { id: tag.id })}
                    consequence={t("tagsTab.deleteConfirm", { id: tag.id })}
                    ariaLabel={t("tagsTab.deleteTag", { id: tag.id })}
                    testId={`tags-delete-${tag.id}`}
                    onDelete={() => handleDelete(tag)}
                  />
                </>
              )}
            </Group>
          </fieldset>
        </PanelActions>
      )}
      <DetailFields
        testId={`tags-read-${tag.id}`}
        fields={[
          {
            label: t("tagsTab.idLabel"),
            value: readOnly ? (
              <EntityLink kind="tag" id={tag.id} testId={`tags-link-${tag.id}`}>
                {tag.id}
              </EntityLink>
            ) : (
              tag.id
            ),
          },
          { label: t("tagsTab.descriptionLabel"), value: tag.description },
          {
            label: t("tagsTab.appliesToLabel"),
            value: tag.appliesTo.map(scopeLabel).join(", "),
          },
          ...(domainsEnabled
            ? [
                {
                  label: t("sourceFormFieldsExtended.allowedDomains"),
                  value: tag.allowedDomains.join(", "),
                },
              ]
            : []),
          { label: t("tagsTab.reasonPolicyLabel"), value: policyLabel(tag.reasonPolicy) },
          { label: t("tagsTab.expiresPolicyLabel"), value: policyLabel(tag.expiresPolicy) },
          {
            label: t("tagsTab.paramPolicyLabel"),
            value: t(`tagsTab.paramPolicy_${tag.paramPolicy}`),
          },
          {
            label: t("tagCitations.requiresCitationLabel"),
            value: tag.requiresCitation ? t("detailFields.yes") : t("detailFields.no"),
          },
          {
            label: t("tagsTab.requiredLabel"),
            value: tag.required ? t("detailFields.yes") : t("detailFields.no"),
          },
          {
            label: t("tagCitations.definitionLabel"),
            value: (
              <TagCitations.readValue citations={tag.citations} testId="tags-defined-by-read" />
            ),
          },
          {
            label: t("tagsTab.colAssignments"),
            value: tag.derived ? t("tagsTab.derivedTag") : String(assignmentCounts[tag.id] ?? 0),
          },
        ]}
      />
      {/* REQ-1467: the value list is editable even on a system tag -- the tag itself is
          code-defined, but which values it admits is the maintainer's call. */}
      {tag.paramPolicy === "required" && (
        <Stack gap="xs" data-testid={`tags-values-${tag.id}`}>
          <Text size="sm" fw={500} c="dimmed">
            {t("tagsTab.editValues", { id: tag.id })}
          </Text>
          <Group gap={4}>
            {tag.paramValues.length === 0 && (
              <Text size="xs" c="dimmed">
                {t("tagsTab.noValues")}
              </Text>
            )}
            {tag.paramValues.map((v) => (
              <Tip key={v.value} text={v.description}>
                <Badge
                  size="sm"
                  variant="light"
                  rightSection={
                    readOnly ? undefined : (
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
                    )
                  }
                >
                  {v.value}
                </Badge>
              </Tip>
            ))}
          </Group>
          {!readOnly && (
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
          )}
        </Stack>
      )}
    </Stack>
  );

  const panelTitle = creating ? t("tagsTab.createButton") : openTag !== undefined ? openTag.id : "";
  const panelBody = creating
    ? form
    : openTag !== undefined
      ? editing
        ? form
        : readView(openTag)
      : null;

  return (
    <>
      {refusal.dialog}
      <div className="page-header">
        <PageTitle icon={TagIcon}>{title}</PageTitle>
        <FilterInput
          value={search}
          onChange={(v) => {
            setSearch(v);
            setPage(1);
          }}
          placeholder={t("tagsTab.filterPlaceholder")}
        />
        <div className="page-actions">
          {!readOnly && (
            <Button
              variant={creating ? "outline" : "filled"}
              onClick={() => (creating ? closeForm() : openCreate())}
              data-testid="tags-create-button"
              aria-label={creating ? t("tagsTab.closeForm") : undefined}
            >
              {creating ? <X size={14} /> : t("tagsTab.addTag")}
            </Button>
          )}
          <HelpBubble
            title={t("tagsTab.purposeTitle")}
            paragraphs={[t("tagsTab.purposeBody"), t("tagsTab.purposeAdd")]}
            ariaLabel={t("tagsTab.purposeAria")}
            testId="tags-purpose-help"
          />
        </div>
      </div>

      <Tabs
        value={scope}
        onChange={(v) => {
          if (v === null) return;
          setScope(v as TagObjectType);
          setPage(1);
        }}
        mb="sm"
      >
        <Tabs.List aria-label={t("tagsTab.colAppliesTo")} data-testid="tags-scope-bar">
          {SCOPES.map((s) => (
            <Tabs.Tab key={s} value={s} data-testid={`tags-scope-${s}`}>
              {scopeLabel(s)}
            </Tabs.Tab>
          ))}
        </Tabs.List>
      </Tabs>

      {usesError && (
        <Alert color="red" data-testid="tags-uses-error">
          {usesError}
        </Alert>
      )}
      <SplitPane
        testId="tags-split"
        storageKey="provisa.tags.detailWidth"
        title={panelTitle}
        onClose={closePanel}
        // Escape closes a tag that is only being looked at; it never drops what is typed.
        closeOnEscape={!creating && !editing}
        labels={{
          panel: t("tagsTab.heading"),
          resize: t("tablesPage.resizeDetail"),
          expand: t("tablesPage.expandDetail"),
          restore: t("tablesPage.restoreDetail"),
          close: t("tablesPage.closeDetail"),
          backToList: t("tablesPage.backToList"),
        }}
        list={
          <>
            {!readOnly && (
              <ListBulkBar
                selection={selection}
                nameOf={(id) => id}
                onDelete={deleteTags}
                testId="tags-bulk"
              />
            )}
            <SortGroupTable
              selection={readOnly ? undefined : selection}
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
                ...(readOnly
                  ? [t("contextViews.colWhereApplied"), t("contextViews.colDependents")]
                  : []),
              ]}
              colSpan={readOnly ? 6 : 4}
              rowKey={(tag) => tag.id}
              empty={t("tagsTab.empty")}
              page={{ index: safePage - 1, size: PAGE_SIZE }}
              onGroupedChange={setGrouped}
              render={(tag) => [
                <ListRow
                  key={tag.id}
                  testId={`tags-row-${tag.id}`}
                  selected={!creating && tag.id === detail.id}
                  onClick={() => openRow(tag.id)}
                >
                  <Table.Td>
                    {tag.id}
                    {(tag.required ||
                      tag.reasonPolicy === "required" ||
                      tag.expiresPolicy === "required" ||
                      tag.requiresCitation ||
                      tag.needsReview) && (
                      <Group gap={4} mt={2}>
                        {tag.required && (
                          <Badge
                            size="xs"
                            variant="light"
                            color="gray"
                            data-testid="tags-required-badge"
                          >
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
                        {tag.reasonPolicy === "required" && (
                          <Badge size="xs" variant="light" color="gray">
                            {t("tagsTab.reasonPolicyBadge", {
                              policy: policyLabel(tag.reasonPolicy),
                            })}
                          </Badge>
                        )}
                        {tag.expiresPolicy === "required" && (
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
                  {/* REQ-1443: a derived tag is computed off each table's registration and is
                      never assigned, so a count of stored assignments would read as "unused" when
                      the tag may hold on every table in the estate. */}
                  <Table.Td>
                    {tag.derived ? (
                      <Badge size="xs" variant="light" color="teal" data-testid="tags-derived">
                        {t("tagsTab.derivedTag")}
                      </Badge>
                    ) : (
                      (assignmentCounts[tag.id] ?? 0)
                    )}
                  </Table.Td>
                  {readOnly && <ContextColumns tag={tag} uses={uses} />}
                </ListRow>,
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
          </>
        }
      >
        {panelBody}
      </SplitPane>
    </>
  );
}

/** REQ-2259: a tag's tables and columns and what depends on it, on the Context Tags page. A derived
 * tag is computed off each table's registration and never assigned, so it is applied nowhere by
 * name. */
function ContextColumns({ tag, uses }: { tag: Tag; uses: Map<string, ContextTagUse> | null }) {
  const { t } = useTranslation();
  if (uses === null) {
    return (
      <>
        <Table.Td />
        <Table.Td />
      </>
    );
  }
  const use = uses.get(tag.id);
  if (use === undefined) throw new Error(`tag ${tag.id} is missing from the context read`);
  const none = use.rowFilters === 0 && use.classificationKinds.length === 0;
  return (
    <>
      <Table.Td>
        <ContextPlaces
          places={use.usages}
          hidden={use.hiddenUsages}
          testId={`tags-where-${tag.id}`}
        />
      </Table.Td>
      <Table.Td data-testid={`tags-dependents-${tag.id}`}>
        {none ? (
          <Text size="xs" c="dimmed">
            {t("contextViews.noDependents")}
          </Text>
        ) : (
          <Stack gap={2}>
            {use.rowFilters > 0 && (
              <Text size="xs">{t("contextViews.rowFilters", { count: use.rowFilters })}</Text>
            )}
            {use.classificationKinds.map((k) => (
              <Text size="xs" key={k}>
                <EntityLink
                  kind="classification"
                  id={k}
                  testId={`tags-classification-${tag.id}-${k}`}
                >
                  {t("contextViews.classifiedAs", { kind: t(`classificationKinds.kind.${k}`) })}
                </EntityLink>
              </Text>
            ))}
          </Stack>
        )}
      </Table.Td>
    </>
  );
}
