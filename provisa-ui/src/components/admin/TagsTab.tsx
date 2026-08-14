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
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Badge,
  Button,
  Checkbox,
  Group,
  Pagination,
  SegmentedControl,
  Select,
  Stack,
  Table,
  TextInput,
  Title,
  Tooltip,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { List, Lock, Pencil, Plus, Tag as TagIcon, Trash2, X } from "lucide-react";
import {
  useTags,
  useTagAssignments,
  useUpsertTag,
  useDeleteTag,
  useUpsertTagParamValue,
  useDeleteTagParamValue,
} from "../../hooks/useTagQueries";
import type { Tag, TagFieldPolicy, TagObjectType, TagParamPolicy } from "../../types/admin";
import { baseTagId } from "../../types/admin";
import { FilterInput } from "./FilterInput";

const PAGE_SIZE = 50;

const SCOPES: TagObjectType[] = ["source", "table", "column", "relationship", "command"];

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
  const { upsertTag } = useUpsertTag();
  const { deleteTag } = useDeleteTag();
  const { upsertTagParamValue } = useUpsertTagParamValue();
  const { deleteTagParamValue } = useDeleteTagParamValue();

  const [scope, setScope] = useState<TagObjectType>("source");
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);
  const [showForm, setShowForm] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [formId, setFormId] = useState("");
  const [formDescription, setFormDescription] = useState("");
  const [formScopes, setFormScopes] = useState<string[]>([]);
  const [formReasonPolicy, setFormReasonPolicy] = useState<TagFieldPolicy>("optional");
  const [formExpiresPolicy, setFormExpiresPolicy] = useState<TagFieldPolicy>("optional");
  const [formParamPolicy, setFormParamPolicy] = useState<TagParamPolicy>("none");
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

  const q = search.toLowerCase();
  const filtered = tags
    .filter((tag) => tag.appliesTo.includes(scope))
    .filter((tag) => tag.id.toLowerCase().includes(q) || tag.description.toLowerCase().includes(q))
    .sort((a, b) => (a.isSystem === b.isSystem ? a.id.localeCompare(b.id) : a.isSystem ? -1 : 1));
  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safePage = Math.min(page, totalPages);
  const paged = filtered.slice((safePage - 1) * PAGE_SIZE, safePage * PAGE_SIZE);

  const resetForm = () => {
    setEditingId(null);
    setFormId("");
    setFormDescription("");
    setFormScopes([]);
    setFormReasonPolicy("optional");
    setFormExpiresPolicy("optional");
    setFormParamPolicy("none");
  };

  const openCreate = () => {
    if (showForm && !editingId) {
      setShowForm(false);
      resetForm();
      return;
    }
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
    setShowForm(true);
  };

  const handleSubmit = async () => {
    if (!formId.trim() || formScopes.length === 0) return;
    const result = await upsertTag(
      formId.trim(),
      formDescription.trim(),
      formScopes,
      formReasonPolicy,
      formExpiresPolicy,
      formParamPolicy,
    );
    if (result.success) {
      notifications.show({ color: "green", message: t("tagsTab.saved", { id: formId.trim() }) });
      setShowForm(false);
      resetForm();
    } else {
      notifications.show({ color: "red", message: result.message });
    }
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
    if (result.success) {
      notifications.show({ message: t("tagsTab.deleted", { id: tag.id }) });
    } else {
      notifications.show({ color: "red", message: result.message });
    }
  };

  return (
    <Stack gap="md">
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
        <Button
          variant={showForm ? "default" : "filled"}
          leftSection={<Plus size={14} />}
          onClick={openCreate}
          data-testid="tags-create-button"
        >
          {showForm ? t("tagsTab.closeForm") : t("tagsTab.addTag")}
        </Button>
      </Group>

      <SegmentedControl
        size="xs"
        value={scope}
        onChange={(v) => {
          setScope(v as TagObjectType);
          setPage(1);
        }}
        data={SCOPES.map((s) => ({ label: scopeLabel(s), value: s }))}
        data-testid="tags-scope-bar"
      />

      {showForm && (
        <Stack gap="sm" maw={480}>
          <TextInput
            label={t("tagsTab.idLabel")}
            description={t("tagsTab.idDesc")}
            placeholder={t("tagsTab.idPlaceholder")}
            value={formId}
            disabled={editingId !== null}
            onChange={(e) => setFormId(e.currentTarget.value)}
          />
          <TextInput
            label={t("tagsTab.descriptionLabel")}
            placeholder={t("tagsTab.descriptionPlaceholder")}
            value={formDescription}
            onChange={(e) => setFormDescription(e.currentTarget.value)}
          />
          <Checkbox.Group
            label={t("tagsTab.appliesToLabel")}
            description={t("tagsTab.appliesToDesc")}
            value={formScopes}
            onChange={setFormScopes}
          >
            <Group gap="md" mt="xs">
              {SCOPES.map((s) => (
                <Checkbox key={s} value={s} label={scopeLabel(s)} />
              ))}
            </Group>
          </Checkbox.Group>
          <Group gap="md" grow>
            <Select
              size="xs"
              label={t("tagsTab.reasonPolicyLabel")}
              data={policyOptions}
              value={formReasonPolicy}
              allowDeselect={false}
              onChange={(v) => setFormReasonPolicy(v as TagFieldPolicy)}
              data-testid="tags-reason-policy"
            />
            <Select
              size="xs"
              label={t("tagsTab.expiresPolicyLabel")}
              data={policyOptions}
              value={formExpiresPolicy}
              allowDeselect={false}
              onChange={(v) => setFormExpiresPolicy(v as TagFieldPolicy)}
              data-testid="tags-expires-policy"
            />
            <Select
              size="xs"
              label={t("tagsTab.paramPolicyLabel")}
              description={t("tagsTab.paramPolicyDesc")}
              data={paramPolicyOptions}
              value={formParamPolicy}
              allowDeselect={false}
              onChange={(v) => setFormParamPolicy(v as TagParamPolicy)}
              data-testid="tags-param-policy"
            />
          </Group>
          <Button
            onClick={handleSubmit}
            disabled={!formId.trim() || formScopes.length === 0}
            style={{ alignSelf: "flex-start" }}
          >
            {editingId ? t("tagsTab.saveButton") : t("tagsTab.createButton")}
          </Button>
        </Stack>
      )}

      <Table.ScrollContainer minWidth={640}>
        <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("tagsTab.colId")}</Table.Th>
              <Table.Th>{t("tagsTab.colDescription")}</Table.Th>
              <Table.Th>{t("tagsTab.colAppliesTo")}</Table.Th>
              <Table.Th>{t("tagsTab.colAssignments")}</Table.Th>
              <Table.Th>{t("tagsTab.colActions")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {filtered.length === 0 && (
              <Table.Tr>
                <Table.Td colSpan={5} ta="center" c="dimmed">
                  {t("tagsTab.empty")}
                </Table.Td>
              </Table.Tr>
            )}
            {paged.map((tag) => [
              <Table.Tr key={tag.id} data-testid={`tags-row-${tag.id}`}>
                <Table.Td>
                  {tag.id}
                  {(tag.reasonPolicy !== "optional" || tag.expiresPolicy !== "optional") && (
                    <Group gap={4} mt={2}>
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
                      <Tooltip
                        label={t(tag.derived ? "tagsTab.derivedTagHelp" : "tagsTab.systemTag")}
                      >
                        <Lock
                          size={13}
                          aria-label={t(
                            tag.derived ? "tagsTab.derivedTagHelp" : "tagsTab.systemTag",
                          )}
                        />
                      </Tooltip>
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
              </Table.Tr>,
              valuesTagId === tag.id && (
                <Table.Tr key={`${tag.id}-values`} data-testid={`tags-values-row-${tag.id}`}>
                  <Table.Td colSpan={5}>
                    <Stack gap="xs">
                      <Group gap={4}>
                        {tag.paramValues.length === 0 && (
                          <span style={{ fontSize: "0.8rem", color: "var(--text-muted)" }}>
                            {t("tagsTab.noValues")}
                          </span>
                        )}
                        {tag.paramValues.map((v) => (
                          <Badge
                            key={v.value}
                            size="sm"
                            variant="light"
                            title={v.description}
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
            ])}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
      {totalPages > 1 && (
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
