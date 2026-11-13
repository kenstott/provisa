// Copyright (c) 2026 Kenneth Stott
// Canary: 7e3a1c9f-52b8-4d6e-9a04-c1f8b3d7e246
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Badge,
  Button,
  Checkbox,
  Group,
  Popover,
  Select,
  Stack,
  Text,
  TextInput,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Pencil, Tag as TagIcon } from "lucide-react";
import type { Tag, TagAssignment, TagObjectType } from "../types/admin";
import { TAG_PARAM_SEPARATOR, baseTagId, tagParam } from "../types/admin";
import {
  useAssignTag,
  useTagAssignments,
  useTagCitations,
  useTags,
  useUnassignTag,
} from "../hooks/useTagQueries";
import { Tip, TipSurface } from "./Tip";
import { ColumnPiiForm } from "./ColumnPiiForm";
import { CancelButton } from "./form/CancelButton";
import { CitationLink, CitationSection, CitationState, TagCitations } from "./TagCitations";

// REQ-1377: one reusable tag-pill + tag-picker control shared by every taggable object surface
// (source, table, column, relationship, command; a data product, REQ-1634; a glossary term,
// REQ-2145; a row rule, a role, a metric, and a reference library document and each of its
// sections, REQ-2208).
// REQ-1375: each assignment may carry a reason and a planned-removal date;
// the tag's reasonPolicy/expiresPolicy drive whether each field is shown
// (not "hidden") and demanded ("required").
interface TagControlProps {
  objectType: TagObjectType;
  sourceId?: string;
  tableId?: number;
  columnName?: string;
  relationshipId?: string;
  commandName?: string;
  productId?: string;
  termId?: number;
  documentId?: number;
  /** A section: its heading, or its page when it has none. */
  sectionHeading?: string | null;
  sectionPage?: number | null;
  /** REQ-2208: the row rule, role or metric the tags are on. */
  rlsRuleId?: number;
  roleId?: string;
  metricName?: string;
  readOnly?: boolean;
  /** Called after a tag is put on or taken off the object, for a surface that also reads them. */
  onChanged?: () => void;
  /** A column's: the glossary term it is pii through (REQ-2145), shown on its pii pill. */
  piiTerm?: string | null;
}

const DEPRECATED_TAG_ID = "deprecated";
const PII_TAG_ID = "pii";

function pillColor(tag: Tag): string {
  if (tag.isSystem) return tag.id === DEPRECATED_TAG_ID ? "red" : "indigo";
  return "gray";
}

function isPastDate(isoDate: string): boolean {
  // ISO yyyy-mm-dd strings compare correctly lexicographically.
  return isoDate < new Date().toISOString().slice(0, 10);
}

export function TagControl({
  objectType,
  sourceId,
  tableId,
  columnName,
  relationshipId,
  commandName,
  productId,
  termId,
  documentId,
  sectionHeading,
  sectionPage,
  rlsRuleId,
  roleId,
  metricName,
  readOnly,
  onChanged,
  piiTerm,
}: TagControlProps) {
  const { t } = useTranslation();
  const { tags } = useTags();
  const { tagAssignments } = useTagAssignments();
  const { assignTag } = useAssignTag();
  const { unassignTag } = useUnassignTag();
  const { citeAssignment, unciteAssignment } = useTagCitations();

  // Which tag's inline reason/expiry form is open in the picker, plus its draft values.
  const [editingTagId, setEditingTagId] = useState<string | null>(null);
  const [reason, setReason] = useState("");
  const [expiresOn, setExpiresOn] = useState("");
  // REQ-1467: the chosen parameter value for a parameterized tag, e.g. "customer" for `entity`.
  const [paramValue, setParamValue] = useState<string | null>(null);

  const matches = (a: TagAssignment): boolean => {
    if (a.objectType !== objectType) return false;
    switch (objectType) {
      case "source":
        return a.sourceId === sourceId;
      case "table":
        return a.tableId === tableId;
      case "column":
        return a.tableId === tableId && a.columnName === columnName;
      case "relationship":
        return a.relationshipId === relationshipId;
      case "command":
        return a.commandName === commandName;
      case "product":
        return a.productId === productId;
      case "term":
        return a.termId === termId;
      case "rls_rule":
        return a.rlsRuleId === rlsRuleId;
      case "role":
        return a.roleId === roleId;
      case "metric":
        return a.metricName === metricName;
      case "document":
        return a.documentId === documentId;
      case "section":
        return (
          a.documentId === documentId &&
          (a.sectionHeading ?? null) === (sectionHeading ?? null) &&
          (a.sectionPage ?? null) === (sectionPage ?? null)
        );
    }
  };

  const identifier = {
    source: sourceId,
    table: tableId,
    column: `${tableId}-${columnName}`,
    relationship: relationshipId,
    command: commandName,
    product: productId,
    term: termId,
    rls_rule: rlsRuleId,
    role: roleId,
    metric: metricName,
    document: documentId,
    section: `${documentId}-${sectionHeading ?? `p${sectionPage}`}`,
  }[objectType];

  const objectAssignments = tagAssignments.filter(matches);
  // REQ-2145: pii on a column goes through its own flow -- on its term, or on the column alone.
  const columnPii = (tag: Tag) => objectType === "column" && tag.id === PII_TAG_ID;
  // REQ-1467: keyed by base id — an assignment's tagId carries its parameter ("entity:customer"),
  // and the registry tag it belongs to is the base. (base_tag_id, object) is unique server-side,
  // so one object never has two entries under one key.
  const assignmentByTagId = new Map(objectAssignments.map((a) => [baseTagId(a.tagId), a]));
  // A column pii through its term shows the tag as its own pills do, saying where it comes from.
  const assignedTags = tags.filter(
    (tag) => assignmentByTagId.has(tag.id) || (tag.id === PII_TAG_ID && !!piiTerm),
  );
  // REQ-1443: a derived tag reports what the object's own registration already says, so there is
  // nothing to check or clear — the server refuses the assignment and offering it would put a
  // control on screen whose only outcome is an error.
  const applicableTags = tags.filter((tag) => tag.appliesTo.includes(objectType) && !tag.derived);

  const buildInput = (tagId: string): TagAssignment => ({
    tagId,
    objectType,
    sourceId: objectType === "source" ? sourceId : undefined,
    tableId: objectType === "table" || objectType === "column" ? tableId : undefined,
    columnName: objectType === "column" ? columnName : undefined,
    relationshipId: objectType === "relationship" ? relationshipId : undefined,
    commandName: objectType === "command" ? commandName : undefined,
    productId: objectType === "product" ? productId : undefined,
    termId: objectType === "term" ? termId : undefined,
    documentId: objectType === "document" || objectType === "section" ? documentId : undefined,
    sectionHeading: objectType === "section" ? sectionHeading : undefined,
    sectionPage: objectType === "section" ? sectionPage : undefined,
    rlsRuleId: objectType === "rls_rule" ? rlsRuleId : undefined,
    roleId: objectType === "role" ? roleId : undefined,
    metricName: objectType === "metric" ? metricName : undefined,
  });

  const closeForm = () => {
    setEditingTagId(null);
    setReason("");
    setExpiresOn("");
    setParamValue(null);
  };

  const openForm = (tagId: string, existing?: TagAssignment) => {
    setEditingTagId(tagId);
    setReason(existing?.reason ?? "");
    setExpiresOn(existing?.expiresOn ?? "");
    setParamValue(existing ? tagParam(existing.tagId) : null);
  };

  const handleUnassign = async (tagId: string) => {
    const result = await unassignTag(buildInput(tagId));
    if (!result.success) {
      notifications.show({ color: "red", message: result.message });
      return;
    }
    onChanged?.();
  };

  const handleCheck = (tag: Tag, checked: boolean) => {
    if (checked) {
      openForm(tag.id, assignmentByTagId.get(tag.id));
      return;
    }
    // pii through the column's term is taken off the term, or the column moved: the picker asks.
    if (columnPii(tag) && piiTerm && !assignmentByTagId.has(tag.id)) {
      openForm(tag.id);
      return;
    }
    if (editingTagId === tag.id) closeForm();
    if (assignmentByTagId.has(tag.id)) void handleUnassign(tag.id);
  };

  const handleApply = async (tag: Tag) => {
    // REQ-1467: a `required` policy means the assignment names the parameter, never the bare tag —
    // the Apply button stays disabled until a value is picked, so paramValue is set by here.
    const tagId =
      tag.paramPolicy === "required" ? `${tag.id}${TAG_PARAM_SEPARATOR}${paramValue}` : tag.id;
    const result = await assignTag({
      ...buildInput(tagId),
      reason: reason || null,
      expiresOn: expiresOn || null,
    });
    if (!result.success) {
      notifications.show({ color: "red", message: result.message });
      return;
    }
    closeForm();
    onChanged?.();
  };

  return (
    <Group
      gap="0.25rem"
      wrap="wrap"
      display="inline-flex"
      data-testid={`tag-control-${objectType}-${identifier}`}
      onClick={(e) => e.stopPropagation()}
    >
      {assignedTags.map((tag) => {
        const assignment = assignmentByTagId.get(tag.id);
        const overdue =
          tag.id === DEPRECATED_TAG_ID &&
          !!assignment?.expiresOn &&
          isPastDate(assignment.expiresOn);
        return (
          <Group key={tag.id} gap="0.15rem" wrap="nowrap" display="inline-flex">
            {/* The chip opens its detail: where the tag is defined, linked into the Library, and
                what this assignment cites. */}
            <Popover withinPortal position="bottom-start" shadow="md">
              <Popover.Target>
                <TipSurface
                  content={
                    <Stack gap={0}>
                      <Text size="xs">{tag.description}</Text>
                      {assignment?.reason &&
                        (tag.id === PII_TAG_ID && objectType === "column" ? (
                          <Text size="xs">
                            {t("tagControl.piiSetHere", { reason: assignment.reason })}
                          </Text>
                        ) : (
                          <Text size="xs">{assignment.reason}</Text>
                        ))}
                      {tag.id === PII_TAG_ID && piiTerm && (
                        <Text size="xs">{t("tagControl.piiViaTerm", { term: piiTerm })}</Text>
                      )}
                      {assignment?.expiresOn && (
                        <Text size="xs">
                          {t("tagControl.plannedRemoval", { date: assignment.expiresOn })}
                        </Text>
                      )}
                      {tag.citations.map((c) => (
                        <Text
                          size="xs"
                          key={`${c.documentId}-${c.sectionHeading}-${c.sectionPage}`}
                        >
                          {t("tagCitations.definedBy", { title: c.documentTitle })} ·{" "}
                          <CitationSection c={c} />
                        </Text>
                      ))}
                    </Stack>
                  }
                >
                  <Badge
                    size="xs"
                    variant={overdue ? "filled" : "light"}
                    color={pillColor(tag)}
                    component="button"
                    type="button"
                    style={{ cursor: "pointer" }}
                    data-testid={`tag-pill-${tag.id}`}
                  >
                    {/* REQ-1467: the assignment's id carries the parameter ("entity:customer"); the
                      registry id alone would show every parameterized pill as the same word. */}
                    {assignment ? assignment.tagId : tag.id}
                  </Badge>
                </TipSurface>
              </Popover.Target>
              <Popover.Dropdown onClick={(e) => e.stopPropagation()}>
                <Stack gap="xs" data-testid={`tag-detail-${tag.id}`}>
                  <Text size="xs" fw={600}>
                    {t("tagCitations.definedByLabel")}
                  </Text>
                  {tag.citations.length === 0 && (
                    <Text size="xs" c="dimmed">
                      {t("tagCitations.none")}
                    </Text>
                  )}
                  {tag.citations.map((c) => (
                    <Group
                      key={`${c.documentId}-${c.sectionHeading}-${c.sectionPage}`}
                      gap="0.25rem"
                    >
                      <CitationLink c={c} testId={`tag-definition-link-${tag.id}`} />
                      <CitationState c={c} />
                    </Group>
                  ))}
                  {assignment && (
                    <>
                      <Text size="xs" fw={600}>
                        {t("tagCitations.citedHere")}
                      </Text>
                      <TagCitations
                        testId={`tag-citations-${tag.id}`}
                        citations={assignment.citations ?? []}
                        readOnly={readOnly}
                        onAdd={(place) => citeAssignment(buildInput(assignment.tagId), place)}
                        onRemove={(c) => unciteAssignment(buildInput(assignment.tagId), c)}
                        onConfirm={(c) => citeAssignment(buildInput(assignment.tagId), c)}
                      />
                    </>
                  )}
                </Stack>
              </Popover.Dropdown>
            </Popover>
            {assignment?.citationGap && (
              <Tip text={t("tagCitations.gapHelp")}>
                <Badge size="xs" variant="light" color="yellow" data-testid={`tag-gap-${tag.id}`}>
                  {t("tagCitations.gap")}
                </Badge>
              </Tip>
            )}
            {assignment?.needsReview && (
              <Tip text={t("tagCitations.reviewHelp")}>
                <Badge
                  size="xs"
                  variant="light"
                  color="orange"
                  data-testid={`tag-review-${tag.id}`}
                >
                  {t("tagCitations.needsReview")}
                </Badge>
              </Tip>
            )}
          </Group>
        );
      })}
      {!readOnly && (
        <Popover withinPortal position="bottom-start" shadow="md">
          <Popover.Target>
            <Tip text={t("tagControl.editTags")} named>
              <ActionIcon
                variant="subtle"
                size="xs"
                aria-label={t("tagControl.editTags")}
                data-testid="tag-picker-toggle"
              >
                <TagIcon size={12} />
              </ActionIcon>
            </Tip>
          </Popover.Target>
          <Popover.Dropdown onClick={(e) => e.stopPropagation()}>
            <Stack gap="0.35rem">
              {applicableTags.length === 0 && (
                <span style={{ fontSize: "0.8rem", color: "var(--text-muted)" }}>
                  {t("tagControl.noTags")}
                </span>
              )}
              {applicableTags.map((tag) => {
                const assignment = assignmentByTagId.get(tag.id);
                const isEditing = editingTagId === tag.id;
                const reasonRequired = tag.reasonPolicy === "required";
                const expiresRequired = tag.expiresPolicy === "required";
                const ownForm = (
                  <Stack gap="0.25rem" pl="1.5rem">
                    {tag.paramPolicy === "required" && (
                      <Select
                        size="xs"
                        required
                        label={t("tagControl.paramLabel")}
                        placeholder={t("tagControl.paramPlaceholder")}
                        comboboxProps={{ withinPortal: true, transitionProps: { duration: 0 } }}
                        data={tag.paramValues.map((v) => ({
                          value: v.value,
                          label: v.description ? `${v.value} — ${v.description}` : v.value,
                        }))}
                        value={paramValue}
                        onChange={setParamValue}
                        data-testid={`tag-param-${tag.id}`}
                      />
                    )}
                    {tag.reasonPolicy !== "hidden" && (
                      <TextInput
                        size="xs"
                        label={t("tagControl.reasonLabel")}
                        required={reasonRequired}
                        placeholder={reasonRequired ? t("tagControl.reasonRequired") : undefined}
                        value={reason}
                        onChange={(e) => setReason(e.currentTarget.value)}
                        data-testid={`tag-reason-${tag.id}`}
                      />
                    )}
                    {tag.expiresPolicy !== "hidden" && (
                      <TextInput
                        size="xs"
                        type="date"
                        label={t("tagControl.expiresLabel")}
                        required={expiresRequired}
                        value={expiresOn}
                        onChange={(e) => setExpiresOn(e.currentTarget.value)}
                        data-testid={`tag-expires-${tag.id}`}
                      />
                    )}
                    <Group gap="0.25rem">
                      <Button
                        size="compact-xs"
                        disabled={
                          (reasonRequired && reason.trim() === "") ||
                          (expiresRequired && expiresOn.trim() === "") ||
                          (tag.paramPolicy === "required" && !paramValue)
                        }
                        onClick={() => void handleApply(tag)}
                        data-testid={`tag-apply-${tag.id}`}
                      >
                        {t("tagControl.apply")}
                      </Button>
                      <CancelButton onClick={closeForm} testId={`tag-cancel-${tag.id}`} />
                    </Group>
                  </Stack>
                );
                return (
                  <Stack key={tag.id} gap="0.25rem">
                    <Group gap="0.25rem" wrap="nowrap">
                      <Checkbox
                        size="xs"
                        label={tag.id}
                        description={tag.description}
                        checked={!!assignment || isEditing || (columnPii(tag) && !!piiTerm)}
                        onChange={(e) => handleCheck(tag, e.currentTarget.checked)}
                        data-testid={`tag-option-${tag.id}`}
                      />
                      {assignment && (
                        <Tip text={t("tagControl.editAssignment")} named>
                          <ActionIcon
                            variant="subtle"
                            size="xs"
                            aria-label={t("tagControl.editAssignment")}
                            onClick={() => openForm(tag.id, assignment)}
                            data-testid={`tag-edit-${tag.id}`}
                          >
                            <Pencil size={12} />
                          </ActionIcon>
                        </Tip>
                      )}
                    </Group>
                    {isEditing &&
                      (columnPii(tag) ? (
                        <Stack gap="0.25rem" pl="1.5rem">
                          <ColumnPiiForm
                            tableId={tableId as number}
                            columnName={columnName as string}
                            inherited={!!piiTerm && !assignment}
                            assign={assignTag}
                            unassign={unassignTag}
                            onDone={closeForm}
                            ownForm={ownForm}
                          />
                        </Stack>
                      ) : (
                        ownForm
                      ))}
                  </Stack>
                );
              })}
            </Stack>
          </Popover.Dropdown>
        </Popover>
      )}
    </Group>
  );
}
