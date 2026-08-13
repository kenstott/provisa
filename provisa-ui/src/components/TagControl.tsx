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
  Stack,
  Text,
  TextInput,
  Tooltip,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Pencil, Tag as TagIcon } from "lucide-react";
import type { Tag, TagAssignment, TagObjectType } from "../types/admin";
import { useAssignTag, useTagAssignments, useTags, useUnassignTag } from "../hooks/useAdminQueries";

// REQ-1377: one reusable tag-pill + tag-picker control shared by the five
// taggable object surfaces (source, table, column, relationship, command).
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
  readOnly?: boolean;
}

const DEPRECATED_TAG_ID = "deprecated";

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
  readOnly,
}: TagControlProps) {
  const { t } = useTranslation();
  const { tags } = useTags();
  const { tagAssignments } = useTagAssignments();
  const { assignTag } = useAssignTag();
  const { unassignTag } = useUnassignTag();

  // Which tag's inline reason/expiry form is open in the picker, plus its draft values.
  const [editingTagId, setEditingTagId] = useState<string | null>(null);
  const [reason, setReason] = useState("");
  const [expiresOn, setExpiresOn] = useState("");

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
    }
  };

  const identifier =
    objectType === "source"
      ? sourceId
      : objectType === "table"
        ? tableId
        : objectType === "column"
          ? `${tableId}-${columnName}`
          : objectType === "relationship"
            ? relationshipId
            : commandName;

  const objectAssignments = tagAssignments.filter(matches);
  const assignmentByTagId = new Map(objectAssignments.map((a) => [a.tagId, a]));
  const assignedTags = tags.filter((tag) => assignmentByTagId.has(tag.id));
  const applicableTags = tags.filter((tag) => tag.appliesTo.includes(objectType));

  const buildInput = (tagId: string): TagAssignment => ({
    tagId,
    objectType,
    sourceId: objectType === "source" ? sourceId : undefined,
    tableId: objectType === "table" || objectType === "column" ? tableId : undefined,
    columnName: objectType === "column" ? columnName : undefined,
    relationshipId: objectType === "relationship" ? relationshipId : undefined,
    commandName: objectType === "command" ? commandName : undefined,
  });

  const closeForm = () => {
    setEditingTagId(null);
    setReason("");
    setExpiresOn("");
  };

  const openForm = (tagId: string, existing?: TagAssignment) => {
    setEditingTagId(tagId);
    setReason(existing?.reason ?? "");
    setExpiresOn(existing?.expiresOn ?? "");
  };

  const handleUnassign = async (tagId: string) => {
    const result = await unassignTag(buildInput(tagId));
    if (!result.success) {
      notifications.show({ color: "red", message: result.message });
    }
  };

  const handleCheck = (tag: Tag, checked: boolean) => {
    if (checked) {
      openForm(tag.id, assignmentByTagId.get(tag.id));
      return;
    }
    if (editingTagId === tag.id) closeForm();
    if (assignmentByTagId.has(tag.id)) void handleUnassign(tag.id);
  };

  const handleApply = async (tagId: string) => {
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
          <Tooltip
            key={tag.id}
            withinPortal
            transitionProps={{ duration: 0 }}
            label={
              <Stack gap={0}>
                <Text size="xs">{tag.description}</Text>
                {assignment?.reason && <Text size="xs">{assignment.reason}</Text>}
                {assignment?.expiresOn && (
                  <Text size="xs">
                    {t("tagControl.plannedRemoval", { date: assignment.expiresOn })}
                  </Text>
                )}
              </Stack>
            }
          >
            <Badge
              size="xs"
              variant={overdue ? "filled" : "light"}
              color={pillColor(tag)}
              data-testid={`tag-pill-${tag.id}`}
            >
              {tag.id}
            </Badge>
          </Tooltip>
        );
      })}
      {!readOnly && (
        <Popover withinPortal position="bottom-start" shadow="md">
          <Popover.Target>
            <ActionIcon
              variant="subtle"
              size="xs"
              aria-label={t("tagControl.editTags")}
              title={t("tagControl.editTags")}
              data-testid="tag-picker-toggle"
            >
              <TagIcon size={12} />
            </ActionIcon>
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
                return (
                  <Stack key={tag.id} gap="0.25rem">
                    <Group gap="0.25rem" wrap="nowrap">
                      <Checkbox
                        size="xs"
                        label={tag.id}
                        description={tag.description}
                        checked={!!assignment || isEditing}
                        onChange={(e) => handleCheck(tag, e.currentTarget.checked)}
                        data-testid={`tag-option-${tag.id}`}
                      />
                      {assignment && (
                        <ActionIcon
                          variant="subtle"
                          size="xs"
                          aria-label={t("tagControl.editAssignment")}
                          title={t("tagControl.editAssignment")}
                          onClick={() => openForm(tag.id, assignment)}
                          data-testid={`tag-edit-${tag.id}`}
                        >
                          <Pencil size={12} />
                        </ActionIcon>
                      )}
                    </Group>
                    {isEditing && (
                      <Stack gap="0.25rem" pl="1.5rem">
                        {tag.reasonPolicy !== "hidden" && (
                          <TextInput
                            size="xs"
                            label={t("tagControl.reasonLabel")}
                            required={reasonRequired}
                            placeholder={
                              reasonRequired ? t("tagControl.reasonRequired") : undefined
                            }
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
                              (expiresRequired && expiresOn.trim() === "")
                            }
                            onClick={() => void handleApply(tag.id)}
                            data-testid={`tag-apply-${tag.id}`}
                          >
                            {t("tagControl.apply")}
                          </Button>
                          <Button
                            size="compact-xs"
                            variant="default"
                            onClick={closeForm}
                            data-testid={`tag-cancel-${tag.id}`}
                          >
                            {t("tagControl.cancel")}
                          </Button>
                        </Group>
                      </Stack>
                    )}
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
