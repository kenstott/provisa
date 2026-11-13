// Copyright (c) 2026 Kenneth Stott
// Canary: 2b8e4f71-9c3a-4d06-b5e2-7a1f0c6d93e8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The one form of a tag's assignment to an object: its value (a parameterized tag), its reason and
// planned-removal date as the tag's policies ask (REQ-1375), and the library places it cites
// (REQ-2208), saved together by one Save. It opens when a tag is picked for an object, and filled
// from the assignment when the tag's chip is opened; read, with no inputs, by a caller who may not
// change the assignment. The tag's own defining citations head it.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Badge, Group, Select, Stack, Text, TextInput } from "@mantine/core";
import type { LibraryCitation, MutationResult, Tag, TagAssignment } from "../types/admin";
import { tagParam } from "../types/admin";
import { citationKey, type CitationPlace } from "../hooks/useTagQueries";
import { FormField, FormFooter, FormLayout, FormRead } from "./form/FormLayout";
import { CitationLink, CitationState, TagCitations } from "./TagCitations";

/** What one Save of the form sends: the assignment's fields and the whole set it cites. */
export interface TagAssignmentDraft {
  paramValue: string | null;
  reason: string;
  expiresOn: string;
  citations: CitationPlace[];
}

interface TagAssignmentFormProps {
  tag: Tag;
  /** The assignment the form edits; none for a tag being put on the object. */
  assignment?: TagAssignment;
  /** Read, with no inputs: the caller may not change the assignment. */
  readOnly?: boolean;
  onApply: (draft: TagAssignmentDraft) => Promise<MutationResult>;
  /** Cites a held citation's place again, recording its text as it reads now. */
  onConfirm: (c: LibraryCitation) => Promise<MutationResult>;
  onCancel: () => void;
}

/** A citation picked in the form and not yet saved: its document's title and where it points. */
function picked(place: CitationPlace, documentTitle: string): LibraryCitation {
  return {
    ...place,
    documentTitle,
    documentSource: "",
    textSha256: "",
    citedBy: null,
    citedAt: "",
    state: "current",
    needsReview: false,
  };
}

const done: MutationResult = { success: true, message: "" };

export function TagAssignmentForm({
  tag,
  assignment,
  readOnly,
  onApply,
  onConfirm,
  onCancel,
}: TagAssignmentFormProps) {
  const { t } = useTranslation();
  const [paramValue, setParamValue] = useState<string | null>(
    assignment ? tagParam(assignment.tagId) : null,
  );
  const [reason, setReason] = useState(assignment?.reason ?? "");
  const [expiresOn, setExpiresOn] = useState(assignment?.expiresOn ?? "");
  const [cited, setCited] = useState<LibraryCitation[]>(assignment?.citations ?? []);

  const reasonRequired = tag.reasonPolicy === "required";
  const expiresRequired = tag.expiresPolicy === "required";
  const paramRequired = tag.paramPolicy === "required";
  const incomplete =
    (reasonRequired && reason.trim() === "") ||
    (expiresRequired && expiresOn.trim() === "") ||
    (paramRequired && !paramValue);

  const add = async (place: CitationPlace, documentTitle: string) => {
    const key = citationKey(place);
    setCited((held) =>
      held.some((c) => citationKey(c) === key) ? held : [...held, picked(place, documentTitle)],
    );
    return done;
  };
  const remove = async (c: LibraryCitation) => {
    setCited((held) => held.filter((h) => citationKey(h) !== citationKey(c)));
    return done;
  };
  // Confirm is the one act that records a citation's text anew: it is cited at once, and the form
  // shows it current, as the next read will.
  const confirm = async (c: LibraryCitation) => {
    const result = await onConfirm(c);
    if (result.success) {
      setCited((held) =>
        held.map((h) =>
          citationKey(h) === citationKey(c) ? { ...h, state: "current", needsReview: false } : h,
        ),
      );
    }
    return result;
  };

  const apply = () =>
    onApply({
      paramValue,
      reason: reason.trim(),
      expiresOn: expiresOn.trim(),
      citations: cited.map(({ documentId, sectionHeading, sectionPage }) => ({
        documentId,
        sectionHeading,
        sectionPage,
      })),
    });

  const fields = (
    <>
      {paramRequired && (
        <FormField size="medium">
          <Select
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
        </FormField>
      )}
      {tag.reasonPolicy !== "hidden" && (
        <FormField size="long">
          <TextInput
            label={t("tagControl.reasonLabel")}
            required={reasonRequired}
            placeholder={reasonRequired ? t("tagControl.reasonRequired") : undefined}
            value={reason}
            onChange={(e) => setReason(e.currentTarget.value)}
            data-testid={`tag-reason-${tag.id}`}
          />
        </FormField>
      )}
      {tag.expiresPolicy !== "hidden" && (
        <FormField size="short">
          <TextInput
            type="date"
            label={t("tagControl.expiresLabel")}
            required={expiresRequired}
            value={expiresOn}
            onChange={(e) => setExpiresOn(e.currentTarget.value)}
            data-testid={`tag-expires-${tag.id}`}
          />
        </FormField>
      )}
      {/* REQ-2208: a tag that requires a citation marks the field; saving without one is a gap,
          shown on the chip, never a refusal. */}
      <FormField size="full" hint={tag.requiresCitation ? t("tagCitations.gapHelp") : undefined}>
        <TagCitations
          label={
            <Group gap="0.35rem" component="span" display="inline-flex">
              {t("tagCitations.citationsLabel")}
              {tag.requiresCitation && (
                <Badge
                  size="xs"
                  variant="light"
                  color="yellow"
                  component="span"
                  data-testid={`tag-citations-needed-${tag.id}`}
                >
                  {t("tagCitations.neededForCoverage")}
                </Badge>
              )}
            </Group>
          }
          testId={`tag-citations-${tag.id}`}
          citations={cited}
          readOnly={readOnly}
          onAdd={add}
          onRemove={remove}
          onConfirm={confirm}
        />
      </FormField>
    </>
  );

  return (
    <Stack gap="xs" data-testid={`tag-form-${tag.id}`}>
      {tag.citations.length > 0 && (
        <Stack gap="0.15rem" data-testid={`tag-defined-by-${tag.id}`}>
          <Text size="xs" fw={600}>
            {t("tagCitations.definedByLabel")}
          </Text>
          {tag.citations.map((c) => (
            <Group key={citationKey(c)} gap="0.25rem">
              <CitationLink c={c} testId={`tag-definition-link-${tag.id}`} />
              <CitationState c={c} />
            </Group>
          ))}
        </Stack>
      )}
      {readOnly ? (
        <FormRead testId={`tag-read-${tag.id}`}>{fields}</FormRead>
      ) : (
        <FormLayout>
          {fields}
          <FormFooter
            onSave={apply}
            onCancel={onCancel}
            saveProps={{ disabled: incomplete, "data-testid": `tag-apply-${tag.id}` }}
            cancelProps={{ "data-testid": `tag-cancel-${tag.id}` }}
          />
        </FormLayout>
      )}
    </Stack>
  );
}
