// Copyright (c) 2026 Kenneth Stott
// Canary: 4f2a9c71-6d3e-4b85-a1f0-8e7c5d2b9a34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The reference library documents and sections a tag's definition cites, or one assignment of the
// tag does, or a glossary term does (TermDocuments): each listed with where it points, a mark when
// the section's text changed or is gone (with Confirm, which cites the same place again), and a
// picker that adds one -- a library document, then one of its sections or the whole of it. A
// term's list also holds what review proposes, marked so, with Accept. Given a label, it is a field
// of a form (the assignment form), and reads as its list in that form's read view.

import { useEffect, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Badge, Button, Group, Input, Select, Stack, Text } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Check, Plus, X } from "lucide-react";
import { fetchLibrary, fetchLibraryDocument, type LibraryDocument } from "../api/referenceLibrary";
import { citationKey, type CitationPlace } from "../hooks/useTagQueries";
import type { LibraryCitation, MutationResult } from "../types/admin";
import { sectionMark } from "../lib/entityHref";
import { EntityLink } from "./EntityLink";
import { Tip } from "./Tip";

const WHOLE = "";

/** What a listed citation shows: the place it points to, its document's title and its state. */
export type CitationRow = CitationPlace &
  Pick<LibraryCitation, "documentTitle" | "state" | "needsReview">;

/** Where a citation points within its document: its heading, its page, or the whole of it. */
export function CitationSection({ c }: { c: CitationPlace }) {
  const { t } = useTranslation();
  if (c.sectionHeading !== null) return <>{c.sectionHeading}</>;
  if (c.sectionPage !== null) return <>{t("tagCitations.page", { page: c.sectionPage })}</>;
  return <>{t("tagCitations.wholeDocument")}</>;
}

/** A citation as a link to the section it cites, on its document's panel in the Library (an
 * EntityLink: plain text to a viewer who may not open the Library), or plain text once the
 * document is gone. */
export function CitationLink({ c, testId }: { c: CitationRow; testId?: string }) {
  const label = (
    <>
      {c.documentTitle} · <CitationSection c={c} />
    </>
  );
  return (
    <Text size="xs" component="span">
      {c.documentId === null ? (
        <span data-testid={testId}>{label}</span>
      ) : (
        <EntityLink
          kind="section"
          id={c.documentId}
          section={sectionMark(c.sectionHeading, c.sectionPage)}
          testId={testId}
        >
          {label}
        </EntityLink>
      )}
    </Text>
  );
}

/** The review mark of a citation whose section changed or is gone. */
export function CitationState({ c }: { c: Pick<LibraryCitation, "state" | "needsReview"> }) {
  const { t } = useTranslation();
  if (!c.needsReview) return null;
  const drifted = c.state === "drifted";
  return (
    <Tip text={t(drifted ? "tagCitations.driftedHelp" : "tagCitations.brokenHelp")}>
      <Badge
        size="xs"
        variant="light"
        color={drifted ? "orange" : "red"}
        data-testid={`citation-state-${c.state}`}
      >
        {t(drifted ? "tagCitations.drifted" : "tagCitations.broken")}
      </Badge>
    </Tip>
  );
}

const sectionValue = (heading: string | null, page: number | null) =>
  heading !== null ? `h:${heading}` : page !== null ? `p:${page}` : WHOLE;

function AddCitation({
  onAdd,
  testId,
}: {
  onAdd: (place: CitationPlace, documentTitle: string) => Promise<MutationResult>;
  testId: string;
}) {
  const { t } = useTranslation();
  const [documents, setDocuments] = useState<LibraryDocument[]>([]);
  const [documentId, setDocumentId] = useState<string | null>(null);
  const [sections, setSections] = useState<{ value: string; label: string }[]>([]);
  const [section, setSection] = useState<string | null>(WHOLE);
  const [problem, setProblem] = useState("");

  useEffect(() => {
    let live = true;
    fetchLibrary()
      .then((d) => live && setDocuments(d.filter((x) => x.index_status === "ready")))
      .catch((e) => live && setProblem(e instanceof Error ? e.message : String(e)));
    return () => {
      live = false;
    };
  }, []);

  const pickDocument = (id: string | null) => {
    setDocumentId(id);
    setSection(WHOLE);
    setSections([]);
    if (id === null) return;
    fetchLibraryDocument(Number(id))
      .then((d) => {
        const seen = new Map<string, string>();
        for (const p of d.passage_list) {
          const value = sectionValue(p.section || null, p.section ? null : p.page);
          if (value === WHOLE || seen.has(value)) continue;
          seen.set(value, p.section || t("tagCitations.page", { page: p.page }));
        }
        setSections([...seen].map(([value, label]) => ({ value, label })));
      })
      .catch((e) => setProblem(e instanceof Error ? e.message : String(e)));
  };

  const add = async () => {
    if (documentId === null || section === null) return;
    const place: CitationPlace = {
      documentId: Number(documentId),
      sectionHeading: section.startsWith("h:") ? section.slice(2) : null,
      sectionPage: section.startsWith("p:") ? Number(section.slice(2)) : null,
    };
    const title = documents.find((d) => d.id === place.documentId)?.title ?? "";
    const result = await onAdd(place, title);
    if (!result.success) {
      notifications.show({ color: "red", message: result.message });
      return;
    }
    setDocumentId(null);
    setSection(WHOLE);
    setSections([]);
  };

  return (
    <Stack gap="0.25rem" data-testid={testId}>
      <Select
        size="xs"
        searchable
        label={t("tagCitations.document")}
        placeholder={t("tagCitations.documentPlaceholder")}
        comboboxProps={{ withinPortal: true, transitionProps: { duration: 0 } }}
        data={documents.map((d) => ({ value: String(d.id), label: d.title }))}
        value={documentId}
        onChange={pickDocument}
        data-testid={`${testId}-document`}
      />
      {documentId !== null && (
        <Select
          size="xs"
          searchable
          allowDeselect={false}
          label={t("tagCitations.section")}
          comboboxProps={{ withinPortal: true, transitionProps: { duration: 0 } }}
          data={[{ value: WHOLE, label: t("tagCitations.wholeDocument") }, ...sections]}
          value={section}
          onChange={setSection}
          data-testid={`${testId}-section`}
        />
      )}
      {problem && (
        <Text size="xs" c="red">
          {problem}
        </Text>
      )}
      <Group>
        <Button
          size="compact-xs"
          leftSection={<Plus size={12} />}
          disabled={documentId === null}
          onClick={() => void add()}
          data-testid={`${testId}-add`}
        >
          {t("tagCitations.add")}
        </Button>
      </Group>
    </Stack>
  );
}

interface TagCitationsProps<C extends CitationRow = LibraryCitation> {
  citations: C[];
  /** Picked: the place, and its document's title as the picker lists it. */
  onAdd: (place: CitationPlace, documentTitle: string) => Promise<MutationResult>;
  onRemove: (c: C) => Promise<MutationResult>;
  /** Cites the same place again, which records its text as it reads now. */
  onConfirm: (c: C) => Promise<MutationResult>;
  testId: string;
  readOnly?: boolean;
  /** As a field of a form: its label, and the hint the form's field gives it. */
  label?: ReactNode;
  description?: ReactNode;
  /** A row's name, unique in the list; the place it points to unless given (a term's list holds
   * several citations of one place). */
  keyOf?: (c: C) => string;
  /** A citation awaiting review: marked proposed, accepted with ``onAccept`` (a term's draft). */
  isProposed?: (c: C) => boolean;
  onAccept?: (c: C) => Promise<MutationResult>;
  /** What a row shows beneath its link (a scan's quote). */
  detail?: (c: C) => ReactNode;
  /** Whether a flagged row is confirmed by citing its place again; every one unless given (a
   * scan's quote that is gone has no place to cite again). */
  confirmable?: (c: C) => boolean;
}

export function TagCitations<C extends CitationRow = LibraryCitation>({
  citations,
  onAdd,
  onRemove,
  onConfirm,
  testId,
  readOnly,
  label,
  description,
  keyOf = citationKey,
  isProposed,
  onAccept,
  detail,
  confirmable,
}: TagCitationsProps<C>) {
  const { t } = useTranslation();
  const run = async (action: Promise<MutationResult>) => {
    const result = await action;
    if (!result.success) notifications.show({ color: "red", message: result.message });
  };
  const list = (
    <Stack gap="0.25rem" data-testid={testId}>
      {citations.length === 0 && (
        <Text size="xs" c="dimmed">
          {t("tagCitations.none")}
        </Text>
      )}
      {citations.map((c) => {
        const key = keyOf(c);
        const proposed = isProposed?.(c) ?? false;
        const quoted = detail?.(c);
        return (
          <Stack key={key} gap={0} data-testid={`${testId}-row-${key}`}>
            <Group gap="0.25rem">
              <CitationLink c={c} />
              {proposed && (
                <Tip text={t("tagCitations.proposedHelp")}>
                  <Badge
                    size="xs"
                    variant="light"
                    color="blue"
                    data-testid={`${testId}-proposed-${key}`}
                  >
                    {t("tagCitations.proposed")}
                  </Badge>
                </Tip>
              )}
              <CitationState c={c} />
              {!readOnly && proposed && onAccept && (
                <Button
                  size="compact-xs"
                  variant="light"
                  leftSection={<Check size={12} />}
                  onClick={() => void run(onAccept(c))}
                  data-testid={`${testId}-accept-${key}`}
                >
                  {t("tagCitations.accept")}
                </Button>
              )}
              {!readOnly && !proposed && c.needsReview && (confirmable?.(c) ?? true) && (
                <Button
                  size="compact-xs"
                  variant="light"
                  onClick={() => void run(onConfirm(c))}
                  data-testid={`${testId}-confirm-${key}`}
                >
                  {t("tagCitations.confirm")}
                </Button>
              )}
              {!readOnly && (
                <Tip text={t(proposed ? "tagCitations.reject" : "tagCitations.remove")} named>
                  <ActionIcon
                    variant="subtle"
                    size="xs"
                    color="gray"
                    aria-label={t(proposed ? "tagCitations.reject" : "tagCitations.remove")}
                    onClick={() => void run(onRemove(c))}
                    data-testid={`${testId}-remove-${key}`}
                  >
                    <X size={12} />
                  </ActionIcon>
                </Tip>
              )}
            </Group>
            {quoted && (
              <Text size="xs" c="dimmed" lineClamp={2} data-testid={`${testId}-detail-${key}`}>
                {quoted}
              </Text>
            )}
          </Stack>
        );
      })}
      {!readOnly && <AddCitation onAdd={onAdd} testId={`${testId}-picker`} />}
    </Stack>
  );
  if (label === undefined) return list;
  return (
    <Input.Wrapper label={label} description={description}>
      {list}
    </Input.Wrapper>
  );
}

// In a form's read view the field reads as its list, with nothing to change.
TagCitations.readValue = (props: Record<string, unknown>) => (
  <TagCitations {...(props as unknown as TagCitationsProps)} label={undefined} readOnly />
);
