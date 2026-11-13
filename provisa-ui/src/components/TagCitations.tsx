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
// tag does: each listed with where it points, a mark when the section's text changed or is gone
// (with Confirm, which cites the same place again), and a picker that adds one -- a library
// document, then one of its sections or the whole of it.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import { ActionIcon, Badge, Button, Group, Select, Stack, Text } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Plus, X } from "lucide-react";
import { fetchLibrary, fetchLibraryDocument, type LibraryDocument } from "../api/referenceLibrary";
import type { CitationPlace } from "../hooks/useTagQueries";
import type { LibraryCitation, MutationResult } from "../types/admin";
import { detailHref } from "./list/useDetailParam";
import { Tip } from "./Tip";

const WHOLE = "";

/** Where a citation points within its document: its heading, its page, or the whole of it. */
export function CitationSection({ c }: { c: CitationPlace }) {
  const { t } = useTranslation();
  if (c.sectionHeading !== null) return <>{c.sectionHeading}</>;
  if (c.sectionPage !== null) return <>{t("tagCitations.page", { page: c.sectionPage })}</>;
  return <>{t("tagCitations.wholeDocument")}</>;
}

/** A citation as a link to its document in the Library, or plain text once the document is gone. */
export function CitationLink({ c, testId }: { c: LibraryCitation; testId?: string }) {
  const label = (
    <>
      {c.documentTitle} · <CitationSection c={c} />
    </>
  );
  return c.documentId === null ? (
    <Text size="xs" component="span" data-testid={testId}>
      {label}
    </Text>
  ) : (
    <Text size="xs" component={Link} to={detailHref("library", c.documentId)} data-testid={testId}>
      {label}
    </Text>
  );
}

/** The review mark of a citation whose section changed or is gone. */
export function CitationState({ c }: { c: LibraryCitation }) {
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
  onAdd: (place: CitationPlace) => Promise<MutationResult>;
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
    const result = await onAdd(place);
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

interface TagCitationsProps {
  citations: LibraryCitation[];
  onAdd: (place: CitationPlace) => Promise<MutationResult>;
  onRemove: (c: LibraryCitation) => Promise<MutationResult>;
  /** Cites the same place again, which records its text as it reads now. */
  onConfirm: (c: LibraryCitation) => Promise<MutationResult>;
  testId: string;
  readOnly?: boolean;
}

export function TagCitations({
  citations,
  onAdd,
  onRemove,
  onConfirm,
  testId,
  readOnly,
}: TagCitationsProps) {
  const { t } = useTranslation();
  const run = async (action: Promise<MutationResult>) => {
    const result = await action;
    if (!result.success) notifications.show({ color: "red", message: result.message });
  };
  return (
    <Stack gap="0.25rem" data-testid={testId}>
      {citations.length === 0 && (
        <Text size="xs" c="dimmed">
          {t("tagCitations.none")}
        </Text>
      )}
      {citations.map((c) => {
        const key = `${c.documentId}-${sectionValue(c.sectionHeading, c.sectionPage)}`;
        return (
          <Group key={key} gap="0.25rem" wrap="nowrap" data-testid={`${testId}-row-${key}`}>
            <CitationLink c={c} />
            <CitationState c={c} />
            {!readOnly && c.needsReview && (
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
              <Tip text={t("tagCitations.remove")} named>
                <ActionIcon
                  variant="subtle"
                  size="xs"
                  color="gray"
                  aria-label={t("tagCitations.remove")}
                  onClick={() => void run(onRemove(c))}
                  data-testid={`${testId}-remove-${key}`}
                >
                  <X size={12} />
                </ActionIcon>
              </Tip>
            )}
          </Group>
        );
      })}
      {!readOnly && <AddCitation onAdd={onAdd} testId={`${testId}-picker`} />}
    </Stack>
  );
}
