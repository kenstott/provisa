// Copyright (c) 2026 Kenneth Stott
// Canary: 2f7c9e14-8a3b-4d61-b5e0-c93a1d47f8b2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2208, REQ-2179: the library documents a term cites, on the term's panel -- the passages a scan
// found it in, the sections picked for it here, and what a scan's draft awaiting review proposes
// merging into it -- each a link to its section, with the library citation list every citer uses
// (TagCitations). A curator who may change the term picks a document and a section, confirms a
// picked one whose text changed, takes one off, and accepts or rejects a proposed one: the review
// queue's own merge and reject (TermReviewQueue), so a proposal is decided the same way wherever.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Text } from "@mantine/core";
import {
  citeForTerm,
  fetchTermCitations,
  unciteForTerm,
  type TermDocumentCitation,
} from "../../api/referenceLibrary";
import { mergeDraft, rejectDraft } from "../../api/termFinding";
import type { CitationPlace } from "../../hooks/useTagQueries";
import type { MutationResult } from "../../types/admin";
import { TagCitations, type CitationRow } from "../TagCitations";

/** A term's citation as the citation list shows it, with the server's row beside it. */
type Row = CitationRow & { key: string; held: TermDocumentCitation };

function row(c: TermDocumentCitation): Row {
  return {
    key: c.key,
    documentId: c.document_id,
    documentTitle: c.document_title,
    sectionHeading: c.section_heading,
    sectionPage: c.section_page,
    state: c.state,
    needsReview: c.needs_review,
    held: c,
  };
}

function failed(e: unknown): MutationResult {
  return { success: false, message: e instanceof Error ? e.message : String(e) };
}

export function TermDocuments({ termId, canEdit }: { termId: number; canEdit: boolean }) {
  const { t } = useTranslation();
  const [rows, setRows] = useState<Row[] | null>(null);
  const [problem, setProblem] = useState("");

  const read = useCallback(
    (live: () => boolean) =>
      fetchTermCitations(termId)
        .then((found) => {
          if (!live()) return;
          setRows(found.map(row));
          setProblem("");
        })
        .catch((e) => live() && setProblem(failed(e).message)),
    [termId],
  );

  useEffect(() => {
    let live = true;
    void read(() => live);
    return () => {
      live = false;
    };
  }, [read]);
  const load = () => read(() => true);

  /** Runs a change, then reads the list again; a refusal is the server's message. */
  const change = async (action: () => Promise<unknown>): Promise<MutationResult> => {
    try {
      await action();
    } catch (e) {
      return failed(e);
    }
    await load();
    return { success: true, message: "" };
  };

  const place = (c: CitationPlace) => ({
    document_id: c.documentId as number,
    section_heading: c.sectionHeading,
    section_page: c.sectionPage,
  });

  if (problem) {
    return (
      <Text size="xs" c="red" data-testid="glossary-term-documents-error">
        {problem}
      </Text>
    );
  }
  if (rows === null) {
    return (
      <Text size="xs" c="dimmed">
        {t("glossaryTab.documentsLoading")}
      </Text>
    );
  }
  return (
    <TagCitations<Row>
      citations={rows}
      testId="glossary-term-documents"
      readOnly={!canEdit}
      keyOf={(c) => c.key}
      isProposed={(c) => c.held.status === "proposed"}
      detail={(c) => c.held.quote}
      // A scan's quote that is gone has no section to cite again: it is taken off, or kept.
      confirmable={(c) => c.held.origin === "picked" && c.documentId !== null}
      onAdd={(picked) => change(() => citeForTerm(termId, place(picked)))}
      onConfirm={(c) => change(() => citeForTerm(termId, place(c)))}
      onAccept={(c) => change(() => mergeDraft(c.held.draft_id as number, { term_id: termId }))}
      onRemove={(c) =>
        change(() =>
          c.held.status === "proposed"
            ? rejectDraft(c.held.draft_id as number)
            : unciteForTerm(termId, c.held),
        )
      }
    />
  );
}
