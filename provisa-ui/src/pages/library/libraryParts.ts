// Copyright (c) 2026 Kenneth Stott
// Canary: 4c7e19a2-b05d-46f8-8e3a-91d2f6c0a7b4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2185: what the Library page's list, its panel and its add form share.

import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import type { TFunction } from "i18next";
import type { LibraryDocument } from "../../api/referenceLibrary";
import { WHOLE_ORG } from "../../components/admin/referenceScope";
import { useDomains } from "../../hooks/useAdminQueries";
import { assignableDomainIds } from "../../lib/internalDomains";

/** REQ-2259: `?reviewsDue=1` narrows the Library to the documents whose citations hold review items,
 * the ones Context Health's Documents count counts. */
export const REVIEWS_DUE_PARAM = "reviewsDue";

/** The files the library reads. */
export const ACCEPT = ".html,.htm,.md,.markdown,.txt,.pdf,.docx,.xlsx,.xml";

export function when(value: string | null, language: string): string {
  return value ? new Date(value).toLocaleString(language) : "";
}

/** The later of a document's refresh and replacement, or when it was added. */
export function addedOrRefreshed(d: LibraryDocument): string {
  const times = [d.refreshed_at, d.replaced_at].filter((v): v is string => v !== null);
  return times.length ? times.sort().at(-1)! : d.added_at;
}

/** The choices for who reads a document: the whole organization, or the org's own domains. */
export function useReaderOptions() {
  const { t } = useTranslation();
  const { domains } = useDomains();
  return useMemo(
    () => [
      { value: WHOLE_ORG, label: t("referenceLibrary.wholeOrg") },
      ...assignableDomainIds(domains.map((d) => d.id)).map((id) => ({ value: id, label: id })),
    ],
    [domains, t],
  );
}

/** What a document's index badge says. */
export function indexLabel(doc: LibraryDocument, t: TFunction): string {
  if (doc.index_status === "ready") return t("referenceLibrary.indexReady");
  if (doc.index_status === "failed") return t("referenceLibrary.indexFailed");
  return doc.index_done !== null && doc.index_total !== null
    ? t("referenceLibrary.indexProgress", { done: doc.index_done, total: doc.index_total })
    : t("referenceLibrary.indexIndexing");
}

/** A reader as its badge names it: the whole organization, or a domain. */
export function readerLabel(domain: string, t: TFunction): string {
  return domain === WHOLE_ORG ? t("referenceLibrary.wholeOrg") : domain;
}

/** Every text a document's row shows, which the list's filter searches. */
export function libraryRowTexts(d: LibraryDocument, t: TFunction, language: string): string[] {
  return [
    d.title,
    d.status === "proposed" ? t("referenceLibrary.statusProposed") : "",
    d.source,
    indexLabel(d, t),
    d.needs_reembedding ? t("referenceLibrary.needsReembedding") : "",
    ...d.domains.map((domain) => readerLabel(domain, t)),
    when(addedOrRefreshed(d), language),
  ];
}
