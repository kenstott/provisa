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
import type { LibraryDocument } from "../../api/referenceLibrary";
import { WHOLE_ORG } from "../../components/admin/referenceScope";
import { useDomains } from "../../hooks/useAdminQueries";
import { assignableDomainIds } from "../../lib/internalDomains";

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
