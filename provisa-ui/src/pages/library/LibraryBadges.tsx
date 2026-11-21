// Copyright (c) 2026 Kenneth Stott
// Canary: 8ab3e6d1-27c4-49f0-b1a6-3e5d90c7f218
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2185: the badges the Library page's list and panel both show.

import { Badge, Group } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { LibraryDocument } from "../../api/referenceLibrary";
import { WHOLE_ORG } from "../../components/admin/referenceScope";
import { Tip } from "../../components/Tip";

export function ReaderBadges({ domains }: { domains: string[] }) {
  const { t } = useTranslation();
  return (
    <Group gap={4} wrap="wrap">
      {domains.map((d) => (
        <Badge key={d} size="xs" variant="light" color={d === WHOLE_ORG ? "grape" : "blue"}>
          {d === WHOLE_ORG ? t("referenceLibrary.wholeOrg") : d}
        </Badge>
      ))}
    </Group>
  );
}

export function StatusBadge({ status }: { status: LibraryDocument["status"] }) {
  const { t } = useTranslation();
  return (
    <Badge size="xs" variant="light" color={status === "proposed" ? "yellow" : "green"}>
      {status === "proposed"
        ? t("referenceLibrary.statusProposed")
        : t("referenceLibrary.statusKept")}
    </Badge>
  );
}

export function StaleBadge({ doc }: { doc: LibraryDocument }) {
  const { t } = useTranslation();
  if (!doc.needs_reembedding) return null;
  return (
    <Tip text={t("referenceLibrary.needsReembeddingHelp", { model: doc.embedding_model })}>
      <Badge size="xs" variant="light" color="orange" data-testid={`reflib-stale-${doc.id}`}>
        {t("referenceLibrary.needsReembedding")}
      </Badge>
    </Tip>
  );
}

/** A document's index status: indexing with its progress where known, ready, or failed. */
export function IndexBadge({ doc, testId }: { doc: LibraryDocument; testId?: string }) {
  const { t } = useTranslation();
  if (doc.index_status === "ready") {
    return (
      <Badge size="xs" variant="light" color="green" data-testid={testId}>
        {t("referenceLibrary.indexReady")}
      </Badge>
    );
  }
  if (doc.index_status === "failed") {
    return (
      <Tip text={doc.index_error ?? ""}>
        <Badge size="xs" variant="light" color="red" data-testid={testId}>
          {t("referenceLibrary.indexFailed")}
        </Badge>
      </Tip>
    );
  }
  const known = doc.index_done !== null && doc.index_total !== null;
  return (
    <Badge size="xs" variant="light" color="blue" data-testid={testId}>
      {known
        ? t("referenceLibrary.indexProgress", { done: doc.index_done, total: doc.index_total })
        : t("referenceLibrary.indexIndexing")}
    </Badge>
  );
}
