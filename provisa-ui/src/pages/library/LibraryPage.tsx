// Copyright (c) 2026 Kenneth Stott
// Canary: 6d2a8f41-9e7b-4c35-a1d0-8b5e3f7c2a96
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2185: the Library, a top-level page shaped like every other: the documents the caller may
// read in a list (title, source, index status, readers, added or refreshed) under the shared
// filter, with the selected one in the panel on the right. The library-editing right adds,
// refreshes and removes on the same page, and sets how the library is searched. A document added
// here exists at once and is indexed in the background, so the list reads itself again while any
// document is indexing.

import React, { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Checkbox, Group, Table, Text } from "@mantine/core";
import { useSearchParams } from "react-router-dom";
import { Check, Library, TriangleAlert } from "lucide-react";
import {
  fetchLibrary,
  fetchReviewsDue,
  reembedLibrary,
  type LibraryDocument,
  type ReembedOutcome,
} from "../../api/referenceLibrary";
import { FilterInput } from "../../components/admin/FilterInput";
import {
  ListEmpty,
  ListHead,
  ListItems,
  ListRow,
  ListTable,
} from "../../components/list/ListTable";
import { SplitPane } from "../../components/list/SplitPane";
import { useRememberedDetail } from "../../components/list/useDetailParam";
import {
  detailListOf,
  filterNarrowing,
  searchNarrowing,
  shownBy,
} from "../../components/list/leftOut";
import { useUrlFilter } from "../../components/list/useUrlFilter";
import { useListSortGroup, type ListColumn } from "../../components/list/useListSortGroup";
import { PageLoading } from "../../components/PageLoading";
import { PageTitle } from "../../components/PageTitle";
import { useCapability } from "../../hooks/useCapability";
import { documentMark } from "../../lib/documentMark";
import { IndexBadge, ReaderBadges, StaleBadge, StatusBadge } from "./LibraryBadges";
import { LibraryAddForm } from "./LibraryAddForm";
import { LibraryDetail } from "./LibraryDetail";
import { addedOrRefreshed, libraryRowTexts, REVIEWS_DUE_PARAM, when } from "./libraryParts";
import { PageActions } from "../../components/PageActions";
import { PageCommand } from "../../components/PageCommand";

/** How often the list is read again while a document is indexing, in milliseconds. */
const POLL_MS = 3000;

export function LibraryPage() {
  const { t, i18n } = useTranslation();
  const canEdit = useCapability("glossary_rw");
  const [documents, setDocuments] = useState<LibraryDocument[] | null>(null);
  const [error, setError] = useState("");
  // REQ-2277: the list's filter, in the URL.
  const [search, setSearch] = useUrlFilter();
  const [searchParams, setSearchParams] = useSearchParams();
  const reviewsDueOnly = searchParams.get(REVIEWS_DUE_PARAM) === "1";
  // The documents with reviews due, read while the list is narrowed to them.
  const [due, setDue] = useState<ReadonlySet<number> | null>(null);
  const setReviewsDueOnly = (on: boolean) =>
    setSearchParams(
      (p) => {
        const n = new URLSearchParams(p);
        if (on) n.set(REVIEWS_DUE_PARAM, "1");
        else n.delete(REVIEWS_DUE_PARAM);
        return n;
      },
      { replace: true },
    );
  // REQ-2241: `?document=<id>` is the open document's panel.
  // REQ-2260: and the one left open is open again on coming back.
  // REQ-2277 rule 9: a document is open only while the list -- the reviews-due narrowing, and the
  // search (REQ-2143: the web-search syntax, over every text a row shows) -- shows its row. While
  // the documents with reviews due are read, the narrowed list is still loading.
  const listNarrowings = [
    filterNarrowing<LibraryDocument>(
      t,
      reviewsDueOnly,
      t("referenceLibrary.reviewsDueOnly"),
      (d) => due !== null && due.has(d.id),
      () => setReviewsDueOnly(false),
    ),
    searchNarrowing<LibraryDocument>(
      t,
      search,
      () => setSearch(""),
      (d) => libraryRowTexts(d, t, i18n.language),
    ),
  ];
  const listLoading = documents === null || (reviewsDueOnly && due === null);
  const detail = useRememberedDetail(
    "library",
    detailListOf(
      t,
      listLoading ? null : documents,
      (d) => String(d.id),
      (d) => d.title,
      listNarrowings,
    ),
  );
  const selected = detail.id === null ? null : Number(detail.id);
  const setSelected = (id: number | null) =>
    id === null ? detail.close() : detail.open(String(id));
  const [adding, setAdding] = useState(false);
  const [reembedding, setReembedding] = useState(false);
  const [outcome, setOutcome] = useState<ReembedOutcome | null>(null);

  useEffect(() => {
    fetchLibrary()
      .then(setDocuments)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, []);

  useEffect(() => {
    if (!reviewsDueOnly) return;
    let live = true;
    fetchReviewsDue().then(
      (r) => live && setDue(new Set(r.documents.map((d) => d.id))),
      (e: unknown) => live && setError(e instanceof Error ? e.message : String(e)),
    );
    return () => {
      live = false;
    };
  }, [reviewsDueOnly]);

  const indexing = (documents ?? []).some((d) => d.index_status === "indexing");
  useEffect(() => {
    if (!indexing) return;
    const timer = window.setInterval(() => {
      fetchLibrary()
        .then(setDocuments)
        .catch((e) => setError(e instanceof Error ? e.message : String(e)));
    }, POLL_MS);
    return () => window.clearInterval(timer);
  }, [indexing]);

  const reembedAll = async () => {
    setReembedding(true);
    setError("");
    setOutcome(null);
    try {
      const result = await reembedLibrary();
      setOutcome(result);
      setDocuments(await fetchLibrary());
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setReembedding(false);
    }
  };
  const stale = (documents ?? []).filter((d) => d.needs_reembedding).length;

  const filtered = shownBy(documents ?? [], listNarrowings);

  const columns: ListColumn<LibraryDocument>[] = useMemo(
    () => [
      { key: "title", label: t("referenceLibrary.colTitle"), sortValue: (d) => d.title },
      { key: "source", label: t("referenceLibrary.colSource"), sortValue: (d) => d.source },
      {
        key: "index",
        label: t("referenceLibrary.colIndex"),
        sortValue: (d) => d.index_status,
        groupValue: (d) =>
          d.index_status === "ready"
            ? t("referenceLibrary.indexReady")
            : d.index_status === "failed"
              ? t("referenceLibrary.indexFailed")
              : t("referenceLibrary.indexIndexing"),
      },
      {
        key: "readers",
        label: t("referenceLibrary.colReaders"),
        sortValue: (d) => d.domains.join(","),
      },
      {
        key: "changed",
        label: t("referenceLibrary.colAddedOrRefreshed"),
        sortValue: (d) => addedOrRefreshed(d),
      },
    ],
    [t],
  );
  const sortGroup = useListSortGroup(filtered, columns, "library");

  if (listLoading && !error) {
    return <PageLoading message={t("referenceLibrary.loading")} />;
  }

  const doc = selected === null ? null : ((documents ?? []).find((d) => d.id === selected) ?? null);
  const replace = (d: LibraryDocument) =>
    setDocuments((prev) => (prev ?? []).map((x) => (x.id === d.id ? d : x)));

  const panel = adding ? (
    <LibraryAddForm
      onCancel={() => setAdding(false)}
      onAdded={(d) => {
        setDocuments((prev) => [d, ...(prev ?? [])]);
        setAdding(false);
        setSelected(d.id);
      }}
    />
  ) : doc ? (
    <LibraryDetail
      key={doc.id}
      doc={doc}
      canEdit={canEdit}
      tab={detail.tab ?? "overview"}
      onTab={detail.setTab}
      onChanged={replace}
      onRemoved={(id) => {
        setDocuments((prev) => (prev ?? []).filter((x) => x.id !== id));
        setSelected(null);
      }}
    />
  ) : null;

  return (
    <div className="page page-sticky-head" data-tour="reference-library">
      <div className="page-header">
        <PageTitle icon={Library}>{t("referenceLibrary.title")}</PageTitle>
        <FilterInput
          value={search}
          onChange={setSearch}
          placeholder={t("referenceLibrary.filterPlaceholder")}
          testId="reflib-filter"
          remember="library"
        />
        <PageActions>
          <PageCommand
            description={t("pageCommands.reviewsDue")}
            docs="ontology/#finding-terms-in-documents-and-tables"
          >
            <Checkbox
              label={t("referenceLibrary.reviewsDueOnly")}
              checked={reviewsDueOnly}
              onChange={(e) => setReviewsDueOnly(e.currentTarget.checked)}
              styles={{ label: { whiteSpace: "nowrap" } }}
              data-testid="reflib-reviews-due-only"
            />
          </PageCommand>
          {canEdit && stale > 0 && (
            <PageCommand
              description={t("pageCommands.reembedAll")}
              docs="ontology/#finding-terms-in-documents-and-tables"
            >
              <Button
                variant="light"
                color="orange"
                data-testid="reflib-reembed-all"
                loading={reembedding}
                onClick={reembedAll}
              >
                {t("referenceLibrary.reembedAll", { n: stale })}
              </Button>
            </PageCommand>
          )}
          {canEdit && !adding && (
            <PageCommand
              description={t("pageCommands.addDocument")}
              docs="ontology/#finding-terms-in-documents-and-tables"
            >
              <Button
                data-testid="reflib-add"
                onClick={() => {
                  setAdding(true);
                  setSelected(null);
                }}
              >
                {t("referenceLibrary.add")}
              </Button>
            </PageCommand>
          )}
        </PageActions>
      </div>
      <Text c="dimmed" size="sm" mb="sm" data-testid="reflib-why">
        {t("referenceLibrary.why")}
      </Text>
      {outcome && (
        <Alert
          color={outcome.failed.length ? "red" : "green"}
          icon={outcome.failed.length ? <TriangleAlert size={16} /> : <Check size={16} />}
          withCloseButton
          style={{ flexShrink: 0 }}
          mb="sm"
          onClose={() => setOutcome(null)}
          data-testid="reflib-reembed-result"
        >
          <Text size="sm">
            {t("referenceLibrary.reembedDone", { n: outcome.reembedded.length })}
          </Text>
          {outcome.failed.length > 0 && (
            <>
              <Text size="sm">{t("referenceLibrary.reembedFailedHeading")}</Text>
              <ul data-testid="reflib-reembed-failures">
                {outcome.failed.map((f) => (
                  <li key={f.document_id}>
                    {t("referenceLibrary.reembedFailure", { title: f.title, reason: f.reason })}
                  </li>
                ))}
              </ul>
            </>
          )}
        </Alert>
      )}
      {error && (
        <Alert color="red" icon={<TriangleAlert size={16} />} mb="sm" data-testid="reflib-error">
          {error}
        </Alert>
      )}
      <SplitPane
        outOfList={detail.outOfList}
        testId="reflib-split"
        storageKey="provisa.referenceLibrary.detailWidth"
        title={adding ? t("referenceLibrary.addTitle") : (doc?.title ?? "")}
        onClose={() => {
          setAdding(false);
          setSelected(null);
        }}
        closeOnEscape={!adding}
        labels={{
          panel: t("referenceLibrary.detailPanel"),
          resize: t("tablesPage.resizeDetail"),
          expand: t("tablesPage.expandDetail"),
          restore: t("tablesPage.restoreDetail"),
          close: t("tablesPage.closeDetail"),
          backToList: t("tablesPage.backToList"),
        }}
        list={
          <ListTable testId="reflib-table">
            <ListHead
              sortGroup={sortGroup}
              columns={[
                { col: "title" },
                { col: "source" },
                { col: "index" },
                { col: "readers" },
                { col: "changed" },
              ]}
            />
            <Table.Tbody>
              {filtered.length === 0 && (
                <ListEmpty colSpan={5} testId="reflib-empty">
                  {/* A list a filter emptied says so, never that the library is empty. */}
                  {(documents ?? []).length === 0
                    ? t("referenceLibrary.empty")
                    : t("referenceLibrary.noMatch")}
                </ListEmpty>
              )}
              <ListItems
                state={sortGroup}
                colSpan={5}
                rowKey={(d) => String(d.id)}
                render={(d) => (
                  <React.Fragment key={d.id}>
                    <ListRow
                      testId={`reflib-row-${d.id}`}
                      // REQ-2201: a demo's tour points at a document by the address it was read from.
                      data-reflib-source={documentMark(d.source)}
                      selected={selected === d.id && !adding}
                      onClick={() => {
                        setAdding(false);
                        setSelected(selected === d.id ? null : d.id);
                      }}
                    >
                      <Table.Td>
                        <Group gap={6} wrap="nowrap">
                          <Text size="sm" fw={600}>
                            {d.title}
                          </Text>
                          {d.status === "proposed" && <StatusBadge status={d.status} />}
                        </Group>
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs" ff="monospace" style={{ wordBreak: "break-all" }}>
                          {d.source}
                        </Text>
                      </Table.Td>
                      <Table.Td>
                        <Group gap={4} wrap="wrap">
                          <IndexBadge doc={d} testId={`reflib-index-${d.id}`} />
                          <StaleBadge doc={d} />
                        </Group>
                      </Table.Td>
                      <Table.Td>
                        <ReaderBadges domains={d.domains} />
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs">{when(addedOrRefreshed(d), i18n.language)}</Text>
                      </Table.Td>
                    </ListRow>
                  </React.Fragment>
                )}
              />
            </Table.Tbody>
          </ListTable>
        }
      >
        {panel}
      </SplitPane>
    </div>
  );
}
