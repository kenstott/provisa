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
import { Alert, Button, Group, SegmentedControl, Table, Text } from "@mantine/core";
import { Check, Library, TriangleAlert } from "lucide-react";
import {
  fetchLibrary,
  fetchRetrieval,
  reembedLibrary,
  saveRetrieval,
  type LibraryDocument,
  type ReembedOutcome,
  type Retrieval,
} from "../../api/referenceLibrary";
import { FilterInput } from "../../components/admin/FilterInput";
import { LabelWithHelp } from "../../components/form/FormLayout";
import { ListEmpty, ListHead, ListItems, ListRow, ListTable } from "../../components/list/ListTable";
import { SplitPane } from "../../components/list/SplitPane";
import { useListSortGroup, type ListColumn } from "../../components/list/useListSortGroup";
import { PageHelp } from "../../components/PageHelp";
import { PageLoading } from "../../components/PageLoading";
import { PageTitle } from "../../components/PageTitle";
import { useCapability } from "../../hooks/useCapability";
import { documentMark } from "../../lib/documentMark";
import { IndexBadge, ReaderBadges, StaleBadge, StatusBadge } from "./LibraryBadges";
import { LibraryAddForm } from "./LibraryAddForm";
import { LibraryDetail } from "./LibraryDetail";
import { addedOrRefreshed, when } from "./libraryParts";

/** How often the list is read again while a document is indexing, in milliseconds. */
const POLL_MS = 3000;

/** The library's retrieval setting, saved as it is changed. */
function RetrievalSetting({ onError }: { onError: (message: string) => void }) {
  const { t } = useTranslation();
  const [retrieval, setRetrieval] = useState<Retrieval | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    fetchRetrieval()
      .then(setRetrieval)
      .catch((e) => onError(e instanceof Error ? e.message : String(e)));
  }, [onError]);

  const change = async (next: Retrieval) => {
    const before = retrieval;
    setRetrieval(next);
    setSaving(true);
    try {
      setRetrieval(await saveRetrieval(next));
    } catch (e) {
      setRetrieval(before);
      onError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Group gap="xs" wrap="nowrap" data-testid="reflib-retrieval">
      <Text size="sm" component="div">
        <LabelWithHelp
          label={t("referenceLibrary.retrieval")}
          help={t("referenceLibrary.retrievalHelp")}
          textId="reflib-retrieval-label"
        />
      </Text>
      <SegmentedControl
        size="xs"
        aria-labelledby="reflib-retrieval-label"
        value={retrieval ?? ""}
        disabled={retrieval === null || saving}
        onChange={(v) => change(v as Retrieval)}
        data={[
          { value: "passage", label: t("referenceLibrary.retrievalPassage") },
          { value: "graph", label: t("referenceLibrary.retrievalGraph") },
        ]}
        data-testid="reflib-retrieval-control"
      />
    </Group>
  );
}

export function LibraryPage() {
  const { t, i18n } = useTranslation();
  const canEdit = useCapability("glossary_rw");
  const [documents, setDocuments] = useState<LibraryDocument[] | null>(null);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<number | null>(null);
  const [adding, setAdding] = useState(false);
  const [reembedding, setReembedding] = useState(false);
  const [outcome, setOutcome] = useState<ReembedOutcome | null>(null);

  useEffect(() => {
    fetchLibrary()
      .then(setDocuments)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, []);

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

  const filtered = useMemo(() => {
    const needle = search.trim().toLowerCase();
    if (!needle) return documents ?? [];
    return (documents ?? []).filter((d) =>
      [d.title, d.source, ...d.domains].some((v) => v.toLowerCase().includes(needle)),
    );
  }, [documents, search]);

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

  if (documents === null && !error) return <PageLoading message={t("referenceLibrary.loading")} />;

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
      onChanged={replace}
      onRemoved={(id) => {
        setDocuments((prev) => (prev ?? []).filter((x) => x.id !== id));
        setSelected(null);
      }}
    />
  ) : null;

  return (
    <div
      className="page-sticky-head"
      style={{ padding: "1rem 1.25rem" }}
      data-tour="reference-library"
    >
      <Group justify="space-between" wrap="nowrap" mb={4}>
        <PageTitle icon={Library} order={3}>
          {t("referenceLibrary.title")}
        </PageTitle>
        <Group gap="xs" wrap="nowrap" data-toolbar>
          <FilterInput
            value={search}
            onChange={setSearch}
            placeholder={t("referenceLibrary.filterPlaceholder")}
            testId="reflib-filter"
            remember="library"
          />
          {canEdit && <RetrievalSetting onError={setError} />}
          {canEdit && stale > 0 && (
            <Button
              size="xs"
              variant="light"
              color="orange"
              data-testid="reflib-reembed-all"
              loading={reembedding}
              onClick={reembedAll}
            >
              {t("referenceLibrary.reembedAll", { n: stale })}
            </Button>
          )}
          {canEdit && !adding && (
            <Button
              size="xs"
              data-testid="reflib-add"
              onClick={() => {
                setAdding(true);
                setSelected(null);
              }}
            >
              {t("referenceLibrary.add")}
            </Button>
          )}
          <PageHelp
            title={t("referenceLibrary.helpTitle")}
            paragraphs={[
              t("referenceLibrary.intro"),
              t("referenceLibrary.helpWhat"),
              t("referenceLibrary.helpWho"),
              t("referenceLibrary.helpSees"),
              t("referenceLibrary.helpEmbedding"),
            ]}
            ariaLabel={t("referenceLibrary.helpLabel")}
            testId="reflib-page-help"
          />
        </Group>
      </Group>
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
                  {t("referenceLibrary.empty")}
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
