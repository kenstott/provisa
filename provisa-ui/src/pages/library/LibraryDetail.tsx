// Copyright (c) 2026 Kenneth Stott
// Canary: e15b7c30-94d2-4f6a-a3b8-07c6d2e9f4a1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2185: a library document in the Library page's panel, in the same section tabs for reading
// and for changing it: overview and status; sections and passages; what cites it, a citation whose
// words the document no longer holds flagged; and who reads it. Only the library-editing right
// sees the actions and the readers form.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  FileButton,
  Group,
  MultiSelect,
  Stack,
  Table,
  Tabs,
  Text,
} from "@mantine/core";
import { Check, DatabaseZap, RefreshCw, RotateCw, TriangleAlert, Upload, X } from "lucide-react";
import {
  fetchCitedBy,
  fetchLibraryDocument,
  keepLibraryDocument,
  reembedLibraryDocument,
  refreshLibraryDocument,
  removeLibraryDocument,
  retryLibraryDocument,
  setLibraryDomains,
  uploadLibraryVersion,
  type LibraryCitedBy,
  type LibraryDocument,
  type LibraryDocumentDetail,
} from "../../api/referenceLibrary";
import { scopeChange } from "../../components/admin/referenceScope";
import { DetailFields } from "../../components/detail/DetailFields";
import { FormField, FormFooter, FormLayout } from "../../components/form/FormLayout";
import { PanelDelete } from "../../components/list/PanelDelete";
import { PanelActions } from "../../components/list/SplitPane";
import { TagControl } from "../../components/TagControl";
import { objectHref, objectName } from "../../components/tagObjectLink";
import { detailHref } from "../../components/list/useDetailParam";
import { Link } from "react-router-dom";
import { Tip } from "../../components/Tip";
import { IndexBadge, ReaderBadges, StaleBadge, StatusBadge } from "./LibraryBadges";
import { ACCEPT, useReaderOptions, when } from "./libraryParts";

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/** Whether passage ``i`` is the first of its section: the one before it is of another section. */
function opensSection(passages: LibraryDocumentDetail["passage_list"], i: number): boolean {
  if (i === 0) return true;
  const [before, p] = [passages[i - 1], passages[i]];
  return before.section !== p.section || (!p.section && before.page !== p.page);
}

function SectionsTab({
  doc,
  detail,
}: {
  doc: LibraryDocument;
  detail: LibraryDocumentDetail | null;
}) {
  const { t } = useTranslation();
  if (doc.index_status !== "ready") {
    return (
      <Text size="sm" c="dimmed" data-testid="reflib-not-indexed">
        {t("referenceLibrary.notYetIndexed")}
      </Text>
    );
  }
  if (detail === null) {
    return (
      <Text size="sm" c="dimmed">
        {t("referenceLibrary.loadingPassages")}
      </Text>
    );
  }
  return (
    <Stack gap="xs" data-testid="reflib-passages">
      {detail.passage_list.map((p, i) => (
        <div key={p.ordinal} className="reflib-passage">
          <Group gap="xs" wrap="nowrap">
            <Text size="xs" c="dimmed">
              {[p.section, p.page !== null ? t("referenceLibrary.page", { page: p.page }) : null]
                .filter(Boolean)
                .join(" · ") || t("referenceLibrary.noSection")}
            </Text>
            {/* REQ-2208: a section's tags, at the first of its passages. A section is named by its
                heading, or by its page when it has none; one with neither is the document's
                untitled opening, tagged as the document. */}
            {opensSection(detail.passage_list, i) && (p.section || p.page !== null) && (
              <TagControl
                objectType="section"
                documentId={doc.id}
                sectionHeading={p.section || null}
                sectionPage={p.section ? null : p.page}
              />
            )}
          </Group>
          <Text size="sm" lineClamp={4} style={{ whiteSpace: "pre-wrap" }}>
            {p.text}
          </Text>
        </div>
      ))}
    </Stack>
  );
}

function CitedByTab({ cited }: { cited: LibraryCitedBy | null }) {
  const { t } = useTranslation();
  if (cited === null) {
    return (
      <Text size="sm" c="dimmed">
        {t("referenceLibrary.loadingPassages")}
      </Text>
    );
  }
  const [tags, assignments] = [cited.tags, cited.tag_assignments];
  if (cited.terms.length === 0 && tags.length === 0 && assignments.length === 0) {
    return (
      <Text size="sm" c="dimmed" data-testid="reflib-cited-empty">
        {t("referenceLibrary.citedByEmpty")}
      </Text>
    );
  }
  const where = (c: { section_heading: string | null; section_page: number | null }) =>
    c.section_heading ??
    (c.section_page !== null
      ? t("referenceLibrary.page", { page: c.section_page })
      : t("tagCitations.citedByWholeDocument"));
  const mark = (c: { state: string; needs_review: boolean }) =>
    c.needs_review && (
      <Badge
        size="xs"
        variant="light"
        color={c.state === "drifted" ? "orange" : "red"}
        data-testid={`reflib-cite-${c.state}`}
      >
        {t(c.state === "drifted" ? "referenceLibrary.citedByDrifted" : "tagCitations.citedByGone")}
      </Badge>
    );
  return (
    <Stack gap="md">
      {cited.terms.length > 0 && (
        <Table data-testid="reflib-cited-by">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("referenceLibrary.citedByTerm")}</Table.Th>
              <Table.Th>{t("referenceLibrary.citedByWhere")}</Table.Th>
              <Table.Th>{t("referenceLibrary.citedByQuote")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {cited.terms.map((c) => (
              <Table.Tr
                key={`${c.term_id}-${c.ordinal}-${c.quote}`}
                data-testid={`reflib-cite-${c.term_id}`}
              >
                <Table.Td>
                  <Group gap={4} wrap="wrap">
                    <Text size="sm">{c.term}</Text>
                    {c.state === "drifted" && (
                      <Tip text={t("referenceLibrary.citedByDriftedHelp")}>
                        <Badge
                          size="xs"
                          variant="light"
                          color="orange"
                          data-testid="reflib-drifted"
                        >
                          {t("referenceLibrary.citedByDrifted")}
                        </Badge>
                      </Tip>
                    )}
                  </Group>
                </Table.Td>
                <Table.Td>
                  <Text size="xs" c="dimmed">
                    {[
                      c.section,
                      c.page !== null ? t("referenceLibrary.page", { page: c.page }) : null,
                    ]
                      .filter(Boolean)
                      .join(" · ") || t("referenceLibrary.noSection")}
                  </Text>
                </Table.Td>
                <Table.Td>
                  <Text size="sm" lineClamp={3}>
                    {c.quote}
                  </Text>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
      {(tags.length > 0 || assignments.length > 0) && (
        <Table data-testid="reflib-cited-by-tags">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("tagCitations.citedByTag")}</Table.Th>
              <Table.Th>{t("tagCitations.citedByOn")}</Table.Th>
              <Table.Th>{t("referenceLibrary.citedByWhere")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {tags.map((c) => (
              <Table.Tr
                key={`def-${c.tag_id}-${c.section_heading}-${c.section_page}`}
                data-testid={`reflib-cite-tag-${c.tag_id}`}
              >
                <Table.Td>
                  <Group gap={4} wrap="wrap">
                    <Text size="sm" component={Link} to={detailHref("tags", c.tag_id)}>
                      {c.tag_id}
                    </Text>
                    {mark(c)}
                  </Group>
                </Table.Td>
                <Table.Td>
                  <Text size="xs" c="dimmed">
                    {t("tagCitations.citedByDefinition")}
                  </Text>
                </Table.Td>
                <Table.Td>
                  <Text size="xs">{where(c)}</Text>
                </Table.Td>
              </Table.Tr>
            ))}
            {assignments.map((c) => (
              <Table.Tr
                key={`on-${c.tag_id}-${c.object_key}-${c.section_heading}-${c.section_page}`}
                data-testid={`reflib-cite-assignment-${c.tag_id}-${c.object_key}`}
              >
                <Table.Td>
                  <Group gap={4} wrap="wrap">
                    <Text
                      size="sm"
                      component={Link}
                      to={detailHref("tags", c.tag_id.split(":")[0])}
                    >
                      {c.tag_id}
                    </Text>
                    {mark(c)}
                  </Group>
                </Table.Td>
                <Table.Td>
                  <Text size="xs" component={Link} to={objectHref(c.object_type, c.object_key)}>
                    {t(`tagsTab.scope_${c.object_type}`)} · {objectName(c.object_key)}
                  </Text>
                </Table.Td>
                <Table.Td>
                  <Text size="xs">{where(c)}</Text>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </Stack>
  );
}

function ReadersTab({
  doc,
  canEdit,
  onChanged,
}: {
  doc: LibraryDocument;
  canEdit: boolean;
  onChanged: (d: LibraryDocument) => void;
}) {
  const { t } = useTranslation();
  const options = useReaderOptions();
  const [readers, setReaders] = useState<string[]>(doc.domains);
  const [saving, setSaving] = useState(false);
  const [problem, setProblem] = useState("");
  const changed =
    readers.length !== doc.domains.length || readers.some((d) => !doc.domains.includes(d));

  if (!canEdit) {
    return (
      <DetailFields
        testId={`reflib-readers-${doc.id}`}
        fields={[
          {
            label: t("referenceLibrary.fieldReaders"),
            value: <ReaderBadges domains={doc.domains} />,
          },
        ]}
      />
    );
  }

  const save = async () => {
    setSaving(true);
    setProblem("");
    try {
      onChanged(await setLibraryDomains(doc.id, readers));
    } catch (e) {
      setProblem(message(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <FormLayout>
      <FormField help={t("referenceLibrary.readersHelp")} size="long">
        <MultiSelect
          label={t("referenceLibrary.fieldReaders")}
          data-testid="reflib-domains"
          data={options}
          value={readers}
          onChange={(v) => setReaders(scopeChange(readers, v))}
          searchable
        />
      </FormField>
      <FormFooter
        saving={saving}
        onSave={save}
        problem={problem || undefined}
        saveProps={{
          "data-testid": "reflib-domains-save",
          disabled: !changed || readers.length === 0,
        }}
      />
    </FormLayout>
  );
}

export function LibraryDetail({
  doc,
  canEdit,
  onChanged,
  onRemoved,
}: {
  doc: LibraryDocument;
  canEdit: boolean;
  onChanged: (d: LibraryDocument) => void;
  onRemoved: (id: number) => void;
}) {
  const { t, i18n } = useTranslation();
  const [detail, setDetail] = useState<LibraryDocumentDetail | null>(null);
  const [cited, setCited] = useState<LibraryCitedBy | null>(null);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const ready = doc.index_status === "ready";

  useEffect(() => {
    if (!ready) return;
    let live = true;
    fetchLibraryDocument(doc.id)
      .then((d) => live && setDetail(d))
      .catch((e) => live && setError(message(e)));
    fetchCitedBy(doc.id)
      .then((c) => live && setCited(c))
      .catch((e) => live && setError(message(e)));
    return () => {
      live = false;
    };
    // The passages and their citations change with the content: read again on refresh or replace.
  }, [doc.id, doc.refreshed_at, doc.replaced_at, ready]);

  const act = async (name: string, run: () => Promise<LibraryDocument>) => {
    setBusy(name);
    setError("");
    try {
      onChanged(await run());
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy("");
    }
  };

  const remove = async () => {
    setError("");
    await removeLibraryDocument(doc.id);
    onRemoved(doc.id);
  };

  const proposed = doc.status === "proposed";

  return (
    <Stack gap="sm" data-testid={`reflib-detail-${doc.id}`}>
      {canEdit && (
        <PanelActions>
          <fieldset className="panel-action-set">
            <Group gap="xs">
              {doc.index_status === "failed" && (
                <Tip text={t("referenceLibrary.retry")} named>
                  <ActionIcon
                    aria-label={t("referenceLibrary.retry")}
                    data-testid="reflib-retry"
                    loading={busy === "retry"}
                    onClick={() => act("retry", () => retryLibraryDocument(doc.id))}
                  >
                    <RotateCw size={14} />
                  </ActionIcon>
                </Tip>
              )}
              {ready && proposed && (
                <Tip text={t("referenceLibrary.keep")} named>
                  <ActionIcon
                    color="green"
                    aria-label={t("referenceLibrary.keep")}
                    data-testid="reflib-keep"
                    loading={busy === "keep"}
                    onClick={() => act("keep", () => keepLibraryDocument(doc.id))}
                  >
                    <Check size={14} />
                  </ActionIcon>
                </Tip>
              )}
              {doc.needs_reembedding && (
                <Tip text={t("referenceLibrary.reembed")} named>
                  <ActionIcon
                    aria-label={t("referenceLibrary.reembed")}
                    data-testid="reflib-reembed"
                    loading={busy === "reembed"}
                    onClick={() => act("reembed", () => reembedLibraryDocument(doc.id))}
                  >
                    <DatabaseZap size={14} />
                  </ActionIcon>
                </Tip>
              )}
              {ready &&
                (doc.source_kind === "url" ? (
                  <Tip text={t("referenceLibrary.refresh")} named>
                    <ActionIcon
                      aria-label={t("referenceLibrary.refresh")}
                      data-testid="reflib-refresh"
                      loading={busy === "refresh"}
                      onClick={() => act("refresh", () => refreshLibraryDocument(doc.id))}
                    >
                      <RefreshCw size={14} />
                    </ActionIcon>
                  </Tip>
                ) : (
                  <FileButton
                    accept={ACCEPT}
                    onChange={(file) =>
                      file && act("version", () => uploadLibraryVersion(doc.id, file))
                    }
                  >
                    {(props) => (
                      <Tip text={t("referenceLibrary.uploadVersion")} named>
                        <ActionIcon
                          {...props}
                          aria-label={t("referenceLibrary.uploadVersion")}
                          data-testid="reflib-upload-version"
                          loading={busy === "version"}
                        >
                          <Upload size={14} />
                        </ActionIcon>
                      </Tip>
                    )}
                  </FileButton>
                ))}
              {/* REQ-2198: resolves once the document is out of the list. */}
              <PanelDelete
                itemKey={String(doc.id)}
                title={
                  proposed ? t("referenceLibrary.discardTitle") : t("referenceLibrary.removeTitle")
                }
                consequence={
                  proposed
                    ? t("referenceLibrary.discardConsequence", { title: doc.title })
                    : t("referenceLibrary.removeConsequence", { title: doc.title })
                }
                confirmLabel={
                  proposed ? t("referenceLibrary.discard") : t("referenceLibrary.remove")
                }
                ariaLabel={proposed ? t("referenceLibrary.discard") : t("referenceLibrary.remove")}
                icon={proposed ? <X size={14} /> : undefined}
                testId={proposed ? "reflib-discard" : "reflib-remove"}
                confirmTestId="reflib-confirm"
                onDelete={remove}
              />
            </Group>
          </fieldset>
        </PanelActions>
      )}
      {error && (
        <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="reflib-detail-error">
          {error}
        </Alert>
      )}
      <Tabs defaultValue="overview" keepMounted={false}>
        <Tabs.List aria-label={t("referenceLibrary.detailPanel")}>
          <Tabs.Tab value="overview" data-testid="reflib-tab-overview">
            {t("referenceLibrary.tabOverview")}
          </Tabs.Tab>
          <Tabs.Tab value="sections" data-testid="reflib-tab-sections">
            {t("referenceLibrary.tabSections")}
          </Tabs.Tab>
          <Tabs.Tab
            value="cited"
            data-testid="reflib-tab-cited"
            rightSection={
              cited && cited.drifted > 0 ? (
                <Badge
                  size="xs"
                  color="orange"
                  circle
                  aria-label={t("referenceLibrary.citedByDriftedCount", { n: cited.drifted })}
                  data-testid="reflib-drifted-count"
                >
                  {cited.drifted}
                </Badge>
              ) : undefined
            }
          >
            {t("referenceLibrary.tabCitedBy")}
          </Tabs.Tab>
          <Tabs.Tab value="readers" data-testid="reflib-tab-readers">
            {t("referenceLibrary.tabReaders")}
          </Tabs.Tab>
        </Tabs.List>
        <Tabs.Panel value="overview" pt="sm">
          <DetailFields
            testId={`reflib-read-${doc.id}`}
            fields={[
              { label: t("referenceLibrary.fieldTitle"), value: doc.title },
              {
                label: t("referenceLibrary.fieldSource"),
                value:
                  doc.source_kind === "url" ? (
                    <a href={doc.source} target="_blank" rel="noreferrer">
                      {doc.source}
                    </a>
                  ) : (
                    doc.source
                  ),
              },
              { label: t("referenceLibrary.fieldIndex"), value: <IndexBadge doc={doc} /> },
              ...(doc.index_error
                ? [{ label: t("referenceLibrary.fieldIndexError"), value: doc.index_error }]
                : []),
              ...(doc.terms_note
                ? [{ label: t("referenceLibrary.fieldTermsNote"), value: doc.terms_note }]
                : []),
              {
                label: t("referenceLibrary.fieldStatus"),
                value: <StatusBadge status={doc.status} />,
              },
              // REQ-2208: the registry's tags on the document, the chip every taggable object has.
              {
                label: t("referenceLibrary.fieldTags"),
                value: <TagControl objectType="document" documentId={doc.id} />,
              },
              { label: t("referenceLibrary.fieldPassages"), value: String(doc.passages) },
              {
                label: t("referenceLibrary.fieldModel"),
                value: (
                  <Group gap="xs">
                    <span>{doc.embedding_model}</span>
                    <StaleBadge doc={doc} />
                  </Group>
                ),
              },
              { label: t("referenceLibrary.fieldAdded"), value: when(doc.added_at, i18n.language) },
              {
                label: t("referenceLibrary.fieldRefreshed"),
                value: when(doc.refreshed_at, i18n.language),
              },
              {
                label: t("referenceLibrary.fieldReplaced"),
                value: when(doc.replaced_at, i18n.language),
              },
            ]}
          />
        </Tabs.Panel>
        <Tabs.Panel value="sections" pt="sm">
          <SectionsTab doc={doc} detail={detail} />
        </Tabs.Panel>
        <Tabs.Panel value="cited" pt="sm">
          <CitedByTab cited={cited} />
        </Tabs.Panel>
        <Tabs.Panel value="readers" pt="sm">
          <ReadersTab doc={doc} canEdit={canEdit} onChanged={onChanged} />
        </Tabs.Panel>
      </Tabs>
    </Stack>
  );
}
