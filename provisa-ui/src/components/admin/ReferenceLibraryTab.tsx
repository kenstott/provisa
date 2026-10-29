// Copyright (c) 2026 Kenneth Stott
// Canary: 6d2a8f41-9e7b-4c35-a1d0-8b5e3f7c2a96
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2185: the Admin Reference library page. The org's library documents in a list -- title,
// source, status, domains, when each was added and last refreshed or replaced -- with the selected
// one in the panel on the right: its facts and passages, Keep or Discard for a proposed one,
// Refresh for a URL document, Upload new version for a file one, Remove, and its domains. Adding a
// document opens its form in the same panel.

import React, { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  FileButton,
  FileInput,
  Group,
  MultiSelect,
  SegmentedControl,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Check, RefreshCw, Trash2, TriangleAlert, Upload, X } from "lucide-react";
import {
  addLibraryDocument,
  fetchLibrary,
  fetchLibraryDocument,
  keepLibraryDocument,
  refreshLibraryDocument,
  removeLibraryDocument,
  setLibraryDomains,
  uploadLibraryVersion,
  type LibraryDocument,
  type LibraryDocumentDetail,
} from "../../api/referenceLibrary";
import { useDomains } from "../../hooks/useAdminQueries";
import { ConfirmDialog } from "../ConfirmDialog";
import { DetailFields } from "../detail/DetailFields";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { ListTable, ListHead, ListRow, ListEmpty, ListItems } from "../list/ListTable";
import { PanelActions, SplitPane } from "../list/SplitPane";
import { useListSortGroup, type ListColumn } from "../list/useListSortGroup";
import { PageLoading } from "../PageLoading";
import { Tip } from "../Tip";
import { scopeChange, WHOLE_ORG } from "./referenceScope";

/** The files the library reads. */
const ACCEPT = ".html,.htm,.md,.markdown,.txt";

function when(value: string | null, language: string): string {
  return value ? new Date(value).toLocaleString(language) : "";
}

/** The later of a document's refresh and replacement: when its content last changed. */
function lastChanged(d: LibraryDocument): string | null {
  const times = [d.refreshed_at, d.replaced_at].filter((v): v is string => v !== null);
  return times.length ? times.sort().at(-1)! : null;
}

function DomainBadges({ domains }: { domains: string[] }) {
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

function StatusBadge({ status }: { status: LibraryDocument["status"] }) {
  const { t } = useTranslation();
  return (
    <Badge size="xs" variant="light" color={status === "proposed" ? "yellow" : "green"}>
      {status === "proposed"
        ? t("referenceLibrary.statusProposed")
        : t("referenceLibrary.statusKept")}
    </Badge>
  );
}

function useDomainOptions() {
  const { t } = useTranslation();
  const { domains } = useDomains();
  return useMemo(
    () => [
      { value: WHOLE_ORG, label: t("referenceLibrary.wholeOrg") },
      ...domains.map((d) => ({ value: d.id, label: d.id })),
    ],
    [domains, t],
  );
}

/** The form that adds a document, by a file or by a URL, in the panel. */
function AddDocumentForm({
  onAdded,
  onCancel,
}: {
  onAdded: (d: LibraryDocument) => void;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const options = useDomainOptions();
  const [mode, setMode] = useState<"file" | "url">("file");
  const [file, setFile] = useState<File | null>(null);
  const [url, setUrl] = useState("");
  const [title, setTitle] = useState("");
  const [domains, setDomains] = useState<string[]>([]);
  const [problem, setProblem] = useState("");

  const save = async () => {
    setProblem("");
    if (mode === "file" ? file === null : !url.trim()) {
      setProblem(
        mode === "file" ? t("referenceLibrary.fileRequired") : t("referenceLibrary.urlRequired"),
      );
      return;
    }
    if (domains.length === 0) {
      setProblem(t("referenceLibrary.domainsRequired"));
      return;
    }
    try {
      const added = await addLibraryDocument(
        mode === "file" && file ? { file, domains, title } : { url: url.trim(), domains, title },
      );
      onAdded(added);
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <FormLayout>
      <FormFooter
        panel
        onSave={save}
        onCancel={onCancel}
        problem={problem || undefined}
        saveProps={{ "data-testid": "reflib-add-save" }}
        cancelProps={{ "data-testid": "reflib-add-cancel" }}
      />
      <FormField size="full">
        <SegmentedControl
          data-testid="reflib-add-mode"
          value={mode}
          onChange={(v) => setMode(v as "file" | "url")}
          data={[
            { value: "file", label: t("referenceLibrary.byFile") },
            { value: "url", label: t("referenceLibrary.byUrl") },
          ]}
        />
      </FormField>
      {mode === "file" ? (
        <FormField help={t("referenceLibrary.fileHelp")} size="long">
          <FileInput
            label={t("referenceLibrary.file")}
            data-testid="reflib-add-file"
            accept={ACCEPT}
            value={file}
            onChange={setFile}
            clearable
          />
        </FormField>
      ) : (
        <FormField help={t("referenceLibrary.urlHelp")} size="long">
          <TextInput
            label={t("referenceLibrary.url")}
            data-testid="reflib-add-url"
            placeholder="https://"
            value={url}
            onChange={(e) => setUrl(e.currentTarget.value)}
          />
        </FormField>
      )}
      <FormField help={t("referenceLibrary.titleHelp")} size="long">
        <TextInput
          label={t("referenceLibrary.fieldTitle")}
          data-testid="reflib-add-title"
          value={title}
          onChange={(e) => setTitle(e.currentTarget.value)}
        />
      </FormField>
      <FormField help={t("referenceLibrary.domainsHelp")} size="long">
        <MultiSelect
          label={t("referenceLibrary.fieldDomains")}
          data-testid="reflib-add-domains"
          data={options}
          value={domains}
          onChange={(v) => setDomains(scopeChange(domains, v))}
          searchable
        />
      </FormField>
    </FormLayout>
  );
}

/** A document's facts, passages and actions, in the panel. */
function DocumentDetail({
  doc,
  onChanged,
  onRemoved,
}: {
  doc: LibraryDocument;
  onChanged: (d: LibraryDocument) => void;
  onRemoved: (id: number) => void;
}) {
  const { t, i18n } = useTranslation();
  const options = useDomainOptions();
  const [detail, setDetail] = useState<LibraryDocumentDetail | null>(null);
  const [domains, setDomains] = useState<string[]>(doc.domains);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [confirm, setConfirm] = useState<"remove" | "discard" | null>(null);

  useEffect(() => {
    let live = true;
    fetchLibraryDocument(doc.id)
      .then((d) => live && setDetail(d))
      .catch((e) => live && setError(String(e instanceof Error ? e.message : e)));
    return () => {
      live = false;
    };
    // The passages change with the content: refetched when the document is refreshed or replaced.
  }, [doc.id, doc.refreshed_at, doc.replaced_at]);

  const act = async (name: string, run: () => Promise<LibraryDocument>) => {
    setBusy(name);
    setError("");
    try {
      onChanged(await run());
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy("");
    }
  };

  const remove = async () => {
    setError("");
    try {
      await removeLibraryDocument(doc.id);
      onRemoved(doc.id);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const proposed = doc.status === "proposed";
  const domainsChanged =
    domains.length !== doc.domains.length || domains.some((d) => !doc.domains.includes(d));

  return (
    <Stack gap="sm" data-testid={`reflib-detail-${doc.id}`}>
      <PanelActions>
        <fieldset className="panel-action-set">
          <Group gap="xs">
            {proposed && (
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
            {doc.source_kind === "url" ? (
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
            )}
            <Tip
              text={proposed ? t("referenceLibrary.discard") : t("referenceLibrary.remove")}
              named
            >
              <ActionIcon
                color="red"
                aria-label={proposed ? t("referenceLibrary.discard") : t("referenceLibrary.remove")}
                data-testid={proposed ? "reflib-discard" : "reflib-remove"}
                onClick={() => setConfirm(proposed ? "discard" : "remove")}
              >
                {proposed ? <X size={14} /> : <Trash2 size={14} />}
              </ActionIcon>
            </Tip>
          </Group>
        </fieldset>
      </PanelActions>
      {error && (
        <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="reflib-detail-error">
          {error}
        </Alert>
      )}
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
          { label: t("referenceLibrary.fieldStatus"), value: <StatusBadge status={doc.status} /> },
          { label: t("referenceLibrary.fieldPassages"), value: String(doc.passages) },
          { label: t("referenceLibrary.fieldModel"), value: doc.embedding_model },
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
      <Group align="flex-end" gap="xs" wrap="nowrap">
        <MultiSelect
          style={{ flex: 1 }}
          label={t("referenceLibrary.fieldDomains")}
          data-testid="reflib-domains"
          data={options}
          value={domains}
          onChange={(v) => setDomains(scopeChange(domains, v))}
          searchable
        />
        <Button
          size="sm"
          variant="default"
          disabled={!domainsChanged || domains.length === 0}
          loading={busy === "domains"}
          data-testid="reflib-domains-save"
          onClick={() => act("domains", () => setLibraryDomains(doc.id, domains))}
        >
          {t("referenceLibrary.saveDomains")}
        </Button>
      </Group>
      <Text size="xs" c="dimmed" mt={-6}>
        {t("referenceLibrary.domainsHelp")}
      </Text>
      <Title order={6} mt="sm">
        {t("referenceLibrary.passagesHeading")}
      </Title>
      {detail === null ? (
        <Text size="sm" c="dimmed">
          {t("referenceLibrary.loadingPassages")}
        </Text>
      ) : (
        <Stack gap="xs" data-testid="reflib-passages">
          {detail.passage_list.map((p) => (
            <div key={p.ordinal} className="reflib-passage">
              <Text size="xs" c="dimmed">
                {[p.section, p.page !== null ? t("referenceLibrary.page", { page: p.page }) : null]
                  .filter(Boolean)
                  .join(" · ") || t("referenceLibrary.noSection")}
              </Text>
              <Text size="sm" lineClamp={4} style={{ whiteSpace: "pre-wrap" }}>
                {p.text}
              </Text>
            </div>
          ))}
        </Stack>
      )}
      <ConfirmDialog
        opened={confirm !== null}
        onCancel={() => setConfirm(null)}
        title={
          confirm === "discard"
            ? t("referenceLibrary.discardTitle")
            : t("referenceLibrary.removeTitle")
        }
        consequence={
          confirm === "discard"
            ? t("referenceLibrary.discardConsequence", { title: doc.title })
            : t("referenceLibrary.removeConsequence", { title: doc.title })
        }
        confirmLabel={
          confirm === "discard" ? t("referenceLibrary.discard") : t("referenceLibrary.remove")
        }
        onConfirm={remove}
        testId="reflib-confirm"
      />
    </Stack>
  );
}

export function ReferenceLibraryTab() {
  const { t, i18n } = useTranslation();
  const [documents, setDocuments] = useState<LibraryDocument[] | null>(null);
  const [error, setError] = useState("");
  const [selected, setSelected] = useState<number | null>(null);
  const [adding, setAdding] = useState(false);

  useEffect(() => {
    fetchLibrary()
      .then(setDocuments)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, []);

  const columns: ListColumn<LibraryDocument>[] = useMemo(
    () => [
      { key: "title", label: t("referenceLibrary.colTitle"), sortValue: (d) => d.title },
      { key: "source", label: t("referenceLibrary.colSource"), sortValue: (d) => d.source },
      {
        key: "status",
        label: t("referenceLibrary.colStatus"),
        sortValue: (d) => d.status,
        groupValue: (d) =>
          d.status === "proposed"
            ? t("referenceLibrary.statusProposed")
            : t("referenceLibrary.statusKept"),
      },
      {
        key: "domains",
        label: t("referenceLibrary.colDomains"),
        sortValue: (d) => d.domains.join(","),
      },
      { key: "added", label: t("referenceLibrary.colAdded"), sortValue: (d) => d.added_at },
      {
        key: "changed",
        label: t("referenceLibrary.colChanged"),
        sortValue: (d) => lastChanged(d) ?? "",
      },
    ],
    [t],
  );
  const sortGroup = useListSortGroup(documents ?? [], columns, "reference-library");

  if (documents === null && !error) return <PageLoading message={t("referenceLibrary.loading")} />;

  const doc = selected === null ? null : ((documents ?? []).find((d) => d.id === selected) ?? null);
  const replace = (d: LibraryDocument) =>
    setDocuments((prev) => (prev ?? []).map((x) => (x.id === d.id ? d : x)));

  const panel = adding ? (
    <AddDocumentForm
      onCancel={() => setAdding(false)}
      onAdded={(d) => {
        setDocuments((prev) => [d, ...(prev ?? [])]);
        setAdding(false);
        setSelected(d.id);
      }}
    />
  ) : doc ? (
    <DocumentDetail
      key={doc.id}
      doc={doc}
      onChanged={replace}
      onRemoved={(id) => {
        setDocuments((prev) => (prev ?? []).filter((x) => x.id !== id));
        setSelected(null);
      }}
    />
  ) : null;

  return (
    <Stack gap="sm" style={{ flex: 1, minHeight: 0 }} data-tour="reference-library">
      <Group justify="space-between" wrap="nowrap" align="flex-start">
        <Text c="dimmed" size="sm" maw={820}>
          {t("referenceLibrary.intro")}
        </Text>
        {!adding && (
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
      </Group>
      {error && (
        <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="reflib-error">
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
                { col: "status" },
                { col: "domains" },
                { col: "added" },
                { col: "changed" },
              ]}
            />
            <Table.Tbody>
              {(documents ?? []).length === 0 && (
                <ListEmpty colSpan={6} testId="reflib-empty">
                  {t("referenceLibrary.empty")}
                </ListEmpty>
              )}
              <ListItems
                state={sortGroup}
                colSpan={6}
                rowKey={(d) => String(d.id)}
                render={(d) => (
                  <React.Fragment key={d.id}>
                    <ListRow
                      testId={`reflib-row-${d.id}`}
                      selected={selected === d.id && !adding}
                      onClick={() => {
                        setAdding(false);
                        setSelected(selected === d.id ? null : d.id);
                      }}
                    >
                      <Table.Td>
                        <Text size="sm" fw={600}>
                          {d.title}
                        </Text>
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs" ff="monospace" style={{ wordBreak: "break-all" }}>
                          {d.source}
                        </Text>
                      </Table.Td>
                      <Table.Td>
                        <StatusBadge status={d.status} />
                      </Table.Td>
                      <Table.Td>
                        <DomainBadges domains={d.domains} />
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs">{when(d.added_at, i18n.language)}</Text>
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs">{when(lastChanged(d), i18n.language)}</Text>
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
    </Stack>
  );
}
