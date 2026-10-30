// Copyright (c) 2026 Kenneth Stott
// Canary: 2f7a9c40-b18e-4d63-95c2-6e0d8b3a1f57
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2179: the glossary's Find terms in a document. The steward picks a reference library
// document they may read, or gives a new one as a file or URL (it joins the library as proposed,
// in the domains they are working in), and starts a scan. The scans made so far are listed below,
// each opening its review queue, and any can be scanned again on demand (REQ-2185): nothing is
// re-reviewed automatically.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  type ComboboxData,
  Alert,
  Badge,
  Button,
  FileInput,
  Group,
  Modal,
  MultiSelect,
  SegmentedControl,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { RotateCw, ScanSearch } from "lucide-react";
import {
  fetchScannableDocuments,
  fetchScans,
  startScan,
  type Scan,
  type ScannableDocument,
  type ScanSource,
} from "../../api/termFinding";
import type { GlossaryTermSummary } from "../../api/glossary";
import { FormField, FormLayout } from "../form/FormLayout";
import { Tip } from "../Tip";
import { ScanStatusBadge, TermReviewQueue } from "./TermReviewQueue";

/** The files the reference library reads. */
const ACCEPT = ".html,.htm,.md,.markdown,.txt,.pdf,.docx";

type Mode = "library" | "file" | "url";

/** The chooser: a library document, or a new file or URL, and the steward's domains. */
function ScanForm({
  documents,
  domainOptions,
  domainsEnabled,
  defaultDomains,
  onStarted,
}: {
  documents: ScannableDocument[];
  domainOptions: ComboboxData;
  domainsEnabled: boolean;
  defaultDomains: string[];
  onStarted: (scan: Scan) => void;
}) {
  const { t } = useTranslation();
  const [mode, setMode] = useState<Mode>(documents.length ? "library" : "file");
  const [documentId, setDocumentId] = useState<string | null>(null);
  const [file, setFile] = useState<File | null>(null);
  const [url, setUrl] = useState("");
  const [domains, setDomains] = useState<string[]>(defaultDomains);
  const [problem, setProblem] = useState("");
  const [starting, setStarting] = useState(false);

  const source = (): ScanSource | null => {
    if (mode === "library") return documentId ? { document_id: Number(documentId) } : null;
    if (mode === "file") return file ? { file } : null;
    return url.trim() ? { url: url.trim() } : null;
  };

  const start = async () => {
    setProblem("");
    const chosen = source();
    if (chosen === null) {
      setProblem(t("findTerms.chooseDocument"));
      return;
    }
    if (domainsEnabled && domains.length === 0) {
      setProblem(t("findTerms.chooseDomains"));
      return;
    }
    setStarting(true);
    try {
      const { scan } = await startScan(chosen, domainsEnabled ? domains : []);
      onStarted(scan);
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    } finally {
      setStarting(false);
    }
  };

  return (
    <FormLayout>
      <FormField size="full">
        <Text size="sm" c="dimmed">
          {t("findTerms.intro")}
        </Text>
      </FormField>
      <FormField size="full">
        <SegmentedControl
          data-testid="find-terms-mode"
          value={mode}
          onChange={(v) => setMode(v as Mode)}
          data={[
            { value: "library", label: t("findTerms.fromLibrary") },
            { value: "file", label: t("findTerms.newFile") },
            { value: "url", label: t("findTerms.newUrl") },
          ]}
        />
      </FormField>
      {mode === "library" && (
        <FormField size="long" help={t("findTerms.documentHelp")}>
          {documents.length ? (
            <Select
              label={t("findTerms.document")}
              placeholder={t("findTerms.documentPlaceholder")}
              data-testid="find-terms-document"
              searchable
              value={documentId}
              onChange={setDocumentId}
              data={documents.map((d) => ({ value: String(d.id), label: d.title }))}
            />
          ) : (
            <Text size="sm" c="dimmed" data-testid="find-terms-no-documents">
              {t("findTerms.noDocuments")}
            </Text>
          )}
        </FormField>
      )}
      {mode === "file" && (
        <FormField size="long" help={t("findTerms.fileHelp")}>
          <FileInput
            label={t("findTerms.file")}
            data-testid="find-terms-file"
            accept={ACCEPT}
            value={file}
            onChange={setFile}
            clearable
          />
        </FormField>
      )}
      {mode === "url" && (
        <FormField size="long" help={t("findTerms.urlHelp")}>
          <TextInput
            label={t("findTerms.url")}
            data-testid="find-terms-url"
            placeholder="https://"
            value={url}
            onChange={(e) => setUrl(e.currentTarget.value)}
          />
        </FormField>
      )}
      {mode !== "library" && (
        <FormField size="full">
          <Text size="xs" c="dimmed" data-testid="find-terms-new-note">
            {t("findTerms.newDocumentNote")}
          </Text>
        </FormField>
      )}
      {domainsEnabled && (
        <FormField size="long" help={t("findTerms.domainsHelp")}>
          <MultiSelect
            label={t("findTerms.domains")}
            data-testid="find-terms-domains"
            data={domainOptions}
            value={domains}
            onChange={setDomains}
            required
          />
        </FormField>
      )}
      {problem && (
        <FormField size="full">
          <Alert color="red" data-testid="find-terms-problem">
            {problem}
          </Alert>
        </FormField>
      )}
      <FormField size="full">
        <Group justify="space-between" wrap="nowrap">
          <Text size="xs" c="dimmed">
            {t("findTerms.budgetNote")}
          </Text>
          <Button
            leftSection={<ScanSearch size={14} />}
            onClick={() => void start()}
            loading={starting}
            data-testid="find-terms-start"
          >
            {t("findTerms.start")}
          </Button>
        </Group>
      </FormField>
    </FormLayout>
  );
}

function ScanList({
  scans,
  onOpen,
  onRescan,
}: {
  scans: Scan[];
  onOpen: (id: number) => void;
  onRescan: (scan: Scan) => void;
}) {
  const { t, i18n } = useTranslation();
  if (!scans.length) {
    return (
      <Text size="sm" c="dimmed" data-testid="find-terms-no-scans">
        {t("findTerms.scansEmpty")}
      </Text>
    );
  }
  return (
    <Table striped highlightOnHover data-testid="find-terms-scans">
      <Table.Thead>
        <Table.Tr>
          <Table.Th>{t("findTerms.colDocument")}</Table.Th>
          <Table.Th>{t("findTerms.colStarted")}</Table.Th>
          <Table.Th>{t("findTerms.colStatus")}</Table.Th>
          <Table.Th>{t("findTerms.colQueue")}</Table.Th>
          <Table.Th />
        </Table.Tr>
      </Table.Thead>
      <Table.Tbody>
        {scans.map((s) => {
          const draft = s.counts.draft ?? 0;
          const decided =
            (s.counts.accepted ?? 0) + (s.counts.merged ?? 0) + (s.counts.rejected ?? 0);
          return (
            <Table.Tr key={s.id} data-testid={`find-terms-scan-${s.id}`}>
              <Table.Td>
                <Group gap={6} wrap="nowrap">
                  <Text size="sm">{s.document_title}</Text>
                  {s.document_removed && (
                    <Tip text={t("findTerms.removedDocumentHint")}>
                      <Badge size="xs" variant="light" color="gray">
                        {t("findTerms.removedDocument")}
                      </Badge>
                    </Tip>
                  )}
                </Group>
              </Table.Td>
              <Table.Td>
                <Text size="sm">{new Date(s.started_at).toLocaleString(i18n.language)}</Text>
              </Table.Td>
              <Table.Td>
                <ScanStatusBadge status={s.status} />
              </Table.Td>
              <Table.Td>
                <Text size="sm">{t("findTerms.queueCounts", { draft, decided })}</Text>
              </Table.Td>
              <Table.Td>
                <Group gap="xs" justify="flex-end" wrap="nowrap">
                  {s.document_id !== null && (
                    <Tip text={t("findTerms.rescanHint")}>
                      <Button
                        size="xs"
                        variant="subtle"
                        leftSection={<RotateCw size={12} />}
                        onClick={() => onRescan(s)}
                        data-testid={`find-terms-rescan-${s.id}`}
                      >
                        {t("findTerms.rescan")}
                      </Button>
                    </Tip>
                  )}
                  <Button
                    size="xs"
                    variant="light"
                    onClick={() => onOpen(s.id)}
                    data-testid={`find-terms-open-${s.id}`}
                  >
                    {t("findTerms.open")}
                  </Button>
                </Group>
              </Table.Td>
            </Table.Tr>
          );
        })}
      </Table.Tbody>
    </Table>
  );
}

export function FindTermsDialog({
  terms,
  domainOptions,
  domainsEnabled,
  defaultDomains,
  onClose,
  onGlossaryChanged,
}: {
  terms: GlossaryTermSummary[];
  domainOptions: ComboboxData;
  domainsEnabled: boolean;
  defaultDomains: string[];
  onClose: () => void;
  onGlossaryChanged: () => void;
}) {
  const { t } = useTranslation();
  const [documents, setDocuments] = useState<ScannableDocument[] | null>(null);
  const [scans, setScans] = useState<Scan[]>([]);
  const [openScan, setOpenScan] = useState<number | null>(null);
  const [problem, setProblem] = useState("");

  const load = useCallback(async () => {
    try {
      const [docs, found] = await Promise.all([fetchScannableDocuments(), fetchScans()]);
      setDocuments(docs);
      setScans(found);
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
      setDocuments([]);
    }
  }, []);

  useEffect(() => {
    const timer = window.setTimeout(() => void load(), 0);
    return () => window.clearTimeout(timer);
  }, [load]);

  const rescan = async (s: Scan) => {
    setProblem("");
    try {
      const { scan } = await startScan({ document_id: s.document_id! }, s.domains);
      setOpenScan(scan.id);
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <Modal
      opened
      onClose={onClose}
      title={t("findTerms.title")}
      size="min(1400px, 94vw)"
      data-testid="find-terms-dialog"
    >
      {openScan !== null ? (
        <TermReviewQueue
          scanId={openScan}
          terms={terms}
          onBack={() => {
            setOpenScan(null);
            void load();
          }}
          onGlossaryChanged={onGlossaryChanged}
        />
      ) : (
        <Stack gap="lg">
          {documents !== null && (
            <ScanForm
              documents={documents}
              domainOptions={domainOptions}
              domainsEnabled={domainsEnabled}
              defaultDomains={defaultDomains}
              onStarted={(scan) => setOpenScan(scan.id)}
            />
          )}
          {problem && (
            <Alert color="red" data-testid="find-terms-load-problem">
              {problem}
            </Alert>
          )}
          <Stack gap="xs">
            <Title order={5}>{t("findTerms.scansTitle")}</Title>
            <ScanList scans={scans} onOpen={setOpenScan} onRescan={(s) => void rescan(s)} />
          </Stack>
        </Stack>
      )}
    </Modal>
  );
}
