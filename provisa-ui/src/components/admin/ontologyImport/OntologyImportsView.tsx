// Copyright (c) 2026 Kenneth Stott
// Canary: 5c9e1a36-b7f4-4d82-a0e3-74d2f8c15b90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2237: the Ontology page's imports. Import ontology reads a file into a new import; the list
// names each import the steward may see with its items and alignments counted by status and what
// was left out of the file; picking one opens its review in the detail panel.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Button, Group, Stack, Table, Text } from "@mantine/core";
import { FileUp } from "lucide-react";
import {
  listOntologyImports,
  type CountsByStatus,
  type OntologyImport,
} from "../../../api/ontologyImport";
import type { GlossaryTermSummary } from "../../../api/glossary";
import { ListRow } from "../../list/ListTable";
import { SortGroupTable } from "../../list/SortGroupTable";
import { SplitPane } from "../../list/SplitPane";
import { ImportOntologyDialog } from "./ImportOntologyDialog";
import { ImportReviewPanel } from "./ImportReviewPanel";

const COUNT_COLOUR: Record<string, string> = {
  pending: "yellow",
  excluded: "gray",
  applied: "teal",
  proposed: "yellow",
  approved: "teal",
  rejected: "gray",
  written: "blue",
};

/** Counts by status as small badges, the statuses with none left out. */
function Counts({
  counts,
  testId,
}: {
  counts: CountsByStatus[keyof CountsByStatus];
  testId: string;
}) {
  const { t } = useTranslation();
  const entries = Object.entries(counts).filter(([, n]) => (n ?? 0) > 0);
  if (entries.length === 0) return <Text size="sm">0</Text>;
  return (
    <Group gap={4} wrap="wrap" data-testid={testId}>
      {entries.map(([status, n]) => (
        <Badge key={status} size="xs" variant="light" color={COUNT_COLOUR[status] ?? "gray"}>
          {t("ontologyImport.countOf", { count: n, status: t(`ontologyImport.status_${status}`) })}
        </Badge>
      ))}
    </Group>
  );
}

function leftOutTotal(i: OntologyImport): number {
  return Object.values(i.summary.left_out).reduce((a, b) => a + b, 0);
}

export function OntologyImportsView({
  terms,
  domainOptions,
  domainsEnabled,
  defaultDomains,
}: {
  terms: GlossaryTermSummary[];
  /** The domains a new term may be made in, as the glossary's own pickers offer them. */
  domainOptions: (string | { value: string; label: string })[];
  domainsEnabled: boolean;
  defaultDomains: string[];
}) {
  const { t } = useTranslation();
  const [imports, setImports] = useState<OntologyImport[] | null>(null);
  const [error, setError] = useState("");
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [importOpen, setImportOpen] = useState(false);

  const refresh = useCallback(async () => {
    try {
      setImports(await listOntologyImports());
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  useEffect(() => {
    const timer = window.setTimeout(() => void refresh(), 0);
    return () => window.clearTimeout(timer);
  }, [refresh]);

  const selected = imports?.find((i) => i.id === selectedId) ?? null;

  return (
    <Stack gap="md" style={{ flex: 1, minHeight: 0 }} data-testid="ontology-imports">
      <Group gap="xs" data-toolbar>
        <Button
          leftSection={<FileUp size={14} />}
          onClick={() => setImportOpen(true)}
          data-testid="ontology-import-btn"
        >
          {t("ontologyImport.importButton")}
        </Button>
      </Group>
      {error && (
        <Alert color="red" data-testid="ontology-imports-error">
          {error}
        </Alert>
      )}
      <SplitPane
        testId="ontology-imports-split"
        storageKey="provisa.ontologyImports.detailWidth"
        title={selected?.title ?? ""}
        onClose={() => setSelectedId(null)}
        labels={{
          panel: t("ontologyImport.panel"),
          resize: t("ontologyImport.resizePanel"),
          expand: t("ontologyImport.expandPanel"),
          restore: t("ontologyImport.restorePanel"),
          close: t("ontologyImport.closePanel"),
          backToList: t("ontologyImport.backToList"),
        }}
        list={
          <SortGroupTable
            testPrefix="ontology-imports"
            minWidth={720}
            rows={imports ?? []}
            initialSortCol="imported"
            columns={[
              { key: "title", label: t("ontologyImport.colTitle"), sortValue: (i) => i.title },
              {
                key: "format",
                label: t("ontologyImport.format"),
                sortValue: (i) => i.format,
                groupValue: (i) => i.format,
              },
              { key: "items", label: t("ontologyImport.colItems") },
              { key: "alignments", label: t("ontologyImport.colAlignments") },
              {
                key: "leftOut",
                label: t("ontologyImport.leftOut"),
                sortValue: (i) => leftOutTotal(i),
              },
              {
                key: "imported",
                label: t("ontologyImport.imported"),
                sortValue: (i) => i.imported_at ?? "",
              },
            ]}
            headers={[
              { col: "title" },
              { col: "format" },
              { col: "items" },
              { col: "alignments" },
              { col: "leftOut" },
              { col: "imported" },
            ]}
            colSpan={6}
            empty={imports === null ? t("ontologyImport.loading") : t("ontologyImport.noImports")}
            emptyTestId="ontology-imports-empty"
            rowKey={(i) => i.id}
            render={(i) => (
              <ListRow
                key={i.id}
                testId={`ontology-import-row-${i.id}`}
                selected={i.id === selectedId}
                onClick={() => setSelectedId(i.id === selectedId ? null : i.id)}
              >
                <Table.Td>
                  <Stack gap={0}>
                    <Text size="sm" fw={500}>
                      {i.title}
                    </Text>
                    <Text size="xs" c="dimmed">
                      {i.file_name}
                    </Text>
                  </Stack>
                </Table.Td>
                <Table.Td>{t(`ontologyImport.format_${i.format}`)}</Table.Td>
                <Table.Td>
                  <Counts counts={i.counts.items} testId={`ontology-import-items-${i.id}`} />
                </Table.Td>
                <Table.Td>
                  <Counts
                    counts={i.counts.alignments}
                    testId={`ontology-import-alignments-${i.id}`}
                  />
                </Table.Td>
                <Table.Td data-testid={`ontology-import-left-out-${i.id}`}>
                  {leftOutTotal(i)}
                </Table.Td>
                <Table.Td>
                  <Stack gap={0}>
                    <Text size="sm">
                      {i.imported_at ? new Date(i.imported_at).toLocaleString() : "—"}
                    </Text>
                    {i.applied_at && (
                      <Badge size="xs" variant="light" color="teal">
                        {t("ontologyImport.appliedBadge")}
                      </Badge>
                    )}
                  </Stack>
                </Table.Td>
              </ListRow>
            )}
          />
        }
      >
        {selected ? (
          <ImportReviewPanel
            key={selected.id}
            importId={selected.id}
            terms={terms}
            onChanged={() => void refresh()}
            onDiscarded={async () => {
              setSelectedId(null);
              await refresh();
            }}
          />
        ) : null}
      </SplitPane>
      {importOpen && (
        <ImportOntologyDialog
          domainOptions={domainOptions.map((o) =>
            typeof o === "string" ? { id: o, label: o } : { id: o.value, label: o.label },
          )}
          domainsEnabled={domainsEnabled}
          defaultDomains={defaultDomains}
          onClose={() => setImportOpen(false)}
          onImported={(review) => {
            setImportOpen(false);
            void refresh().then(() => setSelectedId(review.import.id));
          }}
        />
      )}
    </Stack>
  );
}
