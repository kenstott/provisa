// Copyright (c) 2026 Kenneth Stott
// Canary: 4b7e2d91-c6a3-4f58-8e0b-19d5a7c3f264
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259: Context Health's cards, each built only from a signal already specified and each
// number linked to the page it counts, drawn as a link only for a viewer holding that page's right.

import { useEffect, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Card, Group, Loader, Stack, Text, Title } from "@mantine/core";
import { EntityLink, EntityPageLink } from "../../components/EntityLink";
import {
  fetchOntologyDrift,
  fetchStewardshipSummary,
  listGlossaryTerms,
  type OntologyDrift,
} from "../../api/glossary";
import { fetchClassificationAwaiting, type AwaitingTable } from "../../api/contextViews";
import { fetchReviewsDue, type DocumentReviewsDue } from "../../api/referenceLibrary";
import { fetchReview, fetchSyncing, type SyncEntry } from "../../api/syncing";

/** A read a card shows: loading, its value, or the server's refusal in words. */
type Read<T> =
  { state: "loading" } | { state: "ready"; value: T } | { state: "failed"; error: string };

const message = (e: unknown) => (e instanceof Error ? e.message : String(e));

/** ``load``'s result, read again whenever ``seq`` changes: loading until the read for this
 * ``seq`` is in. */
function useRead<T>(load: () => Promise<T>, seq = 0): Read<T> {
  const [held, setHeld] = useState<{ seq: number; read: Read<T> } | null>(null);
  useEffect(() => {
    let live = true;
    load().then(
      (value) => live && setHeld({ seq, read: { state: "ready", value } }),
      (e: unknown) => live && setHeld({ seq, read: { state: "failed", error: message(e) } }),
    );
    return () => {
      live = false;
    };
  }, [load, seq]);
  return held === null || held.seq !== seq ? { state: "loading" } : held.read;
}

function HealthCard({
  title,
  testId,
  children,
}: {
  title: string;
  testId: string;
  children: ReactNode;
}) {
  return (
    <Card withBorder padding="md" radius="md" data-testid={testId}>
      <Stack gap="xs">
        <Title order={5}>{title}</Title>
        {children}
      </Stack>
    </Card>
  );
}

/** The card's body once ``read`` is in: a loader until then, the refusal if it failed. */
function Body<T>({
  read,
  testId,
  children,
}: {
  read: Read<T>;
  testId: string;
  children: (value: T) => ReactNode;
}) {
  if (read.state === "loading") return <Loader size="xs" />;
  if (read.state === "failed") {
    return (
      <Alert color="red" p="xs" data-testid={`${testId}-error`}>
        {read.error}
      </Alert>
    );
  }
  return <>{children(read.value)}</>;
}

/** One counted signal: its label and its number, the number linked to ``href``. */
export function CountRow({
  label,
  count,
  href,
  testId,
}: {
  label: string;
  count: number;
  href: string;
  testId: string;
}) {
  return (
    <Group justify="space-between" wrap="nowrap" gap="sm">
      <Text size="sm">{label}</Text>
      <Text size="sm" fw={600}>
        <EntityPageLink href={href} testId={testId}>
          {count}
        </EntityPageLink>
      </Text>
    </Group>
  );
}

const ONTOLOGY = "/context/ontology";

interface TermCounts {
  agentVisible: number;
  fromData: number;
  noData: number;
  drafts: number;
}

async function termCounts(): Promise<TermCounts> {
  const [terms, summary] = await Promise.all([
    listGlossaryTerms("", false),
    fetchStewardshipSummary(),
  ]);
  const approved = terms.filter((t) => t.status === "approved");
  return {
    agentVisible: summary.agent_visible.length,
    fromData: approved.filter((t) => t.grounding === "grounded").length,
    noData: summary.approved_not_grounded.length,
    drafts: terms.filter((t) => t.status === "draft").length,
  };
}

/** Terms visible to agents, approved and reaching data, approved but reaching none, drafts. */
export function TermsCard({ seq }: { seq: number }) {
  const { t } = useTranslation();
  const read = useRead(termCounts, seq);
  return (
    <HealthCard title={t("contextHealth.termsTitle")} testId="health-terms">
      <Body read={read} testId="health-terms">
        {(c) => (
          <Stack gap={4}>
            <CountRow
              label={t("contextHealth.termsAgentVisible")}
              count={c.agentVisible}
              href={ONTOLOGY}
              testId="health-terms-agent-visible"
            />
            <CountRow
              label={t("contextHealth.termsFromData")}
              count={c.fromData}
              href={ONTOLOGY}
              testId="health-terms-from-data"
            />
            <CountRow
              label={t("contextHealth.termsNoData")}
              count={c.noData}
              href={ONTOLOGY}
              testId="health-terms-no-data"
            />
            <CountRow
              label={t("contextHealth.termsDrafts")}
              count={c.drafts}
              href={ONTOLOGY}
              testId="health-terms-drafts"
            />
          </Stack>
        )}
      </Body>
    </HealthCard>
  );
}

/** REQ-1386: the grounding drift since the previous period, as the Ontology quality report has it. */
export const DRIFT_REPORT = "/admin/reports?report=ontology_quality";

export function DriftCard() {
  const { t } = useTranslation();
  const read = useRead<OntologyDrift>(fetchOntologyDrift);
  return (
    <HealthCard title={t("contextHealth.driftTitle")} testId="health-drift">
      <Body read={read} testId="health-drift">
        {(drift) =>
          drift.counts === null ? (
            // No commit to compare with is no history, never zero drift.
            <Text size="sm" c="dimmed" data-testid="health-drift-no-history">
              {t("contextHealth.driftNoHistory")}
            </Text>
          ) : (
            <Stack gap={4}>
              <CountRow
                label={t("contextHealth.driftLost")}
                count={drift.counts.lost}
                href={DRIFT_REPORT}
                testId="health-drift-lost"
              />
              <CountRow
                label={t("contextHealth.driftGained")}
                count={drift.counts.gained}
                href={DRIFT_REPORT}
                testId="health-drift-gained"
              />
              <CountRow
                label={t("contextHealth.driftNew")}
                count={drift.counts.new}
                href={DRIFT_REPORT}
                testId="health-drift-new"
              />
            </Stack>
          )
        }
      </Body>
    </HealthCard>
  );
}

const total = (rows: { awaiting: number }[], hidden: number) =>
  rows.reduce((n, r) => n + r.awaiting, hidden);

/** Classification detections no one has accepted or rejected, per table the viewer reads. */
export function ClassificationCard() {
  const { t } = useTranslation();
  const read = useRead<{ tables: AwaitingTable[]; hidden: number }>(fetchClassificationAwaiting);
  return (
    <HealthCard title={t("contextHealth.classificationTitle")} testId="health-classification">
      <Body read={read} testId="health-classification">
        {({ tables, hidden }) => (
          <Stack gap={4}>
            <CountRow
              label={t("contextHealth.classificationAwaiting")}
              count={total(tables, hidden)}
              href="/context/classification"
              testId="health-classification-total"
            />
            {tables.map((row) => (
              <Group key={row.tableId} justify="space-between" wrap="nowrap" gap="sm">
                <Text size="xs" truncate="end">
                  {/* Decisions are made on the table's latest profile, on its Quality tab. */}
                  <EntityLink
                    kind="profile"
                    id={row.tableId}
                    table={row.tableId}
                    testId={`health-classification-table-${row.tableId}`}
                  >
                    {row.tableRef}
                  </EntityLink>
                </Text>
                <Text size="xs">{row.awaiting}</Text>
              </Group>
            ))}
            {hidden > 0 && (
              <Text size="xs" c="dimmed" data-testid="health-classification-hidden">
                {t("contextViews.hiddenPlaces", { count: hidden })}
              </Text>
            )}
          </Stack>
        )}
      </Body>
    </HealthCard>
  );
}

/** REQ-2208: the documents whose citations hold review items. */
export function DocumentsCard() {
  const { t } = useTranslation();
  const read = useRead<{ documents: DocumentReviewsDue[] }>(fetchReviewsDue);
  return (
    <HealthCard title={t("contextHealth.documentsTitle")} testId="health-documents">
      <Body read={read} testId="health-documents">
        {({ documents }) => (
          <Stack gap={4}>
            <CountRow
              label={t("contextHealth.documentsDue")}
              count={documents.length}
              href="/context/documents"
              testId="health-documents-total"
            />
            {documents.map((d) => (
              <Group key={d.id} justify="space-between" wrap="nowrap" gap="sm">
                <Text size="xs" truncate="end">
                  <EntityLink kind="document" id={d.id} testId={`health-documents-${d.id}`}>
                    {d.title}
                  </EntityLink>
                </Text>
                <Text size="xs">{t("contextHealth.documentsReviews", { count: d.reviews })}</Text>
              </Group>
            ))}
          </Stack>
        )}
      </Body>
    </HealthCard>
  );
}

/** REQ-2259, REQ-2145: tags lost. A confirmed tag loss is not recorded yet (job CTX-TAGLOSS), so
 * the card says so rather than showing a number it does not have. */
export function TagsLostCard() {
  const { t } = useTranslation();
  return (
    <HealthCard title={t("contextHealth.tagsLostTitle")} testId="health-tags-lost">
      <Text size="sm" c="dimmed" data-testid="health-tags-lost-unrecorded">
        {t("contextHealth.notYetRecorded")}
      </Text>
    </HealthCard>
  );
}

/** The differences an entry has waiting: a catalog read for them, or why it could not be read. An
 * entry that is not a data catalog, or names no role, has none to read (syncing_router). */
type Waiting =
  { kind: "none" } | { kind: "count"; total: number } | { kind: "failed"; error: string };

async function waitingOf(entry: SyncEntry): Promise<Waiting> {
  if (entry.type !== "data_catalog" || entry.role === null) return { kind: "none" };
  try {
    return { kind: "count", total: (await fetchReview(entry.id)).total };
  } catch (e) {
    return { kind: "failed", error: message(e) };
  }
}

async function syncRows(): Promise<{ entry: SyncEntry; waiting: Waiting }[]> {
  const { entries } = await fetchSyncing();
  return Promise.all(entries.map(async (entry) => ({ entry, waiting: await waitingOf(entry) })));
}

const SYNC = "/context/sync";

function LastRun({ entry }: { entry: SyncEntry }) {
  const { t } = useTranslation();
  const run = entry.last_run;
  if (run === null) {
    return (
      <Text size="xs" c="dimmed">
        {t("contextHealth.syncNoRun")}
      </Text>
    );
  }
  return (
    <Text size="xs" c={run.ok ? undefined : "red"} data-testid={`health-sync-run-${entry.id}`}>
      {run.ok
        ? t("contextHealth.syncRunOk", { count: run.total_published })
        : t("contextHealth.syncRunFailed", { count: run.errors.length })}
    </Text>
  );
}

/** REQ-2247: per catalog, the differences waiting, the last sync and its failures. Drawn only for a
 * viewer holding Context Sync's right: every read behind it asks it. */
export function SyncCard() {
  const { t } = useTranslation();
  const read = useRead(syncRows);
  return (
    <HealthCard title={t("contextHealth.syncTitle")} testId="health-sync">
      <Body read={read} testId="health-sync">
        {(rows) =>
          rows.length === 0 ? (
            <Text size="sm" c="dimmed" data-testid="health-sync-none">
              {t("contextHealth.syncNone")}
            </Text>
          ) : (
            <Stack gap="xs">
              {rows.map(({ entry, waiting }) => (
                <Stack key={entry.id} gap={0} data-testid={`health-sync-${entry.id}`}>
                  <Group justify="space-between" wrap="nowrap" gap="sm">
                    <Text size="sm" fw={600} truncate="end">
                      {entry.id}
                    </Text>
                    {waiting.kind === "count" && (
                      <Text size="sm">
                        <EntityPageLink href={SYNC} testId={`health-sync-waiting-${entry.id}`}>
                          {t("contextHealth.syncWaiting", { count: waiting.total })}
                        </EntityPageLink>
                      </Text>
                    )}
                  </Group>
                  <Text size="xs" c="dimmed">
                    {t(`syncingTab.direction_${entry.direction}`)}
                  </Text>
                  <LastRun entry={entry} />
                  {waiting.kind === "failed" && (
                    <Text size="xs" c="red" data-testid={`health-sync-failed-${entry.id}`}>
                      {waiting.error}
                    </Text>
                  )}
                </Stack>
              ))}
            </Stack>
          )
        }
      </Body>
    </HealthCard>
  );
}
