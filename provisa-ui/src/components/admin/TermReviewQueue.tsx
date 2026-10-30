// Copyright (c) 2026 Kenneth Stott
// Canary: 9a1d5e83-4c27-4b0f-a6e8-37f2c0b9d415
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2179: the review queue of one scan. Each candidate term is a draft card: its category, what
// the scan proposes (a new term, or a merge into the glossary term it duplicates or names again),
// its definition -- the document's own, or a synthesized one marked so -- the synonyms it would
// add, every source passage it came from, and explanations that disagree side by side. The
// steward edits, accepts, merges or rejects each, or accepts the selected ones at once; accepted
// terms are then handed to Generate relationships.

import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Card,
  Checkbox,
  Group,
  Loader,
  SegmentedControl,
  Select,
  SimpleGrid,
  Stack,
  TagsInput,
  Text,
  Textarea,
  TextInput,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import {
  ArrowLeft,
  Check,
  GitMerge,
  Pencil,
  Share2,
  X,
} from "lucide-react";
import {
  acceptDraft,
  bulkAccept,
  editDraft,
  fetchScan,
  mergeDraft,
  rejectDraft,
  type Citation,
  type Draft,
  type Scan,
  type TermRef,
} from "../../api/termFinding";
import { generateGlossaryRelationships, type GlossaryTermSummary } from "../../api/glossary";
import { Tip } from "../Tip";
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";

/** How often a running scan is read again, in milliseconds. */
const POLL_MS = 2000;

export function ScanStatusBadge({ status }: { status: Scan["status"] }) {
  const { t } = useTranslation();
  const [label, color] =
    status === "running"
      ? [t("findTerms.statusRunning"), "blue"]
      : status === "done"
        ? [t("findTerms.statusDone"), "green"]
        : [t("findTerms.statusFailed"), "red"];
  return (
    <Badge size="xs" variant="light" color={color}>
      {label}
    </Badge>
  );
}

function useTermName() {
  const { t } = useTranslation();
  return (ref: TermRef | null) =>
    ref === null ? "" : "outside_domains" in ref ? t("findTerms.outsideDomains") : ref.name;
}

const CATEGORY_KEYS: Record<string, string> = {
  "business concept": "findTerms.categoryBusinessConcept",
  role: "findTerms.categoryRole",
  "organization type": "findTerms.categoryOrganizationType",
  policy: "findTerms.categoryPolicy",
  process: "findTerms.categoryProcess",
  regulation: "findTerms.categoryRegulation",
};

const VARIANT_KEYS: Record<string, string> = {
  abbreviation: "findTerms.variantAbbreviation",
  "alternative name": "findTerms.variantAlternativeName",
  form: "findTerms.variantForm",
  reference: "findTerms.variantReference",
};

function where(c: Citation, t: (k: string, o?: Record<string, unknown>) => string): string {
  return [
    c.section ? t("findTerms.section", { section: c.section }) : null,
    c.page !== null ? t("findTerms.page", { page: c.page }) : null,
  ]
    .filter(Boolean)
    .join(", ");
}

function PassageQuote({ citation, document }: { citation: Citation; document: string }) {
  const { t } = useTranslation();
  const [label, color] =
    citation.role === "definition"
      ? [t("findTerms.roleDefinition"), "teal"]
      : citation.role === "synthesis"
        ? [t("findTerms.roleSynthesis"), "violet"]
        : [t("findTerms.roleSupporting"), "gray"];
  const place = where(citation, t);
  return (
    <Stack gap={2} data-testid="find-terms-passage">
      <Group gap={6}>
        <Badge size="xs" variant="light" color={color}>
          {label}
        </Badge>
        <Text size="xs" c="dimmed">
          {place ? t("findTerms.passageWhere", { document, where: place }) : document}
        </Text>
      </Group>
      <Text
        size="sm"
        fs="italic"
        style={{ borderLeft: "2px solid var(--mantine-color-gray-4)", paddingLeft: 8 }}
      >
        {citation.quote}
      </Text>
    </Stack>
  );
}

function DefinitionBadge({ kind }: { kind: Draft["definition_kind"] }) {
  const { t } = useTranslation();
  if (kind === "explicit")
    return (
      <Badge size="xs" variant="light" color="teal">
        {t("findTerms.definitionExplicit")}
      </Badge>
    );
  if (kind === "synthesized")
    return (
      <Tip text={t("findTerms.definitionSynthesizedHint")}>
        <Badge size="xs" variant="light" color="violet" data-testid="find-terms-synthesized">
          {t("findTerms.definitionSynthesized")}
        </Badge>
      </Tip>
    );
  return (
    <Badge size="xs" variant="light" color="orange">
      {t("findTerms.definitionMissing")}
    </Badge>
  );
}

function ProposalBadge({ draft }: { draft: Draft }) {
  const { t } = useTranslation();
  const termName = useTermName();
  if (draft.status === "accepted")
    return (
      <Badge size="sm" color="green">
        {t("findTerms.statusAccepted")}
      </Badge>
    );
  if (draft.status === "merged")
    return (
      <Badge size="sm" color="green">
        {t("findTerms.statusMerged", { term: termName(draft.term) })}
      </Badge>
    );
  if (draft.status === "rejected")
    return (
      <Badge size="sm" color="gray">
        {t("findTerms.statusRejected")}
      </Badge>
    );
  if (draft.proposal === "new")
    return (
      <Badge size="sm" variant="light" color="blue">
        {t("findTerms.proposalNew")}
      </Badge>
    );
  return (
    <Badge size="sm" variant="light" color="yellow" data-testid="find-terms-match">
      {draft.match_kind === "duplicate"
        ? t("findTerms.proposalDuplicate", { term: termName(draft.match) })
        : t("findTerms.proposalSynonym", { term: termName(draft.match) })}
    </Badge>
  );
}

function DraftCard({
  draft,
  document,
  terms,
  selected,
  onSelect,
  onChanged,
}: {
  draft: Draft;
  document: string;
  terms: GlossaryTermSummary[];
  selected: boolean;
  onSelect: (on: boolean) => void;
  onChanged: (message: string) => void;
}) {
  const { t } = useTranslation();
  const open = draft.status === "draft";
  const [editing, setEditing] = useState(false);
  const [merging, setMerging] = useState(false);
  const [name, setName] = useState(draft.name);
  const [definition, setDefinition] = useState(draft.definition ?? "");
  const [synonyms, setSynonyms] = useState(draft.variants.filter((v) => v.keep).map((v) => v.text));
  const [target, setTarget] = useState<string | null>(
    draft.match && "term_id" in draft.match ? String(draft.match.term_id) : null,
  );
  const [useDefinition, setUseDefinition] = useState(false);
  const [problem, setProblem] = useState("");
  const [busy, setBusy] = useState(false);

  const run = async (fn: () => Promise<unknown>, message: string) => {
    setProblem("");
    setBusy(true);
    try {
      await fn();
      onChanged(message);
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const toggleSynonym = (text: string, on: boolean) =>
    void run(
      () =>
        editDraft(draft.id, {
          synonyms: on ? [...synonyms, text] : synonyms.filter((s) => s !== text),
        }).then((d) => setSynonyms(d.variants.filter((v) => v.keep).map((v) => v.text))),
      "",
    );

  return (
    <Card withBorder padding="sm" data-testid={`find-terms-draft-${draft.id}`}>
      <Stack gap="xs">
        <Group justify="space-between" align="flex-start" wrap="nowrap">
          <Group gap="xs" align="center" wrap="wrap">
            {open && (
              <Checkbox
                checked={selected}
                onChange={(e) => onSelect(e.currentTarget.checked)}
                aria-label={t("findTerms.select", { name: draft.name })}
                data-testid={`find-terms-select-${draft.id}`}
              />
            )}
            <Text fw={600}>{draft.name}</Text>
            <Badge size="xs" variant="outline" color="gray">
              {CATEGORY_KEYS[draft.category] ? t(CATEGORY_KEYS[draft.category]) : draft.category}
            </Badge>
            <ProposalBadge draft={draft} />
          </Group>
          {open && !editing && !merging && (
            <Group gap={4} wrap="nowrap">
              {draft.proposal === "new" ? (
                <Button
                  size="xs"
                  leftSection={<Check size={12} />}
                  loading={busy}
                  onClick={() =>
                    void run(
                      () => acceptDraft(draft.id),
                      t("findTerms.acceptDone", { name: draft.name }),
                    )
                  }
                  data-testid={`find-terms-accept-${draft.id}`}
                >
                  {t("findTerms.accept")}
                </Button>
              ) : null}
              <Button
                size="xs"
                variant={draft.proposal === "merge" ? "filled" : "light"}
                leftSection={<GitMerge size={12} />}
                onClick={() => setMerging(true)}
                data-testid={`find-terms-merge-${draft.id}`}
              >
                {t("findTerms.merge")}
              </Button>
              <Button
                size="xs"
                variant="light"
                leftSection={<Pencil size={12} />}
                onClick={() => setEditing(true)}
                data-testid={`find-terms-edit-${draft.id}`}
              >
                {t("findTerms.edit")}
              </Button>
              <Button
                size="xs"
                variant="subtle"
                color="red"
                leftSection={<X size={12} />}
                onClick={() =>
                  void run(
                    () => rejectDraft(draft.id),
                    t("findTerms.rejectDone", { name: draft.name }),
                  )
                }
                data-testid={`find-terms-reject-${draft.id}`}
              >
                {t("findTerms.reject")}
              </Button>
            </Group>
          )}
        </Group>
        {draft.sense && (
          <Text size="xs" c="dimmed">
            {draft.sense}
          </Text>
        )}
        {draft.homonyms.length > 0 && (
          <Text size="xs" c="orange" data-testid="find-terms-homonyms">
            {t("findTerms.homonyms", { names: draft.homonyms.join(", ") })}
          </Text>
        )}

        {editing ? (
          <FormLayout>
            <FormField size="medium">
              <TextInput
                label={t("findTerms.name")}
                value={name}
                onChange={(e) => setName(e.currentTarget.value)}
                data-testid="find-terms-edit-name"
              />
            </FormField>
            <FormField size="full">
              <Textarea
                label={t("findTerms.definition")}
                value={definition}
                onChange={(e) => setDefinition(e.currentTarget.value)}
                autosize
                minRows={2}
                data-testid="find-terms-edit-definition"
              />
            </FormField>
            <FormField size="long" help={t("findTerms.synonymsHelp")}>
              <TagsInput
                label={t("findTerms.synonyms")}
                value={synonyms}
                onChange={setSynonyms}
                data-testid="find-terms-edit-synonyms"
              />
            </FormField>
            <FormFooter
              saving={busy}
              onCancel={() => setEditing(false)}
              onSave={() =>
                run(
                  () =>
                    editDraft(draft.id, { name, definition: definition || null, synonyms }).then(
                      () => setEditing(false),
                    ),
                  "",
                )
              }
              saveProps={{ "data-testid": "find-terms-edit-save" }}
            />
          </FormLayout>
        ) : (
          <>
            <Stack gap={4}>
              <Group gap={6}>
                <Text size="xs" fw={600} c="dimmed" tt="uppercase">
                  {t("findTerms.definition")}
                </Text>
                <DefinitionBadge kind={draft.definition_kind} />
              </Group>
              {draft.definition && <Text size="sm">{draft.definition}</Text>}
            </Stack>
            <Stack gap={4}>
              <Text size="xs" fw={600} c="dimmed" tt="uppercase">
                {t("findTerms.synonyms")}
              </Text>
              {draft.variants.length ? (
                <Group gap="xs">
                  {draft.variants.map((v) => (
                    <Checkbox
                      key={v.text}
                      size="xs"
                      disabled={!open || busy}
                      checked={synonyms.includes(v.text)}
                      onChange={(e) => toggleSynonym(v.text, e.currentTarget.checked)}
                      label={`${v.text} (${VARIANT_KEYS[v.kind] ? t(VARIANT_KEYS[v.kind]) : v.kind})`}
                      data-testid="find-terms-variant"
                    />
                  ))}
                </Group>
              ) : (
                <Text size="xs" c="dimmed">
                  {t("findTerms.noSynonyms")}
                </Text>
              )}
            </Stack>
          </>
        )}

        {merging && (
          <FormLayout>
            <FormField size="medium">
              <Select
                label={t("findTerms.mergeInto")}
                placeholder={t("findTerms.mergeIntoPlaceholder")}
                searchable
                value={target}
                onChange={setTarget}
                data={terms.map((term) => ({ value: String(term.id), label: term.name }))}
                data-testid="find-terms-merge-target"
              />
            </FormField>
            <FormChecks>
              <Checkbox
                label={t("findTerms.useDefinition")}
                checked={useDefinition}
                disabled={!draft.definition}
                onChange={(e) => setUseDefinition(e.currentTarget.checked)}
                data-testid="find-terms-use-definition"
              />
            </FormChecks>
            <FormFooter
              onCancel={() => setMerging(false)}
              action={
                <Button
                  disabled={target === null}
                  loading={busy}
                  onClick={() =>
                    void run(
                      () =>
                        mergeDraft(draft.id, {
                          term_id: Number(target),
                          use_definition: useDefinition,
                        }),
                      t("findTerms.mergeDone", { name: draft.name }),
                    )
                  }
                  data-testid="find-terms-merge-confirm"
                >
                  {t("findTerms.confirmMerge")}
                </Button>
              }
            />
          </FormLayout>
        )}

        {draft.conflicts.length > 0 && (
          <Stack gap={4} data-testid="find-terms-conflicts">
            <Tip text={t("findTerms.conflictsHint")}>
              <Text size="xs" fw={600} c="red" tt="uppercase">
                {t("findTerms.conflicts")}
              </Text>
            </Tip>
            {draft.conflicts.map(([a, b]) => (
              <SimpleGrid key={`${a.mention}-${b.mention}`} cols={{ base: 1, sm: 2 }} spacing="sm">
                <PassageQuote citation={a} document={document} />
                <PassageQuote citation={b} document={document} />
              </SimpleGrid>
            ))}
          </Stack>
        )}

        <Collapsibles id="find-terms.passages">
          <CollapsibleItem
            value={`draft-${draft.id}`}
            defaultOpen={false}
            controlTestId={`find-terms-passages-toggle-${draft.id}`}
            title={
              <Text size="xs" c="blue">
                {t("findTerms.passages", { count: draft.passages.length })}
              </Text>
            }
          >
            <Stack gap="xs">
              {draft.passages.map((p) => (
                <PassageQuote key={p.mention} citation={p} document={document} />
              ))}
            </Stack>
          </CollapsibleItem>
        </Collapsibles>
        {problem && (
          <Alert color="red" data-testid="find-terms-draft-problem">
            {problem}
          </Alert>
        )}
      </Stack>
    </Card>
  );
}

export function TermReviewQueue({
  scanId,
  terms,
  onBack,
  onGlossaryChanged,
}: {
  scanId: number;
  terms: GlossaryTermSummary[];
  onBack: () => void;
  onGlossaryChanged: () => void;
}) {
  const { t } = useTranslation();
  const [scan, setScan] = useState<Scan | null>(null);
  const [drafts, setDrafts] = useState<Draft[]>([]);
  const [filter, setFilter] = useState<"review" | "all">("review");
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [problem, setProblem] = useState("");
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    try {
      const found = await fetchScan(scanId);
      setScan(found.scan);
      setDrafts(found.drafts);
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    }
  }, [scanId]);

  useEffect(() => {
    const timer = window.setTimeout(() => void load(), 0);
    return () => window.clearTimeout(timer);
  }, [load]);

  // A running scan is read again until it is done or has failed.
  useEffect(() => {
    if (scan?.status !== "running") return;
    const timer = window.setTimeout(() => void load(), POLL_MS);
    return () => window.clearTimeout(timer);
  }, [scan, load]);

  const toReview = useMemo(() => drafts.filter((d) => d.status === "draft"), [drafts]);
  const shown = filter === "review" ? toReview : drafts;
  const anyDecided = drafts.some((d) => d.status === "accepted" || d.status === "merged");

  const changed = async (message: string) => {
    if (message) notifications.show({ color: "green", message });
    await load();
    onGlossaryChanged();
  };

  const acceptSelected = async () => {
    setProblem("");
    setBusy(true);
    try {
      const ids = toReview.filter((d) => selected.has(d.id)).map((d) => d.id);
      const { decided, refused } = await bulkAccept(scanId, ids);
      setSelected(new Set());
      if (refused.length) {
        const names = new Map(drafts.map((d) => [d.id, d.name]));
        setProblem(
          refused
            .map((r) => t("findTerms.bulkRefused", { name: names.get(r.id), message: r.message }))
            .join("\n"),
        );
      }
      await changed(t("findTerms.bulkDone", { count: decided.length }));
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const proposeRelationships = async () => {
    setProblem("");
    setBusy(true);
    try {
      const { added } = await generateGlossaryRelationships();
      await changed(t("findTerms.relationshipsDone", { count: added }));
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  if (scan === null) {
    return problem ? <Alert color="red">{problem}</Alert> : <Loader size="sm" />;
  }
  const summary = scan.summary;
  const allSelected = toReview.length > 0 && toReview.every((d) => selected.has(d.id));

  return (
    <Stack gap="md" data-testid="find-terms-queue">
      <Group justify="space-between" wrap="nowrap">
        <Group gap="xs" wrap="nowrap">
          <Button
            size="xs"
            variant="subtle"
            leftSection={<ArrowLeft size={14} />}
            onClick={onBack}
            data-testid="find-terms-back"
          >
            {t("findTerms.back")}
          </Button>
          <Title order={5}>{scan.document_title}</Title>
          <ScanStatusBadge status={scan.status} />
          {scan.document_removed && (
            <Tip text={t("findTerms.removedDocumentHint")}>
              <Badge size="xs" variant="light" color="gray">
                {t("findTerms.removedDocument")}
              </Badge>
            </Tip>
          )}
        </Group>
      </Group>

      {scan.status === "running" && (
        <Group gap="xs" data-testid="find-terms-running">
          <Loader size="xs" />
          <Text size="sm">{t("findTerms.scanning")}</Text>
        </Group>
      )}
      {scan.status === "failed" && (
        <Alert color="red" data-testid="find-terms-failed">
          {t("findTerms.failed", { error: scan.error })}
        </Alert>
      )}
      {summary && (
        <Stack gap={2}>
          <Text size="sm" data-testid="find-terms-summary">
            {t("findTerms.summary", {
              passages: summary.passages,
              mentions: summary.mentions,
              candidates: summary.candidates,
            })}
          </Text>
          <Text size="xs" c="dimmed">
            {t("findTerms.leftOut", {
              count: summary.left_out.not_a_concept,
              misquoted: summary.left_out.quote_not_in_passage,
              unresolved: summary.left_out.unresolved,
            })}
          </Text>
        </Stack>
      )}

      {scan.status === "done" && (
        <Group justify="space-between" wrap="wrap">
          <Group gap="sm">
            <Checkbox
              checked={allSelected}
              disabled={!toReview.length}
              onChange={(e) =>
                setSelected(
                  e.currentTarget.checked ? new Set(toReview.map((d) => d.id)) : new Set(),
                )
              }
              aria-label={t("findTerms.selectAll")}
              data-testid="find-terms-select-all"
            />
            <Tip text={t("findTerms.acceptSelectedHint")}>
              <Button
                size="xs"
                leftSection={<Check size={12} />}
                disabled={!toReview.some((d) => selected.has(d.id))}
                loading={busy}
                onClick={() => void acceptSelected()}
                data-testid="find-terms-accept-selected"
              >
                {t("findTerms.acceptSelected")}
              </Button>
            </Tip>
            <Tip text={t("findTerms.proposeRelationshipsHint")}>
              <Button
                size="xs"
                variant="light"
                leftSection={<Share2 size={12} />}
                disabled={!anyDecided}
                loading={busy}
                onClick={() => void proposeRelationships()}
                data-testid="find-terms-propose-relationships"
              >
                {t("findTerms.proposeRelationships")}
              </Button>
            </Tip>
          </Group>
          <SegmentedControl
            size="xs"
            value={filter}
            onChange={(v) => setFilter(v as "review" | "all")}
            data={[
              { value: "review", label: `${t("findTerms.filterToReview")} (${toReview.length})` },
              { value: "all", label: `${t("findTerms.filterAll")} (${drafts.length})` },
            ]}
            data-testid="find-terms-filter"
          />
        </Group>
      )}
      {problem && (
        <Alert
          color="red"
          style={{ whiteSpace: "pre-line" }}
          data-testid="find-terms-queue-problem"
        >
          {problem}
        </Alert>
      )}

      {scan.status === "done" && !shown.length && (
        <Text size="sm" c="dimmed">
          {t("findTerms.queueEmpty")}
        </Text>
      )}
      <Stack gap="sm">
        {shown.map((d) => (
          <DraftCard
            key={`${d.id}-${d.status}`}
            draft={d}
            document={scan.document_title}
            terms={terms}
            selected={selected.has(d.id)}
            onSelect={(on) =>
              setSelected((prev) => {
                const next = new Set(prev);
                if (on) next.add(d.id);
                else next.delete(d.id);
                return next;
              })
            }
            onChanged={(message) => void changed(message)}
          />
        ))}
      </Stack>
    </Stack>
  );
}
