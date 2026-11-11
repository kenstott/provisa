// Copyright (c) 2026 Kenneth Stott
// Canary: 8f5b2d70-c1a9-4e36-b8d4-0a6e93f27c15
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2237: an import's review, in the imports list's detail panel. What the file held and what
// was left out of it; its items by kind, each with its definition and IRI and the alignments
// proposed for it, each decided by the steward (one at a time, or the selected ones at once); an
// item renamed, its definition edited, excluded or included again; an alignment of the steward's
// own added; and Apply, what it will do shown first.

import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Checkbox,
  Group,
  Loader,
  Pagination,
  Paper,
  SegmentedControl,
  Stack,
  Text,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Ban, CheckCheck, Pencil, Plus, RotateCcw, Upload } from "lucide-react";
import {
  ITEM_KINDS,
  approveAlignment,
  bulkApprove,
  discardOntologyImport,
  editImportItem,
  fetchOntologyImport,
  rejectAlignment,
  type ImportItem,
  type ImportReview,
  type ItemKind,
} from "../../../api/ontologyImport";
import type { GlossaryTermSummary } from "../../../api/glossary";
import { DetailFields } from "../../detail/DetailFields";
import { PanelActions } from "../../list/SplitPane";
import { PanelDelete } from "../../list/PanelDelete";
import { FilterInput } from "../FilterInput";
import { AlignmentRow } from "./AlignmentRow";
import { AddAlignmentDialog, EditItemDialog } from "./ItemDialogs";
import { ApplyImportDialog } from "./ApplyImportDialog";

const PAGE_SIZE = 25;

const ITEM_STATUS_COLOUR: Record<ImportItem["status"], string> = {
  pending: "yellow",
  excluded: "gray",
  applied: "teal",
};

function failure(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/** What the file held, and what was left out of it by why. */
function ImportSummary({ review }: { review: ImportReview }) {
  const { t } = useTranslation();
  const i = review.import;
  const leftOut = Object.entries(i.summary.left_out).sort(([a], [b]) => a.localeCompare(b));
  const when = (at: string | null, by: string | null) =>
    at ? t("ontologyImport.atBy", { at: new Date(at).toLocaleString(), by: by ?? "—" }) : null;
  return (
    <DetailFields
      testId="ontology-import-summary"
      fields={[
        { label: t("ontologyImport.colFile"), value: i.file_name },
        { label: t("ontologyImport.format"), value: t(`ontologyImport.format_${i.format}`) },
        { label: t("ontologyImport.domains"), value: i.domains.join(", ") },
        { label: t("ontologyImport.imported"), value: when(i.imported_at, i.imported_by) },
        { label: t("ontologyImport.applied"), value: when(i.applied_at, i.applied_by) },
        {
          label: t("ontologyImport.read"),
          value: t("ontologyImport.readCounts", {
            items: i.summary.items,
            statements: i.summary.statements,
          }),
        },
        {
          label: t("ontologyImport.leftOut"),
          testId: "ontology-import-left-out",
          value: leftOut.length ? (
            <Stack gap={2}>
              {leftOut.map(([why, n]) => (
                <Text size="sm" key={why}>
                  {t("ontologyImport.leftOutLine", { count: n, why })}
                </Text>
              ))}
            </Stack>
          ) : (
            t("ontologyImport.leftOutNone")
          ),
        },
      ]}
    />
  );
}

export function ImportReviewPanel({
  importId,
  terms,
  onChanged,
  onDiscarded,
}: {
  importId: number;
  /** The glossary terms the steward may read, for an alignment of their own. */
  terms: GlossaryTermSummary[];
  /** Something was decided or applied: the imports list is read again for its counts. */
  onChanged: () => void;
  onDiscarded: () => Promise<void>;
}) {
  const { t } = useTranslation();
  const [review, setReview] = useState<ImportReview | null>(null);
  const [error, setError] = useState("");
  const [kind, setKind] = useState<ItemKind>("class");
  const [query, setQuery] = useState("");
  const [undecidedOnly, setUndecidedOnly] = useState(false);
  const [page, setPage] = useState(1);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [useDefinition, setUseDefinition] = useState<Set<number>>(new Set());
  const [editing, setEditing] = useState<ImportItem | null>(null);
  const [aligning, setAligning] = useState<ImportItem | null>(null);
  const [applying, setApplying] = useState(false);

  const load = useCallback(async () => {
    setReview(await fetchOntologyImport(importId));
  }, [importId]);

  useEffect(() => {
    let live = true;
    void fetchOntologyImport(importId)
      .then((r) => {
        if (live) setReview(r);
      })
      .catch((e: unknown) => {
        if (live) setError(failure(e));
      });
    return () => {
      live = false;
    };
  }, [importId]);

  // Runs a decision, then reads the review (and the list's counts) again.
  const act = async (fn: () => Promise<unknown>) => {
    setError("");
    try {
      await fn();
      await load();
      onChanged();
    } catch (e) {
      setError(failure(e));
    }
  };

  const byKind = useMemo(() => {
    const counts = Object.fromEntries(ITEM_KINDS.map((k) => [k, 0])) as Record<ItemKind, number>;
    for (const item of review?.items ?? []) counts[item.kind] += 1;
    return counts;
  }, [review]);

  const shown = useMemo(() => {
    const words = query.trim().toLowerCase();
    return (review?.items ?? []).filter(
      (item) =>
        item.kind === kind &&
        (!words ||
          item.name.toLowerCase().includes(words) ||
          (item.iri ?? "").toLowerCase().includes(words)) &&
        (!undecidedOnly || item.alignments.some((a) => a.status === "proposed")),
    );
  }, [review, kind, query, undecidedOnly]);

  if (review === null)
    return error ? (
      <Alert color="red" data-testid="ontology-review-error">
        {error}
      </Alert>
    ) : (
      <Group justify="center" py="md">
        <Loader size="xs" />
      </Group>
    );

  const pages = Math.max(1, Math.ceil(shown.length / PAGE_SIZE));
  const onPage = shown.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);
  // The proposals shown that can be approved together: undecided, on a pending item.
  const selectable = onPage.flatMap((item) =>
    item.status === "pending"
      ? item.alignments.filter((a) => a.status === "proposed").map((a) => a.id)
      : [],
  );
  const toggle = (set: Set<number>, id: number, on: boolean) => {
    const next = new Set(set);
    if (on) next.add(id);
    else next.delete(id);
    return next;
  };

  const runBulk = () =>
    act(async () => {
      const { approved, refused } = await bulkApprove(importId, [...selected]);
      setSelected(new Set());
      notifications.show({
        color: refused.length ? "orange" : "green",
        message: t("ontologyImport.bulkApproved", { count: approved.length }),
      });
      if (refused.length)
        setError(refused.map((r) => t("ontologyImport.bulkRefused", r)).join("\n"));
    });

  return (
    <Stack gap="md" pb="md" data-testid="ontology-review">
      <PanelActions>
        <Group gap="xs" data-toolbar>
          <Button
            leftSection={<Upload size={14} />}
            onClick={() => setApplying(true)}
            data-testid="ontology-apply-btn"
          >
            {t("ontologyImport.apply")}
          </Button>
          <PanelDelete
            itemKey={importId}
            text={t("ontologyImport.discard")}
            ariaLabel={t("ontologyImport.discard")}
            title={t("ontologyImport.discardTitle", { title: review.import.title })}
            consequence={t("ontologyImport.discardConsequence")}
            testId="ontology-discard-btn"
            onDelete={async () => {
              await discardOntologyImport(importId);
              await onDiscarded();
            }}
          />
        </Group>
      </PanelActions>

      {error && (
        <Alert color="red" data-testid="ontology-review-error" style={{ whiteSpace: "pre-line" }}>
          {error}
        </Alert>
      )}

      <ImportSummary review={review} />

      <SegmentedControl
        value={kind}
        onChange={(v) => {
          setKind(v as ItemKind);
          setPage(1);
        }}
        data={ITEM_KINDS.map((k) => ({
          value: k,
          label: t("ontologyImport.kindCount", {
            kind: t(`ontologyImport.kind_${k}`),
            count: byKind[k],
          }),
        }))}
        data-testid="ontology-review-kinds"
      />
      <Group gap="sm" wrap="wrap">
        <FilterInput
          value={query}
          onChange={(v) => {
            setQuery(v);
            setPage(1);
          }}
          placeholder={t("ontologyImport.filterItems")}
          testId="ontology-review-filter"
          remember={false}
        />
        <Checkbox
          size="xs"
          label={t("ontologyImport.undecidedOnly")}
          checked={undecidedOnly}
          onChange={(e) => {
            setUndecidedOnly(e.currentTarget.checked);
            setPage(1);
          }}
          data-testid="ontology-review-undecided"
        />
        <Group gap="xs" data-toolbar>
          <Button
            variant="default"
            size="xs"
            disabled={selectable.length === 0}
            onClick={() => setSelected(new Set([...selected, ...selectable]))}
            data-testid="ontology-select-shown"
          >
            {t("ontologyImport.selectShown")}
          </Button>
          <Button
            variant="light"
            color="teal"
            size="xs"
            leftSection={<CheckCheck size={14} />}
            disabled={selected.size === 0}
            onClick={() => void runBulk()}
            data-testid="ontology-bulk-approve"
          >
            {t("ontologyImport.bulkApprove", { count: selected.size })}
          </Button>
        </Group>
      </Group>

      {onPage.length === 0 ? (
        <Text size="sm" c="dimmed" data-testid="ontology-review-empty">
          {t("ontologyImport.noItems")}
        </Text>
      ) : (
        onPage.map((item) => {
          const pending = item.status === "pending";
          return (
            <Paper
              key={item.id}
              withBorder
              p="sm"
              data-testid={`ontology-item-${item.id}`}
              data-status={item.status}
            >
              <Stack gap={6}>
                <Group justify="space-between" wrap="wrap" gap="xs">
                  <Group gap={6} wrap="wrap">
                    <Text fw={600} size="sm">
                      {item.name}
                    </Text>
                    <Badge size="xs" variant="dot" color={ITEM_STATUS_COLOUR[item.status]}>
                      {t(`ontologyImport.itemStatus_${item.status}`)}
                    </Badge>
                    {item.status === "applied" && item.term && "name" in item.term && (
                      <Text size="xs" c="dimmed">
                        {t("ontologyImport.appliedAs", { name: item.term.name })}
                      </Text>
                    )}
                  </Group>
                  {item.status !== "applied" && (
                    <Group gap={4} data-toolbar>
                      {pending && (
                        <Button
                          size="compact-xs"
                          variant="subtle"
                          leftSection={<Pencil size={12} />}
                          onClick={() => setEditing(item)}
                          data-testid={`ontology-item-edit-${item.id}`}
                        >
                          {t("ontologyImport.edit")}
                        </Button>
                      )}
                      {pending && (
                        <Button
                          size="compact-xs"
                          variant="subtle"
                          leftSection={<Plus size={12} />}
                          onClick={() => setAligning(item)}
                          data-testid={`ontology-item-align-${item.id}`}
                        >
                          {t("ontologyImport.addAlignment")}
                        </Button>
                      )}
                      <Button
                        size="compact-xs"
                        variant="subtle"
                        color={pending ? "orange" : "teal"}
                        leftSection={pending ? <Ban size={12} /> : <RotateCcw size={12} />}
                        onClick={() =>
                          void act(() => editImportItem(item.id, { excluded: pending }))
                        }
                        data-testid={`ontology-item-exclude-${item.id}`}
                      >
                        {pending ? t("ontologyImport.exclude") : t("ontologyImport.include")}
                      </Button>
                    </Group>
                  )}
                </Group>
                {item.iri && (
                  <Text size="xs" ff="monospace" c="dimmed" style={{ wordBreak: "break-all" }}>
                    {item.iri}
                  </Text>
                )}
                <Text size="sm" c={item.definition ? undefined : "dimmed"}>
                  {item.definition ?? t("ontologyImport.noDefinition")}
                </Text>
                {item.alignments.length === 0 ? (
                  <Text size="xs" c="dimmed">
                    {t("ontologyImport.noAlignments")}
                  </Text>
                ) : (
                  <Stack gap={8} pl="xs" style={{ borderInlineStart: "2px solid var(--border)" }}>
                    {item.alignments.map((a) => (
                      <AlignmentRow
                        key={a.id}
                        item={item}
                        alignment={a}
                        canDecide={pending && a.status !== "written"}
                        selected={selected.has(a.id)}
                        onSelect={(on) => setSelected(toggle(selected, a.id, on))}
                        useDefinition={useDefinition.has(a.id)}
                        onUseDefinition={(on) => setUseDefinition(toggle(useDefinition, a.id, on))}
                        onApprove={() =>
                          void act(async () => {
                            await approveAlignment(a.id, useDefinition.has(a.id));
                            setSelected(toggle(selected, a.id, false));
                          })
                        }
                        onReject={() =>
                          void act(async () => {
                            await rejectAlignment(a.id);
                            setSelected(toggle(selected, a.id, false));
                          })
                        }
                      />
                    ))}
                  </Stack>
                )}
              </Stack>
            </Paper>
          );
        })
      )}
      {pages > 1 && (
        <Pagination
          value={page}
          onChange={setPage}
          total={pages}
          size="sm"
          data-testid="ontology-review-pages"
        />
      )}

      {editing && (
        <EditItemDialog
          item={editing}
          onClose={() => setEditing(null)}
          onSaved={(r) => {
            setEditing(null);
            setReview(r);
            onChanged();
          }}
        />
      )}
      {aligning && (
        <AddAlignmentDialog
          importId={importId}
          item={aligning}
          items={review.items}
          terms={terms}
          onClose={() => setAligning(null)}
          onAdded={() => {
            setAligning(null);
            void act(async () => undefined);
          }}
        />
      )}
      {applying && (
        <ApplyImportDialog
          review={review}
          onClose={() => setApplying(false)}
          onApplied={(result) => {
            setApplying(false);
            // The glossary page reads its terms again: the import made and aligned some.
            window.dispatchEvent(new Event("provisa:glossary-changed"));
            notifications.show({
              color: "green",
              message: t("ontologyImport.appliedDone", {
                created: result.created.length,
                aligned: result.aligned.length,
                edges: result.edges.length,
              }),
            });
            void act(async () => undefined);
          }}
        />
      )}
    </Stack>
  );
}
