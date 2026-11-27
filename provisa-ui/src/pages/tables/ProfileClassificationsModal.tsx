// Copyright (c) 2026 Kenneth Stott
// Canary: d68ecd87-942b-49ed-af75-44bcf6614137
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Button, Group, Loader, ScrollArea, Table, Text, Title } from "@mantine/core";
import { ModalLayout } from "../../components/modal/ModalLayout";
import {
  decideClassification,
  fetchClassificationKinds,
  fetchClassifications,
  fetchProfileRun,
  fetchProfileRuns,
  forgetClassification,
  type ClassificationDecision,
} from "../../api/profiler";
import { useAuth } from "../../context/AuthContext";
import { isCustomKind } from "../../lib/classificationKinds";

type Suggestion = Record<string, unknown>;

// A column whose classification sample held too few non-null values to classify, and how many.
type TooFew = { column: string; count: number };

// The columns whose classification sample held too few non-null values to classify, from the
// run's columns rows (each says what its classification sample said of it, and how many values it
// held). Counts only; no value is stored.
function tooFewOf(columns: Record<string, unknown>[] | undefined): TooFew[] {
  return (columns ?? [])
    .filter((c) => c.classification === "too_few_values")
    .map((c) => ({ column: String(c.column_name), count: Number(c.classification_values) }));
}

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

// REQ-2147: the classifications the latest run suggests -- a column whose sampled values passed a
// kind's test -- each accepted or rejected here by the table's steward. Accepting links the column to
// the kind's glossary term and, for a sensitive kind, marks it pii and offers the kind's mask; a
// rejected one is not suggested again for that column and kind.
export function ProfileClassificationsModal({
  tableId,
  tableName,
  onClose,
}: {
  tableId: number;
  tableName: string;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const { actingRoles } = useAuth();
  const [suggestions, setSuggestions] = useState<Suggestion[] | null>(null);
  const [tooFew, setTooFew] = useState<TooFew[]>([]);
  const [runId, setRunId] = useState<string | null>(null);
  const [decisions, setDecisions] = useState<ClassificationDecision[]>([]);
  const [notice, setNotice] = useState<{ ok: boolean; text: string } | null>(null);

  // The org's custom kinds by id, with their labels. A run's suggestion of a custom kind the org
  // has since deleted is no suggestion: the kind is gone from the model, with its decisions.
  const [customKinds, setCustomKinds] = useState<Map<string, string> | null>(null);
  useEffect(() => {
    fetchClassificationKinds()
      .then((r) =>
        setCustomKinds(new Map(r.kinds.filter((k) => k.custom).map((k) => [k.id, k.label]))),
      )
      .catch((e: unknown) => setNotice({ ok: false, text: message(e) }));
  }, []);
  const listed = (kind: unknown) =>
    !isCustomKind(String(kind)) || customKinds?.has(String(kind)) === true;
  const shown =
    customKinds === null || suggestions === null ? null : suggestions.filter((s) => listed(s.kind));

  const reload = useCallback(
    () =>
      fetchClassifications(tableId)
        .then((r) => setDecisions(r.decisions))
        .catch((e: unknown) => setNotice({ ok: false, text: message(e) })),
    [tableId],
  );

  useEffect(() => {
    void reload();
  }, [reload]);

  useEffect(() => {
    if (!actingRoles) return;
    fetchProfileRuns(tableId)
      .then(async (runs) => {
        const latest = runs.find((r) => r.status === "succeeded");
        if (!latest) {
          setSuggestions([]);
          return;
        }
        const results = await fetchProfileRun(tableId, latest.run_id, actingRoles);
        setRunId(latest.run_id);
        setSuggestions(results.classifications ?? []);
        setTooFew(tooFewOf(results.columns));
      })
      .catch((e: unknown) => setNotice({ ok: false, text: message(e) }));
  }, [tableId, actingRoles]);

  // REQ-2147 (amended 2026-11-25): a custom kind is named by the org's label, a shipped one by its
  // translated words.
  const customLabel = (kind: unknown) => {
    const label = customKinds?.get(String(kind));
    if (label === undefined) throw new Error(`classification kind ${String(kind)} is not listed`);
    return label;
  };
  const kindLabel = (kind: unknown) =>
    isCustomKind(String(kind)) ? customLabel(kind) : t(`classificationKinds.kind.${String(kind)}`);
  const kindNoun = (kind: unknown) =>
    isCustomKind(String(kind)) ? customLabel(kind) : t(`classificationKinds.noun.${String(kind)}`);

  const sentence = (s: Suggestion) =>
    t("profileClassificationsModal.suggestion", {
      table: tableName,
      column: String(s.column_name),
      kind: kindNoun(s.kind),
      share: `${Math.round((Number(s.share) || 0) * 100)}%`,
    });

  // The decision on a suggestion, by its column's registered name and its kind.
  const decidedAs = (s: Suggestion) =>
    decisions.find((d) => d.physical_column === s.physical_column && d.kind === s.kind)?.status ??
    null;

  const decide = async (s: Suggestion, status: "accepted" | "rejected") => {
    if (runId == null) return;
    try {
      const r = await decideClassification(tableId, {
        column: String(s.column_name),
        kind: String(s.kind),
        status,
        evidence: String(s.evidence),
        runId,
      });
      if (status === "accepted") {
        const parts = [
          r.term
            ? t("profileClassificationsModal.linked", { term: r.term })
            : t("profileClassificationsModal.keyColumn"),
        ];
        if (r.pii) parts.push(t("profileClassificationsModal.markedPii"));
        if (r.maskPreset)
          parts.push(
            t("profileClassificationsModal.maskOffered", {
              mask: t(`maskList.preset.${r.maskPreset}`),
            }),
          );
        setNotice({ ok: true, text: parts.join(" ") });
      } else {
        setNotice(null);
      }
      await reload();
    } catch (e: unknown) {
      setNotice({ ok: false, text: message(e) });
    }
  };

  const withdraw = async (d: ClassificationDecision) => {
    try {
      await forgetClassification(tableId, d.physical_column, d.kind);
      await reload();
    } catch (e: unknown) {
      setNotice({ ok: false, text: message(e) });
    }
  };

  return (
    <ModalLayout
      opened
      onClose={onClose}
      size="90%"
      title={t("profileClassificationsModal.title", { table: tableName })}
      data-testid="profile-classifications-modal"
    >
      {notice && (
        <Alert color={notice.ok ? "green" : "red"} mb="xs" data-testid="classifications-notice">
          {notice.text}
        </Alert>
      )}
      <Title order={6}>{t("profileClassificationsModal.suggested")}</Title>
      {shown == null && <Loader size="sm" />}
      {shown != null && shown.length === 0 && (
        <Text size="sm" c="dimmed" data-testid="classifications-none">
          {t("profileClassificationsModal.none")}
        </Text>
      )}
      {shown != null && shown.length > 0 && (
        <ScrollArea.Autosize mah={320}>
          <Table withTableBorder fz="xs" data-testid="classifications-suggested">
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("profileClassificationsModal.column")}</Table.Th>
                <Table.Th>{t("profileClassificationsModal.kindHeader")}</Table.Th>
                <Table.Th>{t("profileClassificationsModal.evidence")}</Table.Th>
                <Table.Th />
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {shown.map((s, i) => {
                const status = decidedAs(s);
                return (
                  <Table.Tr key={i} data-testid={`classification-suggestion-${i}`}>
                    <Table.Td>{String(s.column_name)}</Table.Td>
                    <Table.Td>
                      {kindLabel(s.kind)}
                      {s.sensitive === true && (
                        <Badge ml="xs" size="xs" color="red" variant="light">
                          {t("profileClassificationsModal.sensitive")}
                        </Badge>
                      )}
                    </Table.Td>
                    <Table.Td>{sentence(s)}</Table.Td>
                    <Table.Td>
                      {status != null ? (
                        <Badge size="xs" data-testid={`classification-status-${i}`}>
                          {t(`profileClassificationsModal.status_${status}`)}
                        </Badge>
                      ) : (
                        <Group gap={4} wrap="nowrap">
                          <Button
                            size="compact-xs"
                            onClick={() => decide(s, "accepted")}
                            data-testid={`classification-accept-${i}`}
                          >
                            {t("profileClassificationsModal.accept")}
                          </Button>
                          <Button
                            size="compact-xs"
                            variant="default"
                            onClick={() => decide(s, "rejected")}
                            data-testid={`classification-reject-${i}`}
                          >
                            {t("profileClassificationsModal.reject")}
                          </Button>
                        </Group>
                      )}
                    </Table.Td>
                  </Table.Tr>
                );
              })}
            </Table.Tbody>
          </Table>
        </ScrollArea.Autosize>
      )}
      {tooFew.length > 0 && (
        <>
          <Title order={6} mt="md">
            {t("profileClassificationsModal.tooFewTitle")}
          </Title>
          <Text size="xs" c="dimmed">
            {t("profileClassificationsModal.tooFewNote")}
          </Text>
          <Table withTableBorder fz="xs" mt="xs" data-testid="classifications-too-few">
            <Table.Tbody>
              {tooFew.map((f) => (
                <Table.Tr key={f.column} data-testid={`classification-too-few-${f.column}`}>
                  <Table.Td>
                    {t("profileClassificationsModal.tooFew", {
                      column: f.column,
                      count: f.count,
                    })}
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </>
      )}
      <Title order={6} mt="md">
        {t("profileClassificationsModal.decided")}
      </Title>
      {customKinds === null ? (
        <Loader size="sm" />
      ) : decisions.length === 0 ? (
        <Text size="sm" c="dimmed" data-testid="classifications-none-decided">
          {t("profileClassificationsModal.noneDecided")}
        </Text>
      ) : (
        <Table withTableBorder fz="xs" mt="xs" data-testid="classifications-decided">
          <Table.Tbody>
            {decisions.map((d) => (
              <Table.Tr
                key={`${d.physical_column}|${d.kind}`}
                data-testid={`classification-decided-${d.physical_column}-${d.kind}`}
              >
                <Table.Td>{d.physical_column}</Table.Td>
                <Table.Td>{kindLabel(d.kind)}</Table.Td>
                <Table.Td>
                  <Badge size="xs">{t(`profileClassificationsModal.status_${d.status}`)}</Badge>
                </Table.Td>
                <Table.Td>
                  <Button
                    size="compact-xs"
                    variant="default"
                    onClick={() => withdraw(d)}
                    data-testid={`classification-withdraw-${d.physical_column}-${d.kind}`}
                  >
                    {t("profileClassificationsModal.withdraw")}
                  </Button>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </ModalLayout>
  );
}
