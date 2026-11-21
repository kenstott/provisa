// Copyright (c) 2026 Kenneth Stott
// Canary: 3c9e7b52-1a64-4d08-8f3b-6d2a0e9c7f41
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2266: access reviews. A review lists each member's grant with its entitlements and last use,
// grouped by the domain whose owner confirms or revokes it; each decision keeps who, when and why.
// Taken on demand here or on the org's schedule, set here too; exported as CSV.

import { DestructiveConfirmButton } from "../form/DestructiveConfirmButton";
import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Button, Group, Select, Stack, Table, Text, TextInput } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Download, Play } from "lucide-react";
import {
  SCHEDULE_STARTER,
  accessReviewExportUrl,
  decideAccessReviewRow,
  fetchAccessReview,
  fetchAccessReviewSchedule,
  fetchAccessReviews,
  saveAccessReviewSchedule,
  startAccessReview,
  type AccessReviewRow,
  type AccessReviewSummary,
  type ReviewDecision,
} from "../../api/accessReview";
import { FieldGroup, FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { ListEmpty, ListHead, ListItems, ListRow, ListTable } from "../list/ListTable";
import { useListSortGroup, type ListColumn } from "../list/useListSortGroup";
import { useCapability } from "../../hooks/useCapability";

function when(iso: string | null): string {
  return iso ? new Date(iso).toLocaleString() : "—";
}

function member(r: AccessReviewRow): string {
  return r.display_name ?? r.email ?? r.user_id;
}

export function AccessReviews() {
  const { t } = useTranslation();
  const canStart = useCapability("user_management");
  const canSchedule = useCapability("org_settings");
  const [reviews, setReviews] = useState<AccessReviewSummary[]>([]);
  const [reviewId, setReviewId] = useState<number | null>(null);
  const [rows, setRows] = useState<AccessReviewRow[]>([]);
  const [deciding, setDeciding] = useState<{
    row: AccessReviewRow;
    decision: ReviewDecision;
  } | null>(null);
  const [reason, setReason] = useState("");
  const [cron, setCron] = useState("");
  const [error, setError] = useState<string | null>(null);

  const report = useCallback(
    (e: unknown) => setError(e instanceof Error ? e.message : String(e)),
    [],
  );

  const loadReviews = useCallback(
    (select?: number) =>
      fetchAccessReviews()
        .then((found) => {
          setReviews(found);
          setReviewId((current) => select ?? current ?? found[0]?.id ?? null);
        })
        .catch(report),
    [report],
  );

  const loadRows = useCallback(
    (id: number) =>
      fetchAccessReview(id)
        .then((r) => setRows(r.rows))
        .catch(report),
    [report],
  );

  useEffect(() => {
    loadReviews();
  }, [loadReviews]);

  useEffect(() => {
    // A review once chosen stays chosen (the select allows no deselect), so null is only the
    // state before any review exists, when there are no rows to show.
    if (reviewId !== null) loadRows(reviewId);
  }, [reviewId, loadRows]);

  useEffect(() => {
    if (!canSchedule) return;
    fetchAccessReviewSchedule()
      .then((s) => setCron(s.cron ?? ""))
      .catch(report);
  }, [canSchedule, report]);

  const start = async () => {
    try {
      const started = await startAccessReview();
      notifications.show({
        color: "green",
        message: t("accessReview.started", { count: started.rows }),
      });
      setError(null);
      loadReviews(started.id);
    } catch (e) {
      report(e);
    }
  };

  const saveSchedule = async () => {
    try {
      const saved = await saveAccessReviewSchedule(cron.trim() === "" ? null : cron.trim());
      setCron(saved.cron ?? "");
      setError(null);
      notifications.show({ color: "green", message: t("accessReview.scheduleSaved") });
    } catch (e) {
      report(e);
    }
  };

  const decide = async () => {
    if (deciding === null) return;
    try {
      await decideAccessReviewRow(deciding.row.id, deciding.decision, reason.trim());
      setDeciding(null);
      setReason("");
      setError(null);
      if (reviewId !== null) loadRows(reviewId);
      loadReviews();
    } catch (e) {
      report(e);
    }
  };

  const columns: ListColumn<AccessReviewRow>[] = [
    {
      key: "domain",
      label: t("accessReview.colDomain"),
      sortValue: (r) => r.domain_id,
      groupValue: (r) => (r.domain_id === "*" ? t("accessReview.everyDomain") : r.domain_id),
    },
    { key: "member", label: t("accessReview.colMember"), sortValue: member },
    {
      key: "role",
      label: t("accessReview.colRole"),
      sortValue: (r) => r.role_id,
      groupValue: (r) => r.role_id,
    },
    {
      key: "entitlements",
      label: t("accessReview.colEntitlements"),
      sortValue: (r) => r.entitlements.flatMap((e) => e.identifiers).join(", "),
    },
    {
      key: "lastUsed",
      label: t("accessReview.colLastUsed"),
      sortValue: (r) => r.last_used_at ?? "",
    },
    {
      key: "decision",
      label: t("accessReview.colDecision"),
      sortValue: (r) => r.decision ?? "",
      groupValue: (r) =>
        r.decision ? t(`accessReview.decision.${r.decision}`) : t("accessReview.undecided"),
    },
  ];
  const sortGroup = useListSortGroup(rows, columns, "access-review", null, ["domain"]);

  const reviewOptions = reviews.map((r) => ({
    value: String(r.id),
    label: t("accessReview.reviewOption", {
      when: when(r.started_at),
      by: r.started_by === SCHEDULE_STARTER ? t("accessReview.bySchedule") : r.started_by,
      decided: r.decided,
      rows: r.rows,
    }),
  }));

  return (
    <Stack gap="sm" data-testid="access-reviews">
      <Text size="sm" c="dimmed">
        {t("accessReview.help")}
      </Text>
      {error && (
        <Alert
          color="red"
          withCloseButton
          onClose={() => setError(null)}
          data-testid="access-review-error"
        >
          {error}
        </Alert>
      )}

      {canSchedule && (
        <FormLayout data-testid="access-review-schedule">
          <FormField size="medium" hint={t("accessReview.scheduleHint")}>
            <TextInput
              label={t("accessReview.scheduleLabel")}
              placeholder="0 6 1 * *"
              value={cron}
              onChange={(e) => setCron(e.currentTarget.value)}
              data-testid="access-review-schedule-cron"
            />
          </FormField>
          <FormFooter
            onSave={saveSchedule}
            saveProps={{ "data-testid": "access-review-schedule-save" }}
          />
        </FormLayout>
      )}

      <FormLayout data-testid="access-review-picker">
        <FieldGroup>
          <FormField size="long">
            <Select
              label={t("accessReview.reviewLabel")}
              placeholder={t("accessReview.noReviews")}
              data={reviewOptions}
              value={reviewId === null ? null : String(reviewId)}
              onChange={(v) => setReviewId(v === null ? null : Number(v))}
              allowDeselect={false}
              data-testid="access-review-select"
            />
          </FormField>
          {(canStart || reviewId !== null) && (
            <FormField size="medium" afterLabel>
              <Group gap="sm" wrap="nowrap">
                {canStart && (
                  <Button
                    variant="default"
                    leftSection={<Play size={14} />}
                    onClick={start}
                    data-testid="access-review-start"
                  >
                    {t("accessReview.start")}
                  </Button>
                )}
                {canStart && reviewId !== null && (
                  <Button
                    component="a"
                    variant="default"
                    href={accessReviewExportUrl(reviewId)}
                    download
                    leftSection={<Download size={14} />}
                    data-testid="access-review-export"
                  >
                    {t("accessReview.export")}
                  </Button>
                )}
              </Group>
            </FormField>
          )}
        </FieldGroup>
      </FormLayout>

      {deciding !== null && (
        <FormLayout data-testid="access-review-decide">
          <Text size="sm">
            {t(`accessReview.prompt.${deciding.decision}`, {
              member: member(deciding.row),
              role: deciding.row.role_id,
              domain: deciding.row.domain_id,
            })}
          </Text>
          <FormField size="long" hint={t("accessReview.reasonHint")}>
            <TextInput
              label={t("accessReview.reasonLabel")}
              required
              value={reason}
              onChange={(e) => setReason(e.currentTarget.value)}
              data-testid="access-review-reason"
            />
          </FormField>
          <FormFooter
            onCancel={() => setDeciding(null)}
            cancelProps={{ "data-testid": "access-review-decide-cancel" }}
            action={
              deciding.decision === "revoked" ? (
                <DestructiveConfirmButton
                  disabled={reason.trim() === ""}
                  onClick={decide}
                  testId="access-review-decide-submit"
                >
                  {t("accessReview.revoke")}
                </DestructiveConfirmButton>
              ) : (
                <Button
                  disabled={reason.trim() === ""}
                  onClick={decide}
                  data-testid="access-review-decide-submit"
                >
                  {t("accessReview.confirm")}
                </Button>
              )
            }
          />
        </FormLayout>
      )}

      <div className="card-table-scroll">
        <ListTable minWidth={760} testId="access-review-rows">
          <ListHead
            sortGroup={sortGroup}
            columns={[
              { col: "domain" },
              { col: "member" },
              { col: "role" },
              { col: "entitlements" },
              { col: "lastUsed" },
              { col: "decision" },
              t("accessReview.colActions"),
            ]}
          />
          <Table.Tbody>
            {rows.length === 0 && (
              <ListEmpty colSpan={7}>
                {reviewId === null ? t("accessReview.noReviews") : t("accessReview.noRows")}
              </ListEmpty>
            )}
            <ListItems
              state={sortGroup}
              colSpan={7}
              rowKey={(r) => String(r.id)}
              render={(r) => (
                <ListRow testId={`access-review-row-${r.id}`}>
                  <Table.Td>
                    {r.domain_id === "*" ? t("accessReview.everyDomain") : r.domain_id}
                  </Table.Td>
                  <Table.Td>
                    <Text size="sm">{member(r)}</Text>
                    {member(r) !== r.user_id && (
                      <Text size="xs" c="dimmed">
                        {r.user_id}
                      </Text>
                    )}
                  </Table.Td>
                  <Table.Td>{r.role_id}</Table.Td>
                  <Table.Td>
                    <Group gap={4}>
                      {r.entitlements.length === 0 && (
                        <Text size="sm" c="dimmed">
                          {t("accessReview.wholeRole")}
                        </Text>
                      )}
                      {r.entitlements.flatMap((e) =>
                        e.identifiers.map((i) => (
                          <Badge
                            key={`${e.kind}|${e.capability ?? ""}|${i}`}
                            variant="light"
                            tt="none"
                            ff="monospace"
                          >
                            {i}
                          </Badge>
                        )),
                      )}
                    </Group>
                  </Table.Td>
                  <Table.Td>
                    {r.last_used_at ? when(r.last_used_at) : t("accessReview.neverUsed")}
                  </Table.Td>
                  <Table.Td>
                    {r.decision === null ? (
                      <Text size="sm" c="dimmed">
                        {t("accessReview.undecided")}
                      </Text>
                    ) : (
                      <Stack gap={0}>
                        <Badge color={r.decision === "revoked" ? "red" : "green"} variant="light">
                          {t(`accessReview.decision.${r.decision}`)}
                        </Badge>
                        <Text size="xs" c="dimmed">
                          {t("accessReview.decidedBy", {
                            who: r.decided_by,
                            when: when(r.decided_at),
                          })}
                        </Text>
                        <Text size="xs">{r.reason}</Text>
                      </Stack>
                    )}
                  </Table.Td>
                  <Table.Td>
                    {r.can_decide && (
                      <Group gap={4} wrap="nowrap">
                        <Button
                          size="compact-xs"
                          variant="light"
                          onClick={() => {
                            setReason("");
                            setDeciding({ row: r, decision: "confirmed" });
                          }}
                          data-testid={`access-review-confirm-${r.id}`}
                        >
                          {t("accessReview.confirm")}
                        </Button>
                        <Button
                          size="compact-xs"
                          color="red"
                          variant="light"
                          onClick={() => {
                            setReason("");
                            setDeciding({ row: r, decision: "revoked" });
                          }}
                          data-testid={`access-review-revoke-${r.id}`}
                        >
                          {t("accessReview.revoke")}
                        </Button>
                      </Group>
                    )}
                  </Table.Td>
                </ListRow>
              )}
            />
          </Table.Tbody>
        </ListTable>
      </div>
    </Stack>
  );
}
