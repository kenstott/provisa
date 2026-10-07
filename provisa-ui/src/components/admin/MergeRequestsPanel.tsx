// Copyright (c) 2026 Kenneth Stott
// Canary: 71ab0c93-2d64-4e18-95f7-8c30ea1b47d5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useEffect, useState } from "react";
import { Badge, Button, Checkbox, Group, Stack, Table, Text, Textarea, Title } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { useTranslation } from "react-i18next";
import { decideMergeRequest, fetchMergeRequests } from "../../api/environments";
import type { MergeRequest } from "../../api/environments";
import { ListRow } from "../list/ListTable";
import { SortGroupTable } from "../list/SortGroupTable";

const STATE_COLOR: Record<string, string> = {
  open: "blue",
  approved: "green",
  rejected: "red",
  stale: "orange",
};

/**
 * What a request is applying, as a person reads it (REQ-1496).
 *
 * A merge names the environment it comes from; a load names a branch, and the sha beside it because
 * that -- not the branch -- is what will be applied and what the report describes.
 */
function sourceOf(r: MergeRequest): string {
  if (r.source_env !== null) return r.source_env;
  return `${r.source_ref}@${(r.source_sha ?? "").slice(0, 7)}`;
}

/**
 * REQ-1504: the merges waiting on somebody else's decision.
 *
 * A request carries the report it was filed with — that report is what the approver is agreeing
 * to, so it is shown rather than recomputed. A request whose source has moved on since it was
 * filed comes back with a DERIVED state of `stale`: the server never stores staleness, so this
 * table is showing what the server computed at read time, not a column.
 */
export function MergeRequestsPanel({ orgId, canDecide }: { orgId: string; canDecide: boolean }) {
  const { t } = useTranslation();
  const [requests, setRequests] = useState<MergeRequest[]>([]);
  const [openOnly, setOpenOnly] = useState(true);
  const [busy, setBusy] = useState<number | null>(null);
  const [notes, setNotes] = useState<Record<number, string>>({});

  const reload = useCallback(() => {
    fetchMergeRequests(orgId, openOnly)
      .then(setRequests)
      .catch((err: Error) => notifications.show({ color: "red", message: err.message }));
  }, [orgId, openOnly]);

  useEffect(reload, [reload]);

  async function decide(id: number, approve: boolean) {
    setBusy(id);
    try {
      await decideMergeRequest(orgId, id, approve, notes[id]);
      notifications.show({
        color: approve ? "green" : "gray",
        message: t(approve ? "environmentsTab.requestApproved" : "environmentsTab.requestRejected"),
      });
      reload();
    } catch (err) {
      notifications.show({ color: "red", message: (err as Error).message });
    } finally {
      setBusy(null);
    }
  }

  return (
    <Stack gap="sm" data-testid="merge-requests">
      <Group justify="space-between">
        <Title order={4}>{t("environmentsTab.requestsTitle")}</Title>
        <Checkbox
          label={t("environmentsTab.openOnly")}
          checked={openOnly}
          onChange={(e) => setOpenOnly(e.currentTarget.checked)}
          data-testid="merge-requests-open-only"
        />
      </Group>
      {requests.length === 0 ? (
        <Text c="dimmed" data-testid="merge-requests-empty">
          {t("environmentsTab.noRequests")}
        </Text>
      ) : (
        <SortGroupTable
          testPrefix="merge-requests"
          rows={requests}
          columns={[
            { key: "merge", label: t("environmentsTab.colMerge"), sortValue: (r) => r.id },
            { key: "state", label: t("environmentsTab.colState"), sortValue: (r) => r.state, groupValue: (r) => r.state },
            { key: "requestedBy", label: t("environmentsTab.colRequestedBy"), sortValue: (r) => r.requested_by ?? "", groupValue: (r) => r.requested_by ?? "" },
          ]}
          headers={[
            { col: "merge" },
            { col: "state" },
            { col: "requestedBy" },
            t("environmentsTab.colReport"),
            t("environmentsTab.colActions"),
          ]}
          colSpan={5}
          rowKey={(r) => r.id}
          render={(r) => (
              <ListRow key={r.id} testId={`merge-request-${r.id}`}>
                <Table.Td>
                  <Text size="sm">
                    {sourceOf(r)} → {r.target_env}
                  </Text>
                  {r.message && (
                    <Text size="xs" c="dimmed">
                      {r.message}
                    </Text>
                  )}
                </Table.Td>
                <Table.Td>
                  <Badge color={STATE_COLOR[r.state]} data-testid={`merge-request-state-${r.id}`}>
                    {r.state}
                  </Badge>
                </Table.Td>
                <Table.Td>
                  <Text size="sm">{r.requested_by}</Text>
                  <Text size="xs" c="dimmed">
                    {r.requested_at}
                  </Text>
                </Table.Td>
                <Table.Td>
                  <Text size="sm">
                    {t("environmentsTab.reportCounts", {
                      added: r.report.added,
                      changed: r.report.changed,
                      removed: r.report.removed,
                    })}
                  </Text>
                </Table.Td>
                <Table.Td>
                  {canDecide && r.state === "open" ? (
                    <Stack gap="xs">
                      <Textarea
                        autosize
                        minRows={1}
                        placeholder={t("environmentsTab.notePlaceholder")}
                        value={notes[r.id] ?? ""}
                        onChange={(e) =>
                          setNotes((prev) => ({ ...prev, [r.id]: e.currentTarget.value }))
                        }
                        data-testid={`merge-request-note-${r.id}`}
                      />
                      <Group gap="xs">
                        <Button
                          size="compact-sm"
                          color="green"
                          loading={busy === r.id}
                          onClick={() => decide(r.id, true)}
                          data-testid={`merge-request-approve-${r.id}`}
                        >
                          {t("environmentsTab.approve")}
                        </Button>
                        <Button
                          size="compact-sm"
                          color="red"
                          variant="light"
                          loading={busy === r.id}
                          onClick={() => decide(r.id, false)}
                          data-testid={`merge-request-reject-${r.id}`}
                        >
                          {t("environmentsTab.reject")}
                        </Button>
                      </Group>
                    </Stack>
                  ) : (
                    <Text size="xs" c="dimmed">
                      {r.decided_by ? t("environmentsTab.decidedBy", { who: r.decided_by }) : "—"}
                    </Text>
                  )}
                </Table.Td>
              </ListRow>
            )}
        />
      )}
    </Stack>
  );
}
