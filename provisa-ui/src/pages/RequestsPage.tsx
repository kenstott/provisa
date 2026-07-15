// Copyright (c) 2026 Kenneth Stott
// Canary: c543c04b-a6b6-4082-beef-b38df177f30a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Group,
  Modal,
  Pagination,
  Select,
  Stack,
  Table,
  Tabs,
  Text,
  Title,
} from "@mantine/core";
import { FilterInput } from "../components/admin/FilterInput";

const API_BASE = import.meta.env.VITE_API_BASE || "";

const PAGE_SIZE = 50;

interface CreationRequest {
  id: number;
  request_type: string;
  capability: string;
  payload: Record<string, unknown>;
  requested_by: string | null;
  status: string;
  rejection_reason: string | null;
  resolved_by: string | null;
  created_at: string;
  resolved_at: string | null;
  approvals: { approver: string; approved_at: string }[];
  required_approvals: number;
}

async function apiFetch(path: string, opts?: RequestInit) {
  const resp = await fetch(`${API_BASE}${path}`, opts);
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(body.detail || resp.statusText);
  }
  return resp.json();
}

async function fetchRequests(status?: string): Promise<CreationRequest[]> {
  const qs = status ? `?status=${encodeURIComponent(status)}` : "";
  return apiFetch(`/admin/creation-requests${qs}`);
}

async function fetchRejectionReasons(): Promise<Record<string, string[]>> {
  return apiFetch("/admin/creation-requests/rejection-reasons");
}

async function apiApprove(id: number): Promise<CreationRequest> {
  return apiFetch(`/admin/creation-requests/${id}/approve`, { method: "POST" });
}

async function apiExecute(id: number): Promise<{ status: string }> {
  return apiFetch(`/admin/creation-requests/${id}/execute`, { method: "POST" });
}

async function apiReject(id: number, reason: string): Promise<{ status: string }> {
  return apiFetch(`/admin/creation-requests/${id}/reject`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ reason }),
  });
}

export function RequestsPage() {
  const { t } = useTranslation();
  const [tab, setTab] = useState<"pending" | "resolved">("pending");
  const [rows, setRows] = useState<CreationRequest[]>([]);
  const [reasons, setReasons] = useState<Record<string, string[]>>({});
  const [error, setError] = useState<string | null>(null);
  const [rejectingId, setRejectingId] = useState<number | null>(null);
  const [rejectReason, setRejectReason] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);

  const REASON_LABELS: Record<string, string> = {
    duplicate: t("requestsPage.reasonDuplicate"),
    incorrect_join_columns: t("requestsPage.reasonIncorrectJoinColumns"),
    wrong_cardinality: t("requestsPage.reasonWrongCardinality"),
    source_not_registered: t("requestsPage.reasonSourceNotRegistered"),
    insufficient_detail: t("requestsPage.reasonInsufficientDetail"),
    query_invalid: t("requestsPage.reasonQueryInvalid"),
    governance_violation: t("requestsPage.reasonGovernanceViolation"),
    out_of_scope: t("requestsPage.reasonOutOfScope"),
    endpoint_unreachable: t("requestsPage.reasonEndpointUnreachable"),
    schema_mismatch: t("requestsPage.reasonSchemaMismatch"),
  };

  const load = (status: "pending" | "resolved") => {
    fetchRequests(status === "pending" ? "pending" : undefined)
      .then((data) =>
        status === "resolved" ? setRows(data.filter((r) => r.status !== "pending")) : setRows(data),
      )
      .catch((e) => setError(String(e)));
  };

  useEffect(() => {
    load(tab);
    fetchRejectionReasons().then(setReasons).catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab]);

  const doApprove = async (id: number) => {
    setBusy(true);
    setError(null);
    try {
      await apiApprove(id);
      load(tab);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };

  const doExecute = async (id: number) => {
    setBusy(true);
    setError(null);
    try {
      await apiExecute(id);
      load(tab);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };

  const doReject = async () => {
    if (rejectingId === null || !rejectReason) return;
    setBusy(true);
    setError(null);
    try {
      await apiReject(rejectingId, rejectReason);
      setRejectingId(null);
      setRejectReason(null);
      load(tab);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };

  const displayRows =
    tab === "pending"
      ? rows.filter((r) => r.status === "pending")
      : rows.filter((r) => r.status !== "pending");

  const q = search.toLowerCase();
  const filtered = displayRows.filter(
    (r) =>
      String(r.id).includes(q) ||
      r.request_type.toLowerCase().includes(q) ||
      (r.requested_by ?? "").toLowerCase().includes(q) ||
      r.status.toLowerCase().includes(q),
  );
  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safePage = Math.min(page, totalPages);
  const paged = filtered.slice((safePage - 1) * PAGE_SIZE, safePage * PAGE_SIZE);

  const rejectingRequest = rows.find((r) => r.id === rejectingId);
  const reasonOptions = (reasons[rejectingRequest?.request_type ?? ""] ?? []).map((r) => ({
    value: r,
    label: REASON_LABELS[r] ?? r,
  }));

  const statusColor = (status: string) =>
    status === "pending" ? "yellow" : status === "approved" ? "green" : "gray";

  return (
    <Stack gap="md" className="page">
      <Group justify="space-between" wrap="wrap" align="center">
        <Title order={2}>{t("requestsPage.title")}</Title>
        <FilterInput
          value={search}
          onChange={(v) => { setSearch(v); setPage(1); }}
          placeholder={t("requestsPage.filterPlaceholder")}
        />
        <Tabs
          value={tab}
          onChange={(v) => { setTab((v as "pending" | "resolved") ?? "pending"); setPage(1); }}
        >
          <Tabs.List>
            <Tabs.Tab value="pending" data-testid="requests-tab-pending">
              {t("requestsPage.tabPending")}
            </Tabs.Tab>
            <Tabs.Tab value="resolved" data-testid="requests-tab-resolved">
              {t("requestsPage.tabResolved")}
            </Tabs.Tab>
          </Tabs.List>
        </Tabs>
      </Group>

      {error && (
        <Alert color="red" variant="light" data-testid="requests-error">
          {error}
        </Alert>
      )}

      <Modal
        opened={rejectingId !== null}
        onClose={() => { setRejectingId(null); setRejectReason(null); }}
        title={t("requestsPage.rejectTitle", { id: rejectingId })}
      >
        <Stack gap="sm">
          <Select
            label={t("requestsPage.reasonLabel")}
            placeholder={t("requestsPage.reasonPlaceholder")}
            data={reasonOptions}
            value={rejectReason}
            onChange={setRejectReason}
            data-testid="requests-reject-reason"
          />
          <Group gap="sm">
            <Button
              onClick={doReject}
              disabled={!rejectReason || busy}
              data-testid="requests-reject-confirm"
            >
              {t("requestsPage.confirm")}
            </Button>
            <Button
              variant="default"
              onClick={() => { setRejectingId(null); setRejectReason(null); }}
            >
              {t("requestsPage.cancel")}
            </Button>
          </Group>
        </Stack>
      </Modal>

      {filtered.length === 0 ? (
        <Text c="dimmed">
          {search
            ? t("requestsPage.emptyFiltered", { tab })
            : t("requestsPage.empty", { tab })}
        </Text>
      ) : (
        <>
          <Table.ScrollContainer minWidth={900}>
            <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>{t("requestsPage.colId")}</Table.Th>
                  <Table.Th>{t("requestsPage.colType")}</Table.Th>
                  <Table.Th>{t("requestsPage.colRequester")}</Table.Th>
                  <Table.Th>{t("requestsPage.colSubmitted")}</Table.Th>
                  <Table.Th>{t("requestsPage.colPayload")}</Table.Th>
                  <Table.Th>{t("requestsPage.colApprovals")}</Table.Th>
                  <Table.Th>{t("requestsPage.colStatus")}</Table.Th>
                  <Table.Th>{t("requestsPage.colReason")}</Table.Th>
                  <Table.Th>{t("requestsPage.colActions")}</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {paged.map((row) => (
                  <Table.Tr key={row.id}>
                    <Table.Td>{row.id}</Table.Td>
                    <Table.Td>{row.request_type}</Table.Td>
                    <Table.Td>{row.requested_by ?? t("requestsPage.none")}</Table.Td>
                    <Table.Td style={{ whiteSpace: "nowrap" }}>
                      {new Date(row.created_at).toLocaleString()}
                    </Table.Td>
                    <Table.Td style={{ maxWidth: "20rem" }}>
                      <details>
                        <summary style={{ cursor: "pointer" }}>{t("requestsPage.viewPayload")}</summary>
                        <pre style={{ fontSize: "0.75rem", whiteSpace: "pre-wrap" }}>
                          {JSON.stringify(row.payload, null, 2)}
                        </pre>
                      </details>
                    </Table.Td>
                    <Table.Td>{row.approvals.length} / {row.required_approvals}</Table.Td>
                    <Table.Td>
                      <Badge color={statusColor(row.status)} variant="light">
                        {row.status}
                      </Badge>
                    </Table.Td>
                    <Table.Td>{row.rejection_reason ?? t("requestsPage.none")}</Table.Td>
                    <Table.Td style={{ whiteSpace: "nowrap" }}>
                      {row.status === "pending" && (
                        <Group gap="xs" wrap="nowrap">
                          <Button
                            size="compact-xs"
                            onClick={() => doApprove(row.id)}
                            disabled={busy}
                            data-testid={`requests-approve-${row.id}`}
                          >
                            {t("requestsPage.approve", {
                              count: row.approvals.length + 1,
                              total: row.required_approvals,
                            })}
                          </Button>
                          {row.required_approvals === 1 && (
                            <Button
                              size="compact-xs"
                              onClick={() => doExecute(row.id)}
                              disabled={busy}
                              data-testid={`requests-execute-${row.id}`}
                            >
                              {t("requestsPage.execute")}
                            </Button>
                          )}
                          <Button
                            size="compact-xs"
                            variant="default"
                            onClick={() => { setRejectingId(row.id); setRejectReason(null); }}
                            disabled={busy}
                            data-testid={`requests-reject-${row.id}`}
                          >
                            {t("requestsPage.reject")}
                          </Button>
                        </Group>
                      )}
                    </Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
          {totalPages > 1 && (
            <Group justify="flex-end">
              <Pagination total={totalPages} value={safePage} onChange={setPage} size="sm" />
            </Group>
          )}
        </>
      )}
    </Stack>
  );
}
