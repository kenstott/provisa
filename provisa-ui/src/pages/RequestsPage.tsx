// Copyright (c) 2026 Kenneth Stott
// Canary: e5b79d29-d34a-4d18-9605-5817d958161e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { ListOrdered } from "lucide-react";
import { DetailFields } from "../components/detail/DetailFields";
import { PanelActions, SplitPane } from "../components/list/SplitPane";
import { useRememberedDetail } from "../components/list/useDetailParam";
import { useUrlFilter } from "../components/list/useUrlFilter";
import { useRequiredFields } from "../components/form/useRequiredFields";
import { PageTitle } from "../components/PageTitle";
import { useEffect, useState } from "react";
import { useRememberedChoice } from "../hooks/useRememberedChoice";
import { useTranslation } from "react-i18next";
import { FormField, FormFooter, FormLayout } from "../components/form/FormLayout";
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
  VisuallyHidden,
} from "@mantine/core";
import { FilterInput } from "../components/admin/FilterInput";
import { serverMessage, requestFailed, type ServerMessageShape } from "../i18n/serverMessage";
import { ListTable, ListHead, ListRow, ListEmpty, ListItems } from "../components/list/ListTable";
import { useListSortGroup, pageItems, type ListColumn } from "../components/list/useListSortGroup";
import { Tip } from "../components/Tip";
import { matchesQuery } from "../lib/searchItems";
import { useAuth } from "../context/AuthContext";
import { RequestDeliverySetting } from "../components/admin/RequestDeliverySetting";

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
  // REQ-1948: the domains a relationship request touches, the ones no approver has reached yet,
  // and whether this user is one of those who may decide it.
  domains: string[];
  waiting_on: string[];
  can_decide: boolean;
  // Why this user's approval would be refused (the server's own refusal), or null when it counts.
  approve_refusal: ServerMessageShape | null;
  // Its author may take a pending request back (not a relationship request's).
  can_withdraw: boolean;
}

function errorText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

async function apiFetch(path: string, opts?: RequestInit) {
  const resp = await fetch(`${API_BASE}${path}`, opts);
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(body, requestFailed("Request", resp.status)));
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

const REQUEST_TABS = ["pending", "resolved"] as const;

export function RequestsPage() {
  const { t } = useTranslation();
  // REQ-2216: the org's delivery setting is the org administrators' (org_settings).
  const { capabilities } = useAuth();
  const canSetDelivery = capabilities?.includes("org_settings") ?? false;
  const [tab, setTab] = useRememberedChoice("filter.requests.tab", REQUEST_TABS, "pending");
  const [rows, setRows] = useState<CreationRequest[]>([]);
  const [reasons, setReasons] = useState<Record<string, string[]>>({});
  const [error, setError] = useState<string | null>(null);
  const [rejectingId, setRejectingId] = useState<number | null>(null);
  const [rejectReason, setRejectReason] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [search, setSearch] = useUrlFilter();
  const [page, setPage] = useState(1);
  const [loaded, setLoaded] = useState(false);
  const detail = useRememberedDetail("requests", loaded ? rows.map((r) => String(r.id)) : null);
  const reasonRequired = useRequiredFields({ reason: rejectReason });

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
      .then((data) => {
        setRows(status === "resolved" ? data.filter((r) => r.status !== "pending") : data);
        setLoaded(true);
      })
      .catch((e) => setError(errorText(e)));
  };

  useEffect(() => {
    load(tab);
    fetchRejectionReasons()
      .then(setReasons)
      .catch((e) => setError(errorText(e)));
  }, [tab]);

  const doApprove = async (id: number) => {
    setBusy(true);
    setError(null);
    try {
      await apiApprove(id);
      load(tab);
    } catch (e) {
      setError(errorText(e));
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
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  };

  const doReject = async () => {
    if (rejectingId === null || !reasonRequired.validate() || !rejectReason) return;
    setBusy(true);
    setError(null);
    try {
      await apiReject(rejectingId, rejectReason);
      setRejectingId(null);
      setRejectReason(null);
      load(tab);
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  };

  const displayRows =
    tab === "pending"
      ? rows.filter((r) => r.status === "pending")
      : rows.filter((r) => r.status !== "pending");

  const filtered = displayRows.filter((r) =>
    // Every text a request's row shows.
    matchesQuery(
      [
        String(r.id),
        r.request_type,
        r.requested_by,
        new Date(r.created_at).toLocaleString(),
        `${r.approvals.length} / ${r.required_approvals}`,
        r.waiting_on.join(", "),
        r.status,
        r.rejection_reason,
      ],
      search,
    ),
  );
  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safePage = Math.min(page, totalPages);
  // REQ-1940: sort and group are the shared list mechanism.
  const requestColumns: ListColumn<CreationRequest>[] = [
    { key: "id", label: t("requestsPage.colId"), sortValue: (r) => r.id },
    {
      key: "type",
      label: t("requestsPage.colType"),
      sortValue: (r) => r.request_type,
      groupValue: (r) => r.request_type,
    },
    {
      key: "requester",
      label: t("requestsPage.colRequester"),
      sortValue: (r) => r.requested_by ?? t("requestsPage.none"),
      groupValue: (r) => r.requested_by ?? t("requestsPage.none"),
    },
    {
      key: "submitted",
      label: t("requestsPage.colSubmitted"),
      sortValue: (r) => new Date(r.created_at).getTime(),
    },
    {
      key: "approvals",
      label: t("requestsPage.colApprovals"),
      sortValue: (r) => r.approvals.length,
    },
    {
      key: "waiting",
      label: t("requestsPage.colWaitingOn"),
      sortValue: (r) => r.waiting_on.join(", "),
    },
    {
      key: "status",
      label: t("requestsPage.colStatus"),
      sortValue: (r) => r.status,
      groupValue: (r) => r.status,
    },
    {
      key: "reason",
      label: t("requestsPage.colReason"),
      sortValue: (r) => r.rejection_reason ?? t("requestsPage.none"),
    },
  ];
  const sortGroup = useListSortGroup(filtered, requestColumns, "requests");

  const rejectingRequest = rows.find((r) => r.id === rejectingId);
  const reasonOptions = (reasons[rejectingRequest?.request_type ?? ""] ?? []).map((r) => ({
    value: r,
    label: REASON_LABELS[r] ?? r,
  }));

  const statusColor = (status: string) =>
    status === "pending" ? "yellow" : status === "approved" ? "green" : "gray";

  const openRow = rows.find((r) => String(r.id) === detail.id) ?? null;

  /** What the user may do with a request: decide it, take it back, or read why neither. */
  const actions = (row: CreationRequest) => (
    <>
      {row.status === "pending" && !row.can_decide && row.approve_refusal && (
        <Text size="xs" c="dimmed" data-testid={`requests-cannot-${row.id}`}>
          {serverMessage(row.approve_refusal, "")}
        </Text>
      )}
      {row.status === "pending" && !row.can_decide && row.can_withdraw && (
        <PanelActions>
          <fieldset className="panel-action-set">
            <Button
              onClick={() => {
                setRejectingId(row.id);
                setRejectReason(null);
              }}
              disabled={busy}
              data-testid={`requests-withdraw-${row.id}`}
            >
              {t("requestsPage.reject")}
            </Button>
          </fieldset>
        </PanelActions>
      )}
      {row.status === "pending" && row.can_decide && (
        <PanelActions>
          <fieldset className="panel-action-set">
            <Group gap="xs" wrap="nowrap">
              {/* A disabled button takes no pointer, so the hint hangs on a wrapper around it; the
                  reason is also the button's description, for a screen reader. */}
              <Tip
                text={row.approve_refusal ? serverMessage(row.approve_refusal, "") : undefined}
                named
              >
                <span style={{ display: "inline-flex" }}>
                  <Button
                    onClick={() => doApprove(row.id)}
                    disabled={busy || row.approve_refusal !== null}
                    aria-describedby={
                      row.approve_refusal ? `requests-approve-why-${row.id}` : undefined
                    }
                    data-testid={`requests-approve-${row.id}`}
                  >
                    {t("requestsPage.approve")}
                  </Button>
                  {row.approve_refusal && (
                    <VisuallyHidden id={`requests-approve-why-${row.id}`}>
                      {serverMessage(row.approve_refusal, "")}
                    </VisuallyHidden>
                  )}
                </span>
              </Tip>
              {/* Executing only retries a creation that failed when the last approval was
                  given: it is offered once the approvals are in. */}
              {row.approvals.length >= row.required_approvals && row.waiting_on.length === 0 && (
                <Button
                  onClick={() => doExecute(row.id)}
                  disabled={busy}
                  data-testid={`requests-execute-${row.id}`}
                >
                  {t("requestsPage.execute")}
                </Button>
              )}
              <Button
                onClick={() => {
                  setRejectingId(row.id);
                  setRejectReason(null);
                }}
                disabled={busy}
                data-testid={`requests-reject-${row.id}`}
              >
                {t("requestsPage.reject")}
              </Button>
            </Group>
          </fieldset>
        </PanelActions>
      )}
    </>
  );

  const requestView = (row: CreationRequest) => (
    <Stack gap="sm" data-testid={`request-detail-${row.id}`}>
      {actions(row)}
      <DetailFields
        testId={`request-read-${row.id}`}
        fields={[
          { label: t("requestsPage.colId"), value: String(row.id) },
          { label: t("requestsPage.colType"), value: row.request_type },
          { label: t("requestsPage.colRequester"), value: row.requested_by },
          {
            label: t("requestsPage.colSubmitted"),
            value: new Date(row.created_at).toLocaleString(),
          },
          {
            label: t("requestsPage.colApprovals"),
            value: `${row.approvals.length} / ${row.required_approvals}`,
          },
          {
            label: t("requestsPage.colWaitingOn"),
            value: row.waiting_on.length > 0 ? row.waiting_on.join(", ") : null,
          },
          {
            label: t("requestsPage.colStatus"),
            value: (
              <Badge color={statusColor(row.status)} variant="light">
                {row.status}
              </Badge>
            ),
          },
          { label: t("requestsPage.colReason"), value: row.rejection_reason },
          {
            label: t("requestsPage.colPayload"),
            value: (
              <pre
                style={{ fontSize: "0.75rem", whiteSpace: "pre-wrap", margin: 0 }}
                data-testid={`request-payload-${row.id}`}
              >
                {JSON.stringify(row.payload, null, 2)}
              </pre>
            ),
          },
        ]}
      />
    </Stack>
  );

  return (
    <Stack gap="md" className="page">
      <div className="page-header" style={{ marginBottom: 0 }}>
        <PageTitle icon={ListOrdered}>{t("requestsPage.title")}</PageTitle>
        <FilterInput
          value={search}
          onChange={(v) => {
            setSearch(v);
            setPage(1);
          }}
          placeholder={t("requestsPage.filterPlaceholder")}
        />
      </div>
      <Tabs
        value={tab}
        onChange={(v) => {
          setTab((v as "pending" | "resolved") ?? "pending");
          setPage(1);
        }}
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

      {canSetDelivery && <RequestDeliverySetting onError={setError} />}

      {error && (
        <Alert color="red" variant="light" data-testid="requests-error">
          {error}
        </Alert>
      )}

      <Modal
        opened={rejectingId !== null}
        onClose={() => {
          setRejectingId(null);
          setRejectReason(null);
        }}
        title={t("requestsPage.rejectTitle", { id: rejectingId })}
      >
        <FormLayout>
          <FormField size="long">
            <Select
              label={t("requestsPage.reasonLabel")}
              placeholder={t("requestsPage.reasonPlaceholder")}
              data={reasonOptions}
              value={rejectReason}
              onChange={setRejectReason}
              data-testid="requests-reject-reason"
              {...reasonRequired.field("reason")}
            />
          </FormField>
          <FormFooter
            onCancel={() => {
              setRejectingId(null);
              setRejectReason(null);
            }}
            action={
              <Button onClick={doReject} loading={busy} data-testid="requests-reject-confirm">
                {t("requestsPage.confirm")}
              </Button>
            }
          />
        </FormLayout>
      </Modal>

      <SplitPane
        testId="requests-split"
        storageKey="provisa.requests.detailWidth"
        title={openRow !== null ? t("requestsPage.panelTitle", { id: openRow.id }) : ""}
        onClose={() => detail.close()}
        labels={{
          panel: t("requestsPage.panelLabel"),
          resize: t("tablesPage.resizeDetail"),
          expand: t("tablesPage.expandDetail"),
          restore: t("tablesPage.restoreDetail"),
          close: t("tablesPage.closeDetail"),
          backToList: t("tablesPage.backToList"),
        }}
        list={
          <>
            <ListTable minWidth={800} testId="requests-list">
              <ListHead
                columns={[
                  { col: "id" },
                  { col: "type" },
                  { col: "requester" },
                  { col: "submitted" },
                  { col: "approvals" },
                  { col: "waiting" },
                  { col: "status" },
                  { col: "reason" },
                ]}
                sortGroup={sortGroup}
              />
              <Table.Tbody>
                {filtered.length === 0 && (
                  <ListEmpty colSpan={8} testId="requests-empty">
                    {search
                      ? t("requestsPage.emptyFiltered", { tab })
                      : t("requestsPage.empty", { tab })}
                  </ListEmpty>
                )}
                {filtered.length > 0 && (
                  <ListItems
                    state={sortGroup}
                    items={pageItems(sortGroup, safePage - 1, PAGE_SIZE)}
                    colSpan={8}
                    rowKey={(row) => row.id}
                    render={(row) => (
                      <ListRow
                        testId={`request-row-${row.id}`}
                        selected={String(row.id) === detail.id}
                        onClick={() => {
                          if (detail.id === String(row.id)) detail.close();
                          else detail.open(String(row.id));
                        }}
                      >
                        <Table.Td>{row.id}</Table.Td>
                        <Table.Td>{row.request_type}</Table.Td>
                        <Table.Td>{row.requested_by ?? t("requestsPage.none")}</Table.Td>
                        <Table.Td style={{ whiteSpace: "nowrap" }}>
                          {new Date(row.created_at).toLocaleString()}
                        </Table.Td>
                        <Table.Td>
                          {row.approvals.length} / {row.required_approvals}
                        </Table.Td>
                        <Table.Td data-testid={`requests-waiting-${row.id}`}>
                          {row.waiting_on.length > 0
                            ? row.waiting_on.join(", ")
                            : t("requestsPage.none")}
                        </Table.Td>
                        <Table.Td>
                          <Badge color={statusColor(row.status)} variant="light">
                            {row.status}
                          </Badge>
                        </Table.Td>
                        <Table.Td>{row.rejection_reason ?? t("requestsPage.none")}</Table.Td>
                      </ListRow>
                    )}
                  />
                )}
              </Table.Tbody>
            </ListTable>
            {totalPages > 1 && sortGroup.groupBy.length === 0 && (
              <Group justify="flex-end">
                <Pagination total={totalPages} value={safePage} onChange={setPage} size="sm" />
              </Group>
            )}
          </>
        }
      >
        {openRow !== null ? requestView(openRow) : null}
      </SplitPane>
    </Stack>
  );
}
