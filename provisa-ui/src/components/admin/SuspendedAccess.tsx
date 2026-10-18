// Copyright (c) 2026 Kenneth Stott
// Canary: 3a4566a2-4da5-4d1f-9353-d925279a0a9a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2076: naming a role the caller does not hold is a high-severity security event. A personal
// access token that does it is suspended, and so is an identity-provider principal's token access
// over MCP and the non-browser transports. Only an administrator lifts a suspension -- nothing does
// on a timer -- and this is where they see each one, the event behind it, and lift it.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Badge, Button, Code, Group, Stack, Table, Text } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import {
  fetchAuditedStatement,
  fetchOrgSuspensions,
  liftOrgSuspension,
  type OrgSuspensions,
  type RoleProbeEvent,
} from "../../api/admin";
import { ListEmpty, ListTable } from "../list/ListTable";

function when(iso: string): string {
  return new Date(iso).toLocaleString();
}

export function SuspendedAccess({
  orgId,
  onError,
}: {
  orgId: string;
  onError: (e: unknown) => void;
}) {
  const { t } = useTranslation();
  const [data, setData] = useState<OrgSuspensions | null>(null);
  // Statement texts read so far, by audit row id; a refusal (no view_governance) is shown as one.
  const [statements, setStatements] = useState<Record<number, string>>({});

  const load = useCallback(() => {
    fetchOrgSuspensions(orgId).then(setData).catch(onError);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- onError is re-created each render, so depending on it refetches in a loop; the read is keyed on the org alone
  }, [orgId]);

  useEffect(load, [load]);

  const lift = async (target: { token_hash: string } | { user_id: string }) => {
    try {
      await liftOrgSuspension(orgId, target);
      notifications.show({ color: "green", message: t("suspendedAccess.lifted") });
      load();
    } catch (e) {
      onError(e);
    }
  };

  const showStatement = async (id: number) => {
    try {
      const text = await fetchAuditedStatement(id);
      setStatements((prev) => ({ ...prev, [id]: text }));
    } catch (e) {
      const status = (e as { status?: number }).status;
      setStatements((prev) => ({
        ...prev,
        [id]: status === 403 ? t("suspendedAccess.statementNeedsRight") : String(e),
      }));
    }
  };

  // The event that suspended each credential: the latest one naming it.
  const eventFor = (match: (e: RoleProbeEvent) => boolean) => data?.events.find(match);

  const eventDetail = (event: RoleProbeEvent | undefined) =>
    event === undefined ? null : (
      <Stack gap={2}>
        <Text size="xs">
          {t("suspendedAccess.eventSummary", {
            role: event.detail.requested_role,
            transport: event.detail.transport,
            endpoint: event.detail.endpoint,
          })}
        </Text>
        {event.detail.held_roles.length > 0 && (
          <Text size="xs" c="dimmed">
            {t("suspendedAccess.heldRoles", { roles: event.detail.held_roles.join(", ") })}
          </Text>
        )}
        {event.detail.client && (
          <Text size="xs" c="dimmed">
            {t("suspendedAccess.client", { client: event.detail.client })}
          </Text>
        )}
        {event.detail.recent_statement_ids.length > 0 && (
          <Group gap={4}>
            <Text size="xs" c="dimmed">
              {t("suspendedAccess.recentStatements")}
            </Text>
            {event.detail.recent_statement_ids.map((id) => (
              <Button
                key={id}
                size="compact-xs"
                variant="subtle"
                onClick={() => showStatement(id)}
                data-testid={`suspended-statement-${id}`}
              >
                #{id}
              </Button>
            ))}
          </Group>
        )}
        {event.detail.recent_statement_ids
          .filter((id) => statements[id] !== undefined)
          .map((id) => (
            <Code key={id} block data-testid={`suspended-statement-text-${id}`}>
              {statements[id]}
            </Code>
          ))}
      </Stack>
    );

  if (data === null) return null;
  const empty = data.tokens.length === 0 && data.principals.length === 0;
  return (
    <Stack gap="sm" data-testid="suspended-access">
      <Text c="dimmed" size="sm">
        {t("suspendedAccess.help")}
      </Text>
      <ListTable minWidth={640} testId="suspended-access-list">
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("suspendedAccess.colCredential")}</Table.Th>
            <Table.Th>{t("suspendedAccess.colPerson")}</Table.Th>
            <Table.Th>{t("suspendedAccess.colEvent")}</Table.Th>
            <Table.Th>{t("suspendedAccess.colSince")}</Table.Th>
            <Table.Th>{t("suspendedAccess.colActions")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {empty && <ListEmpty colSpan={5}>{t("suspendedAccess.none")}</ListEmpty>}
          {data.tokens.map((tok) => (
            <Table.Tr key={tok.token_hash} data-testid={`suspended-token-${tok.prefix}`}>
              <Table.Td>
                <Badge variant="light" color="red">
                  {t("suspendedAccess.kindToken")}
                </Badge>
                <Text size="sm">
                  {tok.name} <Code>{tok.prefix}</Code>
                </Text>
              </Table.Td>
              <Table.Td>{tok.user_id}</Table.Td>
              <Table.Td>
                {eventDetail(eventFor((e) => e.detail.credential_id === tok.token_hash))}
              </Table.Td>
              <Table.Td>{when(tok.suspended_at)}</Table.Td>
              <Table.Td>
                <Button
                  size="compact-xs"
                  variant="default"
                  onClick={() => lift({ token_hash: tok.token_hash })}
                  data-testid={`suspended-lift-token-${tok.prefix}`}
                >
                  {t("suspendedAccess.lift")}
                </Button>
              </Table.Td>
            </Table.Tr>
          ))}
          {data.principals.map((p) => (
            <Table.Tr key={p.user_id} data-testid={`suspended-principal-${p.user_id}`}>
              <Table.Td>
                <Badge variant="light" color="red">
                  {t("suspendedAccess.kindPrincipal")}
                </Badge>
              </Table.Td>
              <Table.Td>{p.user_id}</Table.Td>
              <Table.Td>
                {eventDetail(
                  eventFor((e) => e.detail.suspended === "principal" && e.subject_id === p.user_id),
                )}
              </Table.Td>
              <Table.Td>{when(p.token_access_suspended_at)}</Table.Td>
              <Table.Td>
                <Button
                  size="compact-xs"
                  variant="default"
                  onClick={() => lift({ user_id: p.user_id })}
                  data-testid={`suspended-lift-principal-${p.user_id}`}
                >
                  {t("suspendedAccess.lift")}
                </Button>
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </ListTable>
    </Stack>
  );
}
