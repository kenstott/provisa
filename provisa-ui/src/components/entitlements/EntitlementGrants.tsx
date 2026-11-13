// Copyright (c) 2026 Kenneth Stott
// Canary: 35bb92c9-80b2-49e3-a1e5-3219fa572606
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219 (ONE API, THREE WAYS TO USE IT): grant access to anyone the people search finds, or to
// a typed email for a person not yet seen, and the grants still waiting on an email. A waiting
// grant attaches when a sign-in token's email matches; until then it can be ended here.

import { DestructiveConfirmButton } from "../form/DestructiveConfirmButton";
import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Button, Group, Stack, Table, Text, TextInput } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Plus } from "lucide-react";
import { endList, fetchPending, type AccessView, type PendingGrant } from "../../api/entitlements";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { ListEmpty, ListHead, ListItems, ListRow, ListTable } from "../list/ListTable";
import { useListSortGroup, type ListColumn } from "../list/useListSortGroup";
import { EntitlementListForm } from "./EntitlementListForm";

const pendingKey = (g: PendingGrant) => `${g.email}|${g.role_id}|${g.kind}`;

export function EntitlementGrants({ roleIds }: { roleIds: string[] }) {
  const { t } = useTranslation();
  const [pending, setPending] = useState<PendingGrant[]>([]);
  const [granting, setGranting] = useState(false);
  const [ending, setEnding] = useState<PendingGrant | null>(null);
  const [endReason, setEndReason] = useState("");
  const [error, setError] = useState<string | null>(null);

  const report = useCallback(
    (e: unknown) => setError(e instanceof Error ? e.message : String(e)),
    [],
  );
  const load = useCallback(() => fetchPending().then(setPending).catch(report), [report]);

  useEffect(() => {
    load();
  }, [load]);

  const saved = (result: AccessView | PendingGrant) => {
    setGranting(false);
    setError(null);
    notifications.show({
      color: "green",
      message:
        "pending" in result
          ? t("entitlements.grants.savedPending", { email: result.email })
          : t("entitlements.grants.saved", { role: result.role_id }),
    });
    load();
  };

  const end = async (g: PendingGrant) => {
    try {
      await endList({
        grantee: { email: g.email },
        roleId: g.role_id,
        kind: g.kind,
        reason: endReason.trim(),
      });
      setEnding(null);
      setEndReason("");
      setError(null);
      load();
    } catch (e) {
      report(e);
    }
  };

  const columns: ListColumn<PendingGrant>[] = [
    { key: "email", label: t("entitlements.grants.colEmail"), sortValue: (g) => g.email },
    {
      key: "role",
      label: t("entitlements.grants.colRole"),
      sortValue: (g) => g.role_id,
      groupValue: (g) => g.role_id,
    },
    {
      key: "kind",
      label: t("entitlements.grants.colKind"),
      sortValue: (g) => t(`entitlements.kind.${g.kind}`),
      groupValue: (g) => t(`entitlements.kind.${g.kind}`),
    },
    {
      key: "entries",
      label: t("entitlements.grants.colEntries"),
      sortValue: (g) => g.identifiers.join(", "),
    },
    { key: "ends", label: t("entitlements.grants.colEnds"), sortValue: (g) => g.expires_at ?? "" },
    { key: "reason", label: t("entitlements.grants.colReason"), sortValue: (g) => g.reason ?? "" },
  ];
  const sortGroup = useListSortGroup(pending, columns, "entitlement-pending");

  return (
    <Stack gap="sm" data-testid="entitlement-grants">
      <Text size="sm" c="dimmed">
        {t("entitlements.grants.help")}
      </Text>
      {error && (
        <Alert
          color="red"
          withCloseButton
          onClose={() => setError(null)}
          data-testid="entitlement-grants-error"
        >
          {error}
        </Alert>
      )}
      {granting ? (
        <EntitlementListForm
          roleIds={roleIds}
          onSaved={saved}
          onCancel={() => setGranting(false)}
          onError={report}
          testId="entitlement-grant-form"
        />
      ) : (
        <Group>
          <Button
            variant="default"
            size="xs"
            leftSection={<Plus size={14} />}
            onClick={() => setGranting(true)}
            data-testid="entitlement-grant-open"
          >
            {t("entitlements.grants.grantButton")}
          </Button>
        </Group>
      )}

      <Text fw={500} size="sm">
        {t("entitlements.grants.pendingHeading")}
      </Text>
      {ending !== null && (
        <FormLayout data-testid="entitlement-pending-end">
          <Text size="sm">
            {t("entitlements.grants.endPrompt", {
              email: ending.email,
              role: ending.role_id,
              kind: t(`entitlements.kind.${ending.kind}`),
            })}
          </Text>
          <FormField size="long" hint={t("entitlements.form.reasonHint")}>
            <TextInput
              label={t("entitlements.form.reasonLabel")}
              required
              value={endReason}
              onChange={(e) => setEndReason(e.currentTarget.value)}
              data-testid="entitlement-pending-end-reason"
            />
          </FormField>
          <FormFooter
            onCancel={() => setEnding(null)}
            cancelProps={{ "data-testid": "entitlement-pending-end-cancel" }}
            action={
              <DestructiveConfirmButton
                disabled={endReason.trim() === ""}
                onClick={() => end(ending)}
                testId="entitlement-pending-end-submit"
              >
                {t("entitlements.grants.endButton")}
              </DestructiveConfirmButton>
            }
          />
        </FormLayout>
      )}
      <div className="card-table-scroll">
        <ListTable minWidth={640} testId="entitlement-pending">
          <ListHead
            sortGroup={sortGroup}
            columns={[
              { col: "email" },
              { col: "role" },
              { col: "kind" },
              { col: "entries" },
              { col: "ends" },
              { col: "reason" },
              t("entitlements.grants.colActions"),
            ]}
          />
          <Table.Tbody>
            {pending.length === 0 && (
              <ListEmpty colSpan={7}>{t("entitlements.grants.noPending")}</ListEmpty>
            )}
            <ListItems
              state={sortGroup}
              colSpan={7}
              rowKey={pendingKey}
              render={(g) => (
                <ListRow testId={`entitlement-pending-${pendingKey(g)}`}>
                  <Table.Td>{g.email}</Table.Td>
                  <Table.Td>{g.role_id}</Table.Td>
                  <Table.Td>{t(`entitlements.kind.${g.kind}`)}</Table.Td>
                  <Table.Td>
                    <Group gap={4}>
                      {g.identifiers.length === 0 && (
                        <Text size="sm" c="dimmed">
                          {t("entitlements.person.nothing")}
                        </Text>
                      )}
                      {g.identifiers.map((i) => (
                        <Badge key={i} variant="light" tt="none" ff="monospace">
                          {i === "*" ? t("entitlements.person.everything") : i}
                        </Badge>
                      ))}
                    </Group>
                  </Table.Td>
                  <Table.Td>{g.expires_at ? g.expires_at.slice(0, 10) : "—"}</Table.Td>
                  <Table.Td>{g.reason ?? "—"}</Table.Td>
                  <Table.Td>
                    <Button
                      size="compact-xs"
                      color="red"
                      variant="light"
                      onClick={() => {
                        setEndReason("");
                        setEnding(g);
                      }}
                      data-testid={`entitlement-pending-end-${pendingKey(g)}`}
                    >
                      {t("entitlements.grants.endButton")}
                    </Button>
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
